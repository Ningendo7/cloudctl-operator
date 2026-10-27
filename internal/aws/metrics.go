package aws

import (
	"context"
	"errors"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithy "github.com/aws/smithy-go"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/tracing"
	"github.com/aws/smithy-go/tracing/smithyoteltracing"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"

	"github.com/Ningendo7/cloudctl-operator/internal/observability"
)

// MetricResult classifies the outcome of one AWS operation for the
// cloudctl_aws_requests_total metric. The result is one of a fixed set, never
// the raw error text, so it stays safe to use as a label.
func MetricResult(err error) string {
	switch {
	case err == nil:
		return observability.ResultSuccess
	case IsPermissionDenied(err):
		return observability.ResultPermissionDenied
	case isThrottled(err):
		return observability.ResultThrottled
	case isNotFound(err):
		return observability.ResultNotFound
	default:
		return observability.ResultError
	}
}

func isThrottled(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch code := apiErr.ErrorCode(); code {
		case errCodeThrottling, errCodeRequestLimitExceeded, errCodeTooManyRequestsException:
			return true
		default:
			return strings.Contains(code, "Throttl")
		}
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == 429
}

// isNotFound matches the many spellings AWS services use for "no such
// resource": NotFound, NoSuchBucket, QueueDoesNotExist, ResourceNotFound...
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return strings.Contains(code, "NotFound") || strings.Contains(code, "NoSuch") || strings.Contains(code, "DoesNotExist")
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == 404
}

// destructiveOperationPrefixes are the verbs of operations that remove or
// disable something. The SDK names every operation Verb+Noun, so a prefix
// is enough, and a service's new delete operation is counted without a code
// change.
var destructiveOperationPrefixes = []string{
	"Delete", "Remove", "Detach", "Revoke", "Unsubscribe", "Purge", "Disable", "Terminate", "ScheduleKeyDeletion",
}

// IsDestructiveOperation reports whether an SDK operation name removes or
// disables something.
func IsDestructiveOperation(operation string) bool {
	for _, prefix := range destructiveOperationPrefixes {
		if strings.HasPrefix(operation, prefix) {
			return true
		}
	}
	return false
}

// MetricsMiddleware records each AWS operation made through a client:
// its outcome and its total wall time. It sits in the Initialize step, which
// runs once per operation outside the SDK's retry loop, so a call the SDK
// retried three times is one request, and its duration includes the retries
// and any time spent waiting on RateLimitMiddleware.
func MetricsMiddleware(service string) func(*smithymiddleware.Stack) error {
	observability.InitAWSRequests(service)
	return func(stack *smithymiddleware.Stack) error {
		return stack.Initialize.Add(
			smithymiddleware.InitializeMiddlewareFunc("CloudctlMetrics", func(
				ctx context.Context,
				in smithymiddleware.InitializeInput,
				next smithymiddleware.InitializeHandler,
			) (smithymiddleware.InitializeOutput, smithymiddleware.Metadata, error) {
				start := time.Now()
				out, metadata, err := next.HandleInitialize(ctx, in)
				observability.AWSRequestDuration.WithLabelValues(service).Observe(time.Since(start).Seconds())
				result := MetricResult(err)
				observability.AWSRequests.WithLabelValues(service, result).Inc()
				if op := smithymiddleware.GetOperationName(ctx); IsDestructiveOperation(op) {
					observability.AWSDestructiveRequests.WithLabelValues(service, op, result).Inc()
				}
				return out, metadata, err
			}),
			smithymiddleware.Before,
		)
	}
}

// withObservability adds the rate limiter and the request metrics to one
// service client. Tracing is the client's TracerProvider option, see
// tracerProvider.
func withObservability(options []func(*smithymiddleware.Stack) error, service string, limiter *rate.Limiter) []func(*smithymiddleware.Stack) error {
	return append(options, RateLimitMiddleware(service, limiter), MetricsMiddleware(service))
}

// tracerProvider makes the SDK emit one span per operation (named like
// "SQS.GetQueueUrl"), nested under the span of the section making the call.
// It follows the global provider, so it is a no-op with tracing off.
func tracerProvider() tracing.TracerProvider {
	return smithyoteltracing.Adapt(otel.GetTracerProvider())
}
