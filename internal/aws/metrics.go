package aws

import (
	"context"
	"errors"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithy "github.com/aws/smithy-go"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
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

// MetricsMiddleware records each AWS operation made through a client:
// its outcome and its total wall time. It sits in the Initialize step, which
// runs once per operation outside the SDK's retry loop, so a call the SDK
// retried three times is one request, and its duration includes the retries
// and any time spent waiting on RateLimitMiddleware.
func MetricsMiddleware(service string) func(*smithymiddleware.Stack) error {
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
				observability.AWSRequests.WithLabelValues(service, MetricResult(err)).Inc()
				return out, metadata, err
			}),
			smithymiddleware.Before,
		)
	}
}

// withObservability adds everything one service client needs on top of the
// SDK's defaults: the rate limiter, the request metrics, and a trace span per
// operation (named like "SQS.GetQueueUrl", nested under the span of the
// section making the call, and a no-op with tracing off).
func withObservability(options []func(*smithymiddleware.Stack) error, service string, limiter *rate.Limiter) []func(*smithymiddleware.Stack) error {
	options = append(options, RateLimitMiddleware(service, limiter), MetricsMiddleware(service))
	otelaws.AppendMiddlewares(&options)
	return options
}
