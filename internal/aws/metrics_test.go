/*
Copyright 2026 Patrick Ajah.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aws

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	smithy "github.com/aws/smithy-go"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"golang.org/x/time/rate"

	"github.com/Ningendo7/cloudctl-operator/internal/observability"
)

func TestMetricResult(t *testing.T) {
	httpStatus := func(code int) error {
		return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}},
			Err:      errors.New("bare status error"),
		}}
	}
	api := func(code string) error { return &smithy.GenericAPIError{Code: code, Message: "m"} }

	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"nil":                        {nil, observability.ResultSuccess},
		"throttling exception":       {api("ThrottlingException"), observability.ResultThrottled},
		"request limit exceeded":     {api("RequestLimitExceeded"), observability.ResultThrottled},
		"too many requests":          {api("TooManyRequestsException"), observability.ResultThrottled},
		"a throttling variant":       {api("ProvisionedThroughputThrottlingException"), observability.ResultThrottled},
		"http 429":                   {httpStatus(429), observability.ResultThrottled},
		"access denied":              {api("AccessDenied"), observability.ResultPermissionDenied},
		"not authorized":             {api("NotAuthorized"), observability.ResultPermissionDenied},
		"http 403":                   {httpStatus(403), observability.ResultPermissionDenied},
		"no such bucket":             {api("NoSuchBucket"), observability.ResultNotFound},
		"queue does not exist":       {api("QueueDoesNotExist"), observability.ResultNotFound},
		"resource not found":         {api("ResourceNotFoundException"), observability.ResultNotFound},
		"plain not found":            {api("NotFound"), observability.ResultNotFound},
		"http 404":                   {httpStatus(404), observability.ResultNotFound},
		"internal failure":           {api("InternalFailure"), observability.ResultError},
		"an error that is not AWS's": {errors.New("connection refused"), observability.ResultError},
		"wrapped throttling":         {errors.Join(errors.New("context"), api("ThrottlingException")), observability.ResultThrottled},
	} {
		if got := MetricResult(tc.err); got != tc.want {
			t.Errorf("%s: MetricResult() = %q, want %q", name, got, tc.want)
		}
	}
}

// stubHTTP answers every request from respond(attempt) without any network.
type stubHTTP struct {
	calls   atomic.Int32
	respond func(attempt int) (status int, body string)
}

func (s *stubHTTP) Do(r *http.Request) (*http.Response, error) {
	status, body := s.respond(int(s.calls.Add(1)))
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    r,
	}, nil
}

// sqsClient is a real SDK client wired only to the stub, so the middleware
// under test runs inside the real operation pipeline.
func sqsClient(stub *stubHTTP, retryer aws.Retryer, apiOptions ...func(*smithymiddleware.Stack) error) *sqs.Client {
	return sqs.New(sqs.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
		HTTPClient:  stub,
		Retryer:     retryer,
		APIOptions:  apiOptions,
	})
}

func counter(t *testing.T, service, result string) float64 {
	t.Helper()
	var m dto.Metric
	if err := observability.AWSRequests.WithLabelValues(service, result).Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	return m.GetCounter().GetValue()
}

func durationSamples(t *testing.T, service string) uint64 {
	t.Helper()
	var m dto.Metric
	obs, err := observability.AWSRequestDuration.GetMetricWithLabelValues(service)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues() error = %v", err)
	}
	if err := obs.(interface{ Write(*dto.Metric) error }).Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestMetricsMiddleware_RecordsEachOutcomeUnderItsService(t *testing.T) {
	for _, tc := range []struct {
		service string
		status  int
		body    string
		want    string
	}{
		{"mw_success", 200, `{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/q"}`, observability.ResultSuccess},
		{"mw_denied", 400, `{"__type":"AccessDeniedException","message":"no"}`, observability.ResultPermissionDenied},
		{"mw_throttled", 400, `{"__type":"ThrottlingException","message":"slow down"}`, observability.ResultThrottled},
		{"mw_missing", 400, `{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"gone"}`, observability.ResultNotFound},
		{"mw_broken", 400, `{"__type":"SomethingElse","message":"?"}`, observability.ResultError},
	} {
		t.Run(tc.service, func(t *testing.T) {
			stub := &stubHTTP{respond: func(int) (int, string) { return tc.status, tc.body }}
			client := sqsClient(stub, aws.NopRetryer{}, MetricsMiddleware(tc.service))

			_, _ = client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String("q")})

			if got := counter(t, tc.service, tc.want); got != 1 {
				t.Errorf("requests{service=%s,result=%s} = %v, want 1", tc.service, tc.want, got)
			}
			for _, other := range []string{
				observability.ResultSuccess, observability.ResultThrottled, observability.ResultPermissionDenied,
				observability.ResultNotFound, observability.ResultError,
			} {
				if other != tc.want && counter(t, tc.service, other) != 0 {
					t.Errorf("requests{service=%s,result=%s} should be 0", tc.service, other)
				}
			}
			if got := durationSamples(t, tc.service); got != 1 {
				t.Errorf("duration samples for %s = %d, want 1", tc.service, got)
			}
		})
	}
}

// One operation is one request however often the SDK retried it, and its
// duration covers the retries.
func TestMetricsMiddleware_CountsARetriedOperationOnce(t *testing.T) {
	stub := &stubHTTP{respond: func(int) (int, string) { return 500, `{"__type":"InternalFailure","message":"x"}` }}
	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 3
		o.RateLimiter = ratelimit.None
		o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
	})
	client := sqsClient(stub, retryer, MetricsMiddleware("mw_retried"))

	_, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String("q")})
	if err == nil {
		t.Fatal("expected the operation to fail")
	}
	if stub.calls.Load() != 3 {
		t.Fatalf("expected the SDK to attempt the call 3 times, got %d", stub.calls.Load())
	}
	if got := counter(t, "mw_retried", observability.ResultError); got != 1 {
		t.Errorf("requests = %v, want exactly 1 for one retried operation", got)
	}
	if got := durationSamples(t, "mw_retried"); got != 1 {
		t.Errorf("duration samples = %d, want 1", got)
	}
}

func TestRateLimitMiddleware_RecordsTimeSpentWaiting(t *testing.T) {
	stub := &stubHTTP{respond: func(int) (int, string) {
		return 200, `{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/q"}`
	}}
	// 5 calls a second with a burst of one: the second call must wait ~200ms.
	limiter := rate.NewLimiter(rate.Limit(5), 1)
	client := sqsClient(stub, aws.NopRetryer{}, RateLimitMiddleware("rl_wait", limiter))

	for range 2 {
		if _, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String("q")}); err != nil {
			t.Fatalf("GetQueueUrl() error = %v", err)
		}
	}

	var m dto.Metric
	if err := observability.AWSRateLimitWait.WithLabelValues("rl_wait").Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if waited := m.GetCounter().GetValue(); waited < 0.1 || waited > 5 {
		t.Errorf("rate-limit wait = %vs, want roughly 0.2s for the second call", waited)
	}
}

func TestRateLimitMiddleware_NilLimiterRecordsNothing(t *testing.T) {
	stub := &stubHTTP{respond: func(int) (int, string) {
		return 200, `{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/q"}`
	}}
	client := sqsClient(stub, aws.NopRetryer{}, RateLimitMiddleware("rl_nil", nil))
	if _, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String("q")}); err != nil {
		t.Fatalf("GetQueueUrl() error = %v", err)
	}
	var m dto.Metric
	if err := observability.AWSRateLimitWait.WithLabelValues("rl_nil").Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if m.GetCounter().GetValue() != 0 {
		t.Errorf("expected no wait recorded without a limiter, got %v", m.GetCounter().GetValue())
	}
}

// withObservability is what every service client is built with, so it has to
// do all three things at once on one real operation: take the span, count the
// request, and leave the rate limiter in place.
func TestWithObservability_TracesCountsAndLimitsOneOperation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })

	stub := &stubHTTP{respond: func(int) (int, string) {
		return 200, `{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/q"}`
	}}
	client := sqsClient(stub, aws.NopRetryer{})
	options := withObservability(nil, "obs_all", rate.NewLimiter(rate.Inf, 1))
	client = sqs.New(sqs.Options{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
		HTTPClient: stub, Retryer: aws.NopRetryer{}, APIOptions: options,
	})

	ctx, parent := provider.Tracer("test").Start(context.Background(), "reconcile sqs")
	if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("q")}); err != nil {
		t.Fatalf("GetQueueUrl() error = %v", err)
	}
	parent.End()

	var call sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if strings.Contains(s.Name(), "GetQueueUrl") {
			call = s
		}
	}
	if call == nil {
		t.Fatalf("expected a span for the GetQueueUrl operation, got %d spans", len(recorder.Ended()))
	}
	if call.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Error("expected the AWS operation to nest under the section span that made it")
	}
	if got := counter(t, "obs_all", observability.ResultSuccess); got != 1 {
		t.Errorf("requests{service=obs_all,result=success} = %v, want 1", got)
	}
}
