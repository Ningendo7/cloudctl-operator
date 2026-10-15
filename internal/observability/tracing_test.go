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

package observability

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// useRecorder installs a recording tracer provider for one test and puts the
// previous (normally no-op) one back afterwards.
func useRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func TestTracingEnabled(t *testing.T) {
	for name, tc := range map[string]struct {
		env  map[string]string
		want bool
	}{
		"nothing set":            {nil, false},
		"the general endpoint":   {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317"}, true},
		"the traces endpoint":    {map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector:4317"}, true},
		"an empty general value": {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ""}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := TracingEnabled(); got != tc.want {
				t.Errorf("TracingEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// With no provider installed, which is every deployment that doesn't set an
// endpoint, spans must cost nothing and record nothing.
func TestTracer_IsANoOpUntilInitialised(t *testing.T) {
	_, span := Tracer().Start(context.Background(), "anything")
	defer span.End()
	if span.IsRecording() {
		t.Error("expected spans to be no-ops before tracing is initialised")
	}
}

func TestInitTracing_InstallsARecordingProvider_AndShutsDownCleanly(t *testing.T) {
	// The exporter connects lazily, so an unreachable address is fine here.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	shutdown, err := InitTracing(context.Background())
	if err != nil {
		t.Fatalf("InitTracing() error = %v", err)
	}
	_, span := Tracer().Start(context.Background(), "x")
	if !span.IsRecording() {
		t.Error("expected a recording span once tracing is initialised")
	}
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = shutdown(ctx) // flushing to nothing may error; it must return
}

func capturingLogger() (logr.Logger, *[]string) {
	var lines []string
	return funcr.New(func(prefix, args string) { lines = append(lines, prefix+args) }, funcr.Options{}), &lines
}

func TestLoggerWithTrace_AddsTheTraceIDInsideASpan(t *testing.T) {
	useRecorder(t)
	ctx, span := Tracer().Start(context.Background(), "Reconcile")
	defer span.End()

	log, lines := capturingLogger()
	LoggerWithTrace(ctx, log).Info("hello")

	if len(*lines) != 1 || !strings.Contains((*lines)[0], span.SpanContext().TraceID().String()) {
		t.Errorf("expected the trace ID %s in the log line, got %v", span.SpanContext().TraceID(), *lines)
	}
	if !strings.Contains((*lines)[0], "trace_id") || !strings.Contains((*lines)[0], "span_id") {
		t.Errorf("expected trace_id and span_id keys, got %v", *lines)
	}
}

func TestLoggerWithTrace_LeavesTheLoggerAloneWithoutASpan(t *testing.T) {
	log, lines := capturingLogger()
	LoggerWithTrace(context.Background(), log).Info("hello")
	if len(*lines) != 1 || strings.Contains((*lines)[0], "trace_id") {
		t.Errorf("expected no trace fields without a span, got %v", *lines)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTraceKubernetesRequests_SpansNestUnderTheCaller_AndWatchesAreSkipped(t *testing.T) {
	recorder := useRecorder(t)
	transport := TraceKubernetesRequests(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
	}))

	ctx, parent := Tracer().Start(context.Background(), "patch status")
	do := func(url string) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, nil)
		if err != nil {
			t.Fatalf("NewRequest() error = %v", err)
		}
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip() error = %v", err)
		}
		_ = resp.Body.Close()
	}
	do("https://10.0.0.1:443/apis/deps.cloudctl.io/v1alpha1/namespaces/a/appdependencies/x/status")
	do("https://10.0.0.1:443/api/v1/namespaces/a/configmaps?watch=true&resourceVersion=1")
	parent.End()

	var api []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "kube-apiserver PATCH" {
			api = append(api, s)
		}
	}
	if len(api) != 1 {
		t.Fatalf("expected exactly one API request span (the watch must be skipped), got %d", len(api))
	}
	if api[0].Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Error("expected the API request span to nest under the span that made the request")
	}
}
