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
	"fmt"
	"net/http"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "cloudctl-operator"

// TracingEnvVar is the standard OpenTelemetry variable that turns tracing
// on. Everything else (headers, protocol, sampler, resource attributes) is
// read by the SDK from its own standard variables, so no backend needs any
// operator-specific setting.
const TracingEnvVar = "OTEL_EXPORTER_OTLP_ENDPOINT"

var serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// TracingEnabled reports whether the environment asks for tracing.
func TracingEnabled() bool {
	return os.Getenv(TracingEnvVar) != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Tracer returns the operator's tracer. Until InitTracing installs a
// provider it is a no-op: starting a span costs nothing and sends nothing.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// InitTracing installs an OTLP/gRPC trace exporter as the global provider
// and returns the function that flushes it at shutdown. Callers only call it
// when TracingEnabled.
func InitTracing(ctx context.Context) (shutdown func(context.Context) error, err error) {
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating the OTLP trace exporter: %w", err)
	}
	res, err := resource.New(ctx, resourceOptions()...)
	if err != nil {
		return nil, fmt.Errorf("building the OpenTelemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

func resourceOptions() []resource.Option {
	opts := []resource.Option{
		resource.WithAttributes(semconv.ServiceName(tracerName)),
		resource.WithAttributes(semconv.ServiceInstanceID(os.Getenv("HOSTNAME"))),
		// Last, so OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES win.
		resource.WithFromEnv(),
	}
	if data, err := os.ReadFile(serviceAccountNamespaceFile); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			opts = append(opts, resource.WithAttributes(semconv.K8SNamespaceName(ns)))
		}
	}
	return opts
}

// TraceFields returns the active span's trace and span IDs as logger
// key-values, so a log line and the trace of the reconcile that wrote it can
// be found from each other. It returns nil when there is no recording span,
// which is always the case with tracing off.
func TraceFields(ctx context.Context) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []any{"trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String()}
}

// TraceKubernetesRequests wraps the Kubernetes client's transport so each
// request to the API server becomes a span under the span of whatever made
// it: the finalizer update, the status patch, the ConfigMap and Secret
// applies. Reads served from the informer cache never reach the transport, so
// only real API traffic shows up. Watches are skipped: they stay open for
// minutes and would arrive as one enormous span.
func TraceKubernetesRequests(next http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(next,
		otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Query().Get("watch") == "" }),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return "kube-apiserver " + r.Method
		}),
	)
}
