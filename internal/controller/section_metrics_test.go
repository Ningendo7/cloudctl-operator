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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/go-logr/logr/funcr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/observability"
)

// fataler is what the helpers need from a test, satisfied by *testing.T and
// by Ginkgo's GinkgoT() alike.
type fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

func counterValue(t fataler, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	var m dto.Metric
	if err := vec.WithLabelValues(labels...).Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	return m.GetCounter().GetValue()
}

func histogramCount(t fataler, vec *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	observer, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues() error = %v", err)
	}
	var m dto.Metric
	if err := observer.(prometheus.Metric).Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestSectionLabel(t *testing.T) {
	for in, want := range map[string]string{
		"SQSReady": "sqs", "SNSReady": "sns", "DynamoDBReady": "dynamodb", "S3Ready": "s3",
		"RDSReady": "rds", "KMSReady": "kms", "AlarmsReady": "alarms", "IAMReady": "iam",
		"ConnectionInfoReady": "connectioninfo",
	} {
		if got := sectionLabel(in); got != want {
			t.Errorf("sectionLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSectionResult(t *testing.T) {
	denied := &smithy.GenericAPIError{Code: "AccessDenied", Message: "no"}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"success":                          {nil, observability.SectionReady},
		"an AWS permission error":          {denied, observability.SectionPermissionDenied},
		"a wrapped permission error":       {&cloudctlaws.ReconcileError{Err: denied}, observability.SectionPermissionDenied},
		"a retryable error":                {&cloudctlaws.ReconcileError{Err: errors.New("busy"), Retryable: true}, observability.SectionTransient},
		"a retryable error with reason":    {&cloudctlaws.ReconcileError{Err: errors.New("creating"), Retryable: true, Reason: "InstanceCreating"}, observability.SectionTransient},
		"a refusal with its own reason":    {&cloudctlaws.ReconcileError{Err: errors.New("no"), Reason: "AdoptionConflict"}, observability.SectionRefused},
		"a missing-instance refusal":       {&cloudctlaws.ReconcileError{Err: errors.New("gone"), Reason: "InstanceMissing"}, observability.SectionRefused},
		"anything else":                    {errors.New("boom"), observability.SectionError},
		"a reconcile error with no reason": {&cloudctlaws.ReconcileError{Err: errors.New("boom")}, observability.SectionError},
	} {
		if got := sectionResult(tc.err); got != tc.want {
			t.Errorf("%s: sectionResult() = %q, want %q", name, got, tc.want)
		}
	}
}

// setSectionCondition is where every section's outcome is decided, so it is
// where each outcome is counted.
func TestSetSectionCondition_CountsTheOutcome(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{}
	cr.Name, cr.Namespace = "x", "default"

	for _, tc := range []struct {
		conditionType, section, result string
		err                            error
	}{
		{"SQSReady", "sqs", observability.SectionReady, nil},
		{"RDSReady", "rds", observability.SectionPermissionDenied, &smithy.GenericAPIError{Code: "AccessDenied"}},
		{"IAMReady", "iam", observability.SectionTransient, &cloudctlaws.ReconcileError{Err: errors.New("busy"), Retryable: true}},
		{"KMSReady", "kms", observability.SectionRefused, &cloudctlaws.ReconcileError{Err: errors.New("no"), Reason: "LegacyKMSKeyRef"}},
		{"S3Ready", "s3", observability.SectionError, errors.New("boom")},
	} {
		before := counterValue(t, observability.SectionReconciles, tc.section, tc.result)
		setSectionCondition(context.Background(), cr, tc.conditionType, tc.err, nil)
		if got := counterValue(t, observability.SectionReconciles, tc.section, tc.result) - before; got != 1 {
			t.Errorf("section_reconcile_total{section=%s,result=%s} rose by %v, want 1", tc.section, tc.result, got)
		}
	}
}

func TestObserveSectionDuration(t *testing.T) {
	before := histogramCount(t, observability.SectionReconcileDuration, "dynamodb")
	observeSectionDuration("DynamoDBReady", time.Now().Add(-250*time.Millisecond))
	if got := histogramCount(t, observability.SectionReconcileDuration, "dynamodb") - before; got != 1 {
		t.Errorf("duration samples rose by %d, want 1", got)
	}
}

// Every event the operator emits is counted, on the same path that creates
// it, so a reason added anywhere later is counted without further wiring.
func TestEventRecorder_CountsEveryEvent(t *testing.T) {
	r := &AppDependenciesReconciler{Recorder: events.NewFakeRecorder(10)}
	cr := &depsv1alpha1.AppDependencies{}
	cr.Name, cr.Namespace = "x", "default"
	record := eventRecorderFor(r, cr)

	for _, e := range []struct{ eventType, reason string }{
		{"Warning", "QueueMissing"}, {"Warning", "QueueMissing"}, {"Normal", "QueueCreated"},
	} {
		before := counterValue(t, observability.Events, e.eventType, e.reason)
		record(e.eventType, e.reason, "message that must not become a label")
		if got := counterValue(t, observability.Events, e.eventType, e.reason) - before; got != 1 {
			t.Errorf("events_total{type=%s,reason=%s} rose by %v, want 1", e.eventType, e.reason, got)
		}
	}
}

// The collector's "stuck" must use the same thresholds as the condition.
func TestPendingDeletionGracePeriods(t *testing.T) {
	want := map[string]time.Duration{
		"sqs": 7 * 24 * time.Hour, "sns": 7 * 24 * time.Hour, "dynamodb": 7 * 24 * time.Hour,
		"s3": 7 * 24 * time.Hour, "rds": time.Hour,
	}
	got := PendingDeletionGracePeriods()
	for typ, d := range want {
		if got[typ] != d {
			t.Errorf("grace for %s = %v, want %v", typ, got[typ], d)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d types, want %d: %v", len(got), len(want), got)
	}
}

// cleaner is what useSpanRecorder needs from a test: *testing.T, or
// Ginkgo's GinkgoT().
type cleaner interface {
	fataler
	Cleanup(func())
}

func useSpanRecorder(t cleaner) *tracetest.SpanRecorder {
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

func spanNamed(t fataler, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("no span named %q among %d spans", name, len(spans))
	return nil
}

func TestStartSpan_RecordsAnErrorOnTheSpan(t *testing.T) {
	recorder := useSpanRecorder(t)
	_, end := startSpan(context.Background(), "work")
	end(errors.New("boom"))

	span := spanNamed(t, recorder.Ended(), "work")
	if span.Status().Code != codes.Error || span.Status().Description != "boom" {
		t.Errorf("expected an error status carrying the message, got %+v", span.Status())
	}
	if len(span.Events()) == 0 {
		t.Error("expected the error recorded as a span event")
	}
}

func TestStartSpan_NoErrorLeavesTheStatusUnset(t *testing.T) {
	recorder := useSpanRecorder(t)
	_, end := startSpan(context.Background(), "work")
	end(nil)
	if got := spanNamed(t, recorder.Ended(), "work").Status().Code; got != codes.Unset {
		t.Errorf("status = %v, want Unset", got)
	}
}

func TestStartSectionSpan_NamesTheSectionAndPhase(t *testing.T) {
	recorder := useSpanRecorder(t)
	_, endReconcile := startSectionSpan(context.Background(), "DynamoDBReady", "reconcile")
	endReconcile(nil)
	_, endFinalize := startSectionSpan(context.Background(), "RDSReady", "finalize")
	endFinalize(nil)

	spans := recorder.Ended()
	reconcile := spanNamed(t, spans, "reconcile dynamodb")
	if !hasAttribute(reconcile, "cloudctl.section", "dynamodb") {
		t.Errorf("expected cloudctl.section=dynamodb, got %v", reconcile.Attributes())
	}
	spanNamed(t, spans, "finalize rds")
}

func hasAttribute(span sdktrace.ReadOnlySpan, key, value string) bool {
	for _, a := range span.Attributes() {
		if string(a.Key) == key && a.Value.AsString() == value {
			return true
		}
	}
	return false
}

// The log lines of a reconcile carry its trace ID, which is how its logs are
// found from its trace and the other way round.
func TestStartReconcileSpan_PutsTheTraceIDInTheLogger(t *testing.T) {
	recorder := useSpanRecorder(t)
	var lines []string
	logger := funcr.New(func(prefix, args string) { lines = append(lines, prefix+args) }, funcr.Options{})
	ctx := logf.IntoContext(context.Background(), logger)

	ctx, end := startReconcileSpan(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "checkout"}})
	logf.FromContext(ctx).Info("inside the reconcile")
	end(nil)

	root := spanNamed(t, recorder.Ended(), "Reconcile")
	if !hasAttribute(root, "cloudctl.namespace", "team-a") || !hasAttribute(root, "cloudctl.name", "checkout") {
		t.Errorf("expected the CR's namespace and name on the root span, got %v", root.Attributes())
	}
	if len(lines) != 1 || !strings.Contains(lines[0], root.SpanContext().TraceID().String()) {
		t.Errorf("expected the root span's trace ID in the log line, got %v", lines)
	}
}
