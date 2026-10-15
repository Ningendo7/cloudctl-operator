package controller

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/observability"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/dynamodb"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/s3"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sns"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sqs"
)

// sectionLabel is the metrics label for a section: its condition type,
// lowercased, without the "Ready" suffix ("SQSReady" -> "sqs").
func sectionLabel(conditionType string) string {
	return strings.ToLower(strings.TrimSuffix(conditionType, "Ready"))
}

// sectionResult classifies one section's outcome for the
// cloudctl_section_reconcile_total metric, in the same precedence
// setSectionCondition uses for the condition's reason.
//
// "refused" is a non-retryable error that names a stable reason of its own
// (AdoptionConflict, InstanceMissing, LegacyKMSKeyRef, ...): the operator
// deliberately declined to act and a human has to change something.
func sectionResult(err error) string {
	var reconcileErr *cloudctlaws.ReconcileError
	switch {
	case err == nil:
		return observability.SectionReady
	case cloudctlaws.IsPermissionDenied(err):
		return observability.SectionPermissionDenied
	case isRetryable(err):
		return observability.SectionTransient
	case errors.As(err, &reconcileErr) && reconcileErr.Reason != "":
		return observability.SectionRefused
	default:
		return observability.SectionError
	}
}

// observeSection records one section's outcome.
func observeSection(conditionType string, err error) {
	observability.SectionReconciles.WithLabelValues(sectionLabel(conditionType), sectionResult(err)).Inc()
}

// observeSectionDuration records how long one section took.
func observeSectionDuration(conditionType string, start time.Time) {
	observability.SectionReconcileDuration.WithLabelValues(sectionLabel(conditionType)).Observe(time.Since(start).Seconds())
}

// PendingDeletionGracePeriods gives, per ledger resource type, how long a
// resource may sit pending deletion before the state collector reports it
// stuck: each package's own grace period, so the metric and the
// StuckPendingDeletion condition agree.
func PendingDeletionGracePeriods() map[string]time.Duration {
	return map[string]time.Duration{
		resourceTypeSQS:      sqs.PendingDeletionGracePeriod,
		resourceTypeSNS:      sns.PendingDeletionGracePeriod,
		resourceTypeDynamoDB: dynamodb.PendingDeletionGracePeriod,
		resourceTypeS3:       s3.PendingDeletionGracePeriod,
		resourceTypeRDS:      rds.PendingDeletionGracePeriod,
	}
}

// startSpan starts a span as a child of whatever span ctx carries, and returns
// the context to use inside it plus the function that ends it, recording err
// on the way. With tracing off the tracer is a no-op and this costs nothing.
func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	ctx, span := observability.Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// startSectionSpan starts the span for one section's phase ("reconcile" or
// "finalize"). The AWS calls and Kubernetes API requests the section makes
// nest under it.
func startSectionSpan(ctx context.Context, conditionType, phase string) (context.Context, func(error)) {
	section := sectionLabel(conditionType)
	return startSpan(ctx, phase+" "+section, attribute.String("cloudctl.section", section))
}

// startReconcileSpan starts the root span for one reconcile, and returns a
// context whose logger also carries the trace ID, so the logs of a slow or
// failed reconcile can be found from its trace and the other way round.
// Names and namespaces are fine on a span, unlike on a metric label.
func startReconcileSpan(ctx context.Context, req ctrl.Request) (context.Context, func(error)) {
	ctx, end := startSpan(ctx, "Reconcile",
		attribute.String("cloudctl.namespace", req.Namespace),
		attribute.String("cloudctl.name", req.Name),
	)
	return logf.IntoContext(ctx, observability.LoggerWithTrace(ctx, logf.FromContext(ctx))), end
}
