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
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	equality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/observability"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/configmap"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// checkpointFor returns a Checkpoint that writes ledger onto cr and persists
// it immediately, rather than waiting for this whole reconcile pass's single
// trailing write — closing the gap where a crash between two AWS calls of a
// multi-step create (KMS's CreateKey/CreateAlias, S3's
// CreateBucket/PutBucketTagging) would otherwise lose the in-memory record
// of the first call's result.
//
// # Patches rather than updates
//
// original must be the object as it stood at the very start of this
// reconcile, captured once and shared by every section — not a fresh
// DeepCopy taken when this particular section starts. A per-section
// snapshot would already contain earlier sections' in-memory-only condition
// changes (nothing's reached the server yet on a brand new CR), making them
// look unchanged to this diff and dropping them from the patch; the
// Patch() response then overwrites cr with the server's version, silently
// erasing conditions that were never actually persisted.
func checkpointFor(r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) status.Checkpoint {
	return func(ctx context.Context, ledger []depsv1alpha1.ManagedResource) error {
		cr.Status.ManagedResources = ledger
		return r.Status().Patch(ctx, cr, statusPatch(original, cr))
	}
}

// statusPatch diffs cr against original, ignoring resourceVersion: an
// earlier write in this pass (the finalizer update, a checkpoint) moves
// cr's version past original's, and a diff that included it would make the
// patch fail with a conflict if anything else touched the CR meanwhile.
func statusPatch(original, cr *depsv1alpha1.AppDependencies) client.Patch {
	from := original.DeepCopy()
	from.ResourceVersion = cr.ResourceVersion
	return client.MergeFrom(from)
}

func statusNeedsPatch(versionBefore string, original, cr *depsv1alpha1.AppDependencies) bool {
	return cr.ResourceVersion != versionBefore || !equality.Semantic.DeepEqual(original.Status, cr.Status)
}

// eventRecorderFor adapts r.Recorder into a status.EventRecorder bound to
// cr - the one place a resource package's plain (eventType, reason, message)
// report becomes an actual Kubernetes Event, so packages like kms don't need
// to depend on the events API themselves. Returns nil if r.Recorder is unset
// (test fixtures that build a reconciler directly, bypassing cmd/main.go's
// mgr.GetEventRecorder call) - callers already treat a nil
// status.EventRecorder as "don't report events." action is reused as reason
// since this codebase has no separate action taxonomy; note is a format
// string so a literal '%' in message is never misread as a verb.
func eventRecorderFor(r *AppDependenciesReconciler, cr *depsv1alpha1.AppDependencies) status.EventRecorder {
	if r.Recorder == nil {
		return nil
	}
	return func(eventType, reason, message string) {
		observability.Events.WithLabelValues(eventType, reason).Inc()
		r.Recorder.Eventf(cr, nil, eventType, reason, reason, "%s", message)
	}
}

// DriftDetectionInterval is how often a healthy CR is re-reconciled even
// without a spec change, to catch out-of-band AWS-side drift (e.g. someone
// deletes a queue via the console) that no Kubernetes watch can see.
const DriftDetectionInterval = 5 * time.Minute

// driftRequeueJitterFraction keeps a fleet of CRs reconciled around the
// same time from staying locked in step on every later requeue too.
const driftRequeueJitterFraction = 0.2

func jitteredDriftRequeue() time.Duration {
	return jittered(DriftDetectionInterval)
}

// jittered spreads d by up to driftRequeueJitterFraction either way.
func jittered(d time.Duration) time.Duration {
	span := float64(d) * driftRequeueJitterFraction
	return d + time.Duration(rand.Float64()*2*span-span) //nolint:gosec // jitter, not a secret
}

// transientRequeueInterval is how soon a transient AWS error (throttling,
// eventual consistency) is first retried - short, since the SDK's own
// retryer has already been through its internal backoff before an error like
// this ever reached us. Each further failure of the same CR waits twice as
// long, up to transientMaxDelay.
const transientRequeueInterval = 30 * time.Second

// transientMaxDelay caps that backoff. It is well under the drift interval
// so a waiting state (an instance still creating, a grant not there yet) is
// never noticed more than a couple of minutes late.
const transientMaxDelay = 2 * time.Minute

// transientLongAfter is the number of consecutive failures (about 17 minutes
// of them) after which a CR is retried at the drift pace: a wait that long
// is no blip, and polling it every couple of minutes only spends calls.
const transientLongAfter = 10

// Resource-type tags shared across every section/ledger lookup in this
// package - named constants rather than repeated literals so a typo can't
// diverge between, say, a section's own sectionContext call and its
// cross-reference lookup in shared_with_status.go/consume_status.go.
const (
	resourceTypeSQS      = "sqs"
	resourceTypeSNS      = "sns"
	resourceTypeDynamoDB = "dynamodb"
	resourceTypeS3       = "s3"
	resourceTypeRDS      = "rds"

	conditionTypeSQSReady            = "SQSReady"
	conditionTypeSNSReady            = "SNSReady"
	conditionTypeDynamoDBReady       = "DynamoDBReady"
	conditionTypeS3Ready             = "S3Ready"
	conditionTypeRDSReady            = "RDSReady"
	conditionTypeKMSReady            = "KMSReady"
	conditionTypeAlarmsReady         = "AlarmsReady"
	conditionTypeIAMReady            = "IAMReady"
	conditionTypeConnectionInfoReady = "ConnectionInfoReady"
)

// sectionTypes lists every per-section Ready condition type this CR can
// produce, used to compute the aggregate Ready condition. Kept in sync
// with allSections above — each entry here should have a matching
// section constructor registered there.
var sectionTypes = []string{
	conditionTypeSQSReady, conditionTypeSNSReady, conditionTypeDynamoDBReady, conditionTypeS3Ready, conditionTypeRDSReady,
	conditionTypeKMSReady, conditionTypeAlarmsReady, conditionTypeIAMReady, conditionTypeConnectionInfoReady,
}

type section struct {
	name      string
	reconcile func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error
	// finalize's blocked return lists a human-readable reason per ledger
	// entry still waiting to drain (nil if done is true or err != nil) -
	// threaded up to finalizeDesiredState so it can report *why* deletion
	// hasn't completed instead of leaving status silent about it.
	finalize func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (done bool, blocked []string, err error)
}

// ForceDeleteAllAnnotation lets a human override a Delete-policy resource's
// non-empty guard specifically to unstick a CR's deletion - never read
// outside the finalize path. This exists because the guard's Force field
// only ever gets refreshed from spec while a resource is still declared
// there; once it's been removed from spec (the common way a CR ends up
// here at all), there is no spec field left to flip back to true, so an
// explicit, CR-level, deliberately-separate escape hatch is the only way
// back short of stripping the finalizer outright and accepting an orphan.
const ForceDeleteAllAnnotation = "cloudctl.io/force-delete-all"

// allSections lists every resource-type section this CR reconciles, in
// dependency order — kmsSection runs first, since other sections' kmsKeyRef
// resolves against keys it just wrote to the ledger, and iamSection runs
// last, since deriving this CR's IAM policy reads the ARNs the other
// sections just wrote into this same reconcile pass's ledger. Adding a new resource type means adding one file
// (section_<type>.go) with its own constructor, and one line here — this
// file's size doesn't grow with the number of resource types.
func allSections(r *AppDependenciesReconciler, original *depsv1alpha1.AppDependencies) []section {
	return []section{
		kmsSection(r, original),
		sqsSection(r, original),
		snsSection(r, original),
		dynamodbSection(r, original),
		s3Section(r, original),
		rdsSection(r, original),
		alarmsSection(r),
		iamSection(r),
	}
}

// finalizeOrder is allSections with kms moved last: a key must outlive the
// resources encrypted with it, the reverse of the order they're created in.
func finalizeOrder(sections []section) []section {
	ordered := make([]section, 0, len(sections))
	var keys []section
	for _, s := range sections {
		if s.name == conditionTypeKMSReady {
			keys = append(keys, s)
			continue
		}
		ordered = append(ordered, s)
	}
	return append(ordered, keys...)
}

func ensureDesiredState(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) error {
	// Shares one DescribeDBInstances call per RDS instance across this
	// whole pass - rdsSection's own status check, and the ConfigMap/Secret
	// connection-info delivery below, would otherwise each describe the
	// same instance independently. See rds.WithDescribeCache.
	ctx = rds.WithDescribeCache(ctx)

	checkpoint := checkpointFor(r, cr, original)
	lastCheckpointed := cr.Status.DeepCopy()
	var firstErr error
	// Whether every failure of this pass is a wait that a watch will end.
	onlyWaits := true
	record := func(err error) {
		if err == nil {
			return
		}
		if firstErr == nil {
			firstErr = err
		}
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.WakesOnChange {
			onlyWaits = false
		}
	}
	for _, s := range allSections(r, original) {
		sectionStart := time.Now()
		sectionCtx, endSection := startSectionSpan(ctx, s.name, "reconcile")
		err := s.reconcile(sectionCtx, cr)
		endSection(err)
		record(err)
		observeSectionDuration(s.name, sectionStart)
		if !equality.Semantic.DeepEqual(cr.Status, *lastCheckpointed) {
			record(checkpoint(ctx, cr.Status.ManagedResources))
			lastCheckpointed = cr.Status.DeepCopy()
		}
	}
	warnIfLedgerLarge(cr, eventRecorderFor(r, cr))
	checkSharedWithReferences(ctx, r.Client, cr)
	checkConsumeReferences(ctx, r.Client, cr)

	// Runs last, after every section has written this pass's ARNs into the
	// ledger: the ConfigMap it generates is only as fresh as that ledger
	// data, so anything reconciled earlier in this same pass is already
	// reflected in it, not lagging a full reconcile behind. The mirrored
	// RDS credentials Secret is the same kind of connection-info delivery
	// (just the one piece of it that's secret, not identifiers), so it
	// shares this same condition and timing rather than getting its own.
	connCtx, endConn := startSectionSpan(ctx, conditionTypeConnectionInfoReady, "reconcile")
	connCtx, cancelConn := connectionInfoContext(connCtx, cr)
	defer cancelConn()
	connErr := configmap.Ensure(connCtx, r.Client, r.APIReader, r.AWSClients.RDS, r.AWSClients.Region, r.AWSClients.AccountID, cr)
	if connErr == nil {
		connErr = rds.EnsureCredentialsSecret(connCtx, r.AWSClients.SecretsManager, r.AWSClients.RDS, r.Client, r.APIReader, cr)
	}
	endConn(connErr)
	setSectionCondition(ctx, cr, conditionTypeConnectionInfoReady, connErr, eventRecorderFor(r, cr))
	record(connErr)
	if firstErr != nil && onlyWaits {
		return waitingError{firstErr}
	}
	return firstErr
}

// finalizeDesiredState runs cleanup for a CR that's being deleted, tearing
// down (or relinquishing, per each resource's deletionPolicy) everything in
// the ledger. done is false if at least one resource is still waiting to
// drain before it can actually be deleted (PendingDeletion /
// StuckPendingDeletion) — the caller must keep the finalizer in place in
// that case, or the resource would be silently abandoned once the CR
// disappears, with nothing left to ever check on it again.
func finalizeDesiredState(ctx context.Context, r *AppDependenciesReconciler, cr *depsv1alpha1.AppDependencies) (done bool, err error) {
	allDone := true
	var firstErr error
	var blocked []string
	// original is nil here: finalize closures only ever call Cleanup, never
	// checkpointFor, so there's nothing that would dereference it.
	for _, s := range finalizeOrder(allSections(r, nil)) {
		sectionCtx, endSection := startSectionSpan(ctx, s.name, "finalize")
		done, sectionBlocked, err := s.finalize(sectionCtx, cr)
		endSection(err)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			allDone = false
			continue
		}
		if !done {
			allDone = false
			blocked = append(blocked, sectionBlocked...)
		}
	}

	// A hard error already gets its own visibility via the deletion
	// reconcile's own log line and standard requeue/backoff; this condition
	// is specifically for the silent case - nothing failed, it's just
	// waiting, potentially forever, on a guard with no remaining way to
	// clear itself from spec.
	if !allDone && firstErr == nil {
		status.SetDeletionBlocked(&cr.Status.Conditions, cr.Generation, blocked)
	}

	return allDone, firstErr
}

func setSectionCondition(ctx context.Context, cr *depsv1alpha1.AppDependencies, conditionType string, err error, recordEvent status.EventRecorder) {
	observeSection(conditionType, err)
	if err == nil {
		status.SetSectionCondition(
			&cr.Status.Conditions,
			conditionType,
			metav1.ConditionTrue,
			reasons.Reconciled, "reconciled successfully",
			cr.Generation,
			sectionTypes,
		)
		return
	}

	reason := reasons.Error
	var reconcileErr *cloudctlaws.ReconcileError
	switch {
	case errors.As(err, &reconcileErr) && reconcileErr.Reason != "":
		reason = reconcileErr.Reason
	case cloudctlaws.IsPermissionDenied(err):
		reason = reasons.PermissionDenied
	case isRetryable(err):
		reason = reasons.TransientError
	}

	// The log keeps the full error; the condition and event get it without
	// AWS request IDs, which would make every pass look like a change.
	logf.FromContext(ctx).Error(err, "Section failed to reconcile", "section", conditionType, "reason", reason)
	message := status.StripRequestIDs(err.Error())

	// A Warning event here, not just the condition below: status.conditions
	// alone is invisible to the standard `kubectl describe`/`get events`
	// triage flow, which only shows Events. It carries the condition's
	// reason, with the section in the text; a reason whose own code already
	// records an event is not recorded twice.
	if recordEvent != nil && !reasons.EmitsOwnEvent(reason) {
		recordEvent("Warning", reason, fmt.Sprintf("%s: %s", conditionType, message))
	}

	status.SetSectionCondition(
		&cr.Status.Conditions,
		conditionType,
		metav1.ConditionFalse,
		reason,
		message,
		cr.Generation,
		sectionTypes,
	)
}

// waitingError is a pass's error when every failure was a wait that a watch
// will end, so the CR needn't be retried at the fast transient pace.
type waitingError struct{ error }

func (e waitingError) Unwrap() error { return e.error }

// isRetryable trusts a ReconcileError's own verdict, and otherwise
// classifies whatever AWS or network error is in the chain, so an error
// returned without a ReconcileError isn't treated as permanent.
func isRetryable(err error) bool {
	var reconcileErr *cloudctlaws.ReconcileError
	if errors.As(err, &reconcileErr) {
		return reconcileErr.Retryable
	}
	return cloudctlaws.IsRetryable(err)
}

// largeLedgerEntries is when a ledger is reported as large. Status stops
// accepting writes at about 4,500 entries.
const largeLedgerEntries = 300

func warnIfLedgerLarge(cr *depsv1alpha1.AppDependencies, recordEvent status.EventRecorder) {
	n := len(cr.Status.ManagedResources)
	if n <= largeLedgerEntries || recordEvent == nil {
		return
	}
	recordEvent("Warning", "LedgerLarge", fmt.Sprintf("This CR tracks %d resources. Entries for retained encrypted resources and RDS instances stay until the CR is deleted, so a CR that keeps replacing them grows without limit; use a new CR for new resources", n))
}

// arnRegion returns the region of an ARN, or "" for one without a region
// (S3, IAM) and for anything that is not an ARN, such as a snapshot ID.
func arnRegion(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 || parts[0] != "arn" {
		return ""
	}
	return parts[3]
}

// sectionForEntryType is the condition a ledger entry's refusal is shown on.
func sectionForEntryType(entryType string) string {
	switch {
	case strings.HasPrefix(entryType, "sqs"):
		return conditionTypeSQSReady
	case strings.HasPrefix(entryType, "sns"):
		return conditionTypeSNSReady
	case strings.HasPrefix(entryType, "s3"):
		return conditionTypeS3Ready
	case strings.HasPrefix(entryType, "dynamodb"):
		return conditionTypeDynamoDBReady
	case strings.HasPrefix(entryType, "iam"):
		return conditionTypeIAMReady
	case strings.HasPrefix(entryType, "rds"), strings.HasPrefix(entryType, "ec2"):
		return conditionTypeRDSReady
	}
	return conditionTypeKMSReady
}

// refuseOtherRegion refuses a CR whose ledger names a resource in another
// region than this operator's. Resources do not move between regions, and
// looking for them here would find some missing and recreate or forget them.
func refuseOtherRegion(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	region := r.AWSClients.Region
	if region == "" {
		return false, nil
	}
	for _, entry := range cr.Status.ManagedResources {
		if other := arnRegion(entry.ARN); other != "" && other != region {
			setSectionCondition(ctx, cr, sectionForEntryType(entry.Type), &cloudctlaws.ReconcileError{
				Err: fmt.Errorf("%s %q is in %s but this operator works in %s; resources do not move between regions, so this CR needs an operator in %s. Nothing is reconciled meanwhile",
					entry.Type, entry.Name, other, region, other),
				Reason: reasons.RegionChanged,
			}, eventRecorderFor(r, cr))
			return true, r.Status().Patch(ctx, cr, statusPatch(original, cr))
		}
	}
	return false, nil
}
