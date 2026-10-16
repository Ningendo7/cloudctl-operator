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

package kms

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// deletionQuietWindow is how long a key sits noticed-as-no-longer-needed
// before Cleanup ever calls ScheduleKeyDeletion — the same propagation-
// safety reasoning as every other resource type's quiet window, guarding
// against acting on a removal that's about to be reverted.
const deletionQuietWindow = 10 * time.Minute

// scheduledDeletionWindowDays is the PendingWindowInDays passed to
// ScheduleKeyDeletion — AWS's own maximum (30, also its default), not the
// minimum (7) it allows. This project's own quiet window already provides
// one layer of safety before this call is ever made; using AWS's longest
// window on top of that maximizes the chance a mistake is still
// reversible (via CancelKeyDeletion) by the time anyone notices. A KMS key
// deletion is uniquely unrecoverable among every resource type this
// operator manages, since it can make data held by a completely
// different, still-live resource permanently unreadable — not just delete
// data of its own.
const scheduledDeletionWindowDays = 30

type CleanupReason string

const (
	// CleanupReasonRetained means deletionPolicy is Retain — permanent by
	// design, not expected to ever auto-delete.
	CleanupReasonRetained CleanupReason = "Retained"
	// CleanupReasonPendingDeletion means deletionPolicy is Delete but the
	// key is still within this operator's own quiet window. Unlike every
	// other resource type there's no "stuck" variant: once the quiet
	// window elapses, ScheduleKeyDeletion either succeeds (the ledger
	// entry is removed — AWS's own 30-day window takes over from there)
	// or fails as an ordinary retryable/non-retryable error, never as an
	// indefinitely-blocked state waiting on some other condition to change.
	CleanupReasonPendingDeletion CleanupReason = "PendingDeletion"
	// CleanupReasonInUse means the CR is being deleted but resources
	// encrypted with the key are still in the ledger, draining. The key waits
	// for them: deleting it first would make their data unreadable.
	CleanupReasonInUse CleanupReason = "InUseByEncryptedData"
)

// KeyUse says a key still has data encrypted with it, from the caller's
// view of the ledger. Retained is true when at least one of those resources
// is kept on purpose (deletionPolicy Retain, or a final snapshot): the key
// must then outlive this CR too, whatever its own deletionPolicy says.
type KeyUse struct {
	Retained bool
}

// CleanupResult reports what happened to a ledger entry Cleanup did not
// delete, so status can surface *why* instead of one flat "orphaned"
// bucket that can't distinguish "retained on purpose" from "still waiting."
type CleanupResult struct {
	Name   string
	Reason CleanupReason
}

// Cleanup finds ledger entries for kms resources no longer declared in
// spec (or every kms entry, if deleting is true) and either schedules
// their deletion, retains-and-relinquishes them, or holds them pending
// deletion during the quiet window, depending on their captured
// deletionPolicy.
//
// inUse carries the ledger names of keys that other resources still need,
// computed by the controller from both spec and ledger: dedicated keys
// provisioned via EnsureDedicatedKey, keys referenced through kmsKeyRef, and
// keys of resources that have left spec but still hold data (pending
// deletion, retained, or an RDS final snapshot). Those keys are never
// declared in spec.KMS.Resources — nothing about them is user-visible —
// so without this, this function's own declared-vs-ledger diff would see
// every dedicated key as "not declared" and try to delete it on every
// single reconcile, regardless of whether the resource that owns it still
// exists. Treated identically to spec.KMS.Resources entries otherwise:
// still declared means still needed, clearing any in-progress pending
// deletion.
//
// Unlike every other resource type, there's no "is it empty" check here
// in the usual sense — a KMS key has no AWS-queryable signal analogous to
// a message, item, or object count for "is anything still using this to
// encrypt data." Keys are never shared across CRs, so the caller passes in
// every key this CR's own resources still reference (see inUse). Something encrypted with this key entirely
// outside this operator's bookkeeping is invisible to that. The quiet
// window, that list, and AWS's own PendingDeletion window are the only
// safety layers, and there is no force override to skip any of them —
// force-deleting a key can make still-live data permanently unreadable.
func Cleanup(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID string,
	spec *depsv1alpha1.KMSSpec,
	inUse map[string]KeyUse,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := declaredKeyNames(spec, inUse, deleting)

	updatedLedger = ledger
	var firstErr error
	for _, entry := range ledger {
		if entry.Type != resourceType {
			continue
		}

		if use, needed := inUse[entry.Name]; deleting && needed {
			held, waiting := holdKeyForData(entry, use, recordEvent)
			if waiting {
				results = append(results, CleanupResult{Name: entry.Name, Reason: CleanupReasonInUse})
				continue
			}
			entry = held
		}

		if declared[entry.Name] {
			if entry.PendingDeletionSince != nil {
				cleared := entry
				cleared.PendingDeletionSince = nil
				status.UpsertManagedResource(&updatedLedger, cleared)
			}
			continue
		}

		if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
			if entry.State != depsv1alpha1.ManagedResourceStateRelinquished {
				relinquished, relErr := relinquishIfStillTagged(ctx, client, namespace, crName, crUID, entry)
				if relErr != nil {
					if firstErr == nil {
						firstErr = relErr
					}
					continue
				}
				if relinquished && recordEvent != nil {
					recordEvent("Normal", "KeyOwnershipRelinquished", fmt.Sprintf("Relinquished ownership of retained KMS key %s (%s) - no longer declared in spec", entry.Name, entry.ARN))
				}
				status.UpsertManagedResource(&updatedLedger, status.Relinquished(entry))
			}
			results = append(results, CleanupResult{Name: entry.Name, Reason: CleanupReasonRetained})
			continue
		}

		describeOut, dErr := client.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: &entry.ARN})
		if dErr != nil {
			var notFound *types.NotFoundException
			if errors.As(dErr, &notFound) {
				// Already gone in AWS — just drop it from the ledger.
				status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
				continue
			}
			if firstErr == nil {
				firstErr = wrapAWSError(dErr, fmt.Sprintf("looking up KMS key %q before delete", entry.Name))
			}
			continue
		}
		if describeOut.KeyMetadata.KeyState == types.KeyStatePendingDeletion {
			// Already scheduled from a previous pass — nothing left for us
			// to do but wait out AWS's own window; our ledger entry for it
			// was already removed the pass ScheduleKeyDeletion succeeded.
			continue
		}

		tags, tErr := listAllResourceTags(ctx, client, entry.ARN)
		if tErr != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of KMS key %q before delete", entry.Name))
			}
			continue
		}
		if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, crUID) {
			if firstErr == nil {
				firstErr = fmt.Errorf("KMS key %q no longer verified as owned by this CR — refusing to delete it", entry.Name)
			}
			continue
		}

		if entry.PendingDeletionSince == nil {
			// First time this key has come up for deletion — never act on
			// the same pass it's first noticed, giving a moment for a
			// spec change that's about to be reverted.
			updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
			if recordEvent != nil {
				recordEvent("Warning", "KeyDeletionPending", fmt.Sprintf("KMS key %s (%s) is no longer declared and will be scheduled for deletion in %s unless it reappears in spec", entry.Name, entry.ARN, deletionQuietWindow))
			}
			continue
		}
		if time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow {
			results = append(results, CleanupResult{Name: entry.Name, Reason: CleanupReasonPendingDeletion})
			continue
		}

		windowDays := int32(scheduledDeletionWindowDays)
		if _, sErr := client.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
			KeyId:               &entry.ARN,
			PendingWindowInDays: &windowDays,
		}); sErr != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(sErr, fmt.Sprintf("scheduling deletion of KMS key %q", entry.Name))
			}
			continue
		}
		if recordEvent != nil {
			recordEvent("Warning", "KeyDeletionScheduled", fmt.Sprintf("Scheduled KMS key %s (%s) for deletion in %d days", entry.Name, entry.ARN, scheduledDeletionWindowDays))
		}
		status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
	}

	return updatedLedger, results, firstErr
}

// declaredKeyNames is the set of ledger names Cleanup treats as still wanted:
// the keys declared under kms.resources and every key in use. While the CR is
// being deleted nothing is declared, and inUse is handled key by key instead.
func declaredKeyNames(spec *depsv1alpha1.KMSSpec, inUse map[string]KeyUse, deleting bool) map[string]bool {
	declared := map[string]bool{}
	if deleting {
		return declared
	}
	if spec != nil {
		for _, k := range spec.Resources {
			declared[k.Name] = true
		}
	}
	for name := range inUse {
		declared[name] = true
	}
	return declared
}

// holdKeyForData decides what a CR's deletion does to a key that data is
// still encrypted with. If that data is draining towards deletion, the key
// waits (waiting is true). If it is retained, the key is retained too,
// whatever its own deletionPolicy said, so it is relinquished like any
// retained key instead of scheduled for deletion.
func holdKeyForData(entry depsv1alpha1.ManagedResource, use KeyUse, recordEvent status.EventRecorder) (held depsv1alpha1.ManagedResource, waiting bool) {
	if !use.Retained {
		return entry, true
	}
	if recordEvent != nil && entry.DeletionPolicy == depsv1alpha1.DeletionPolicyDelete {
		recordEvent("Warning", "KeyRetainedForData", fmt.Sprintf("Keeping KMS key %s (%s) despite its Delete policy: resources encrypted with it are retained", entry.Name, entry.ARN))
	}
	entry.DeletionPolicy = depsv1alpha1.DeletionPolicyRetain
	return entry, false
}

func markPendingDeletion(
	ledger []depsv1alpha1.ManagedResource,
	results []CleanupResult,
	entry depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, []CleanupResult) {
	since := entry.PendingDeletionSince
	if since == nil {
		now := metav1.Now()
		since = &now
	}
	updated := entry
	updated.PendingDeletionSince = since
	status.UpsertManagedResource(&ledger, updated)
	return ledger, append(results, CleanupResult{Name: entry.Name, Reason: CleanupReasonPendingDeletion})
}

// relinquishIfStillTagged removes our ownership tag from a resource whose
// deletionPolicy is Retain and is no longer declared — we're explicitly
// saying we no longer manage it, so the AWS-side tag shouldn't keep
// claiming otherwise. The ledger keeps the entry for visibility; only the
// AWS-side ownership claim is relinquished. Idempotent — safe on every
// reconcile pass.
func relinquishIfStillTagged(ctx context.Context, client kmsAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource) (relinquished bool, err error) {
	describeOut, err := client.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: &entry.ARN})
	if err != nil {
		var notFound *types.NotFoundException
		if errors.As(err, &notFound) {
			return false, nil // already gone, nothing to relinquish
		}
		return false, wrapAWSError(err, fmt.Sprintf("checking state of retained key %q", entry.Name))
	}
	if describeOut.KeyMetadata.KeyState == types.KeyStatePendingDeletion {
		return false, nil // already being deleted (out-of-band, or a previous pass), nothing to relinquish
	}

	tags, tErr := listAllResourceTags(ctx, client, entry.ARN)
	if tErr != nil {
		return false, wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained KMS key %q", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, crUID) {
		return false, nil // already relinquished, or never verified as ours — don't touch it
	}

	if _, uErr := client.UntagResource(ctx, &kms.UntagResourceInput{
		KeyId:   &entry.ARN,
		TagKeys: []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return false, wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained KMS key %q", entry.Name))
	}
	return true, nil
}
