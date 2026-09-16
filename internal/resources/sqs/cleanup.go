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

package sqs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// PendingDeletionGracePeriod is how long a Delete-policy resource is
// allowed to sit non-empty before its status escalates from "waiting for
// it to drain" to "stuck, needs manual attention." We never auto-force-
// delete once this expires — silently destroying data because a clock ran
// out would be worse than the problem it's meant to catch.
const PendingDeletionGracePeriod = 7 * 24 * time.Hour

// deletionQuietWindow is how long a queue sits denied-and-held before we
// ever trust an "empty" read enough to delete it. GetQueueAttributes'
// message counters are explicitly documented by AWS as approximate/
// eventually consistent, so a message sent moments before we decided to
// delete could still not be reflected on the very next call. This is
// unrelated to PendingDeletionGracePeriod, which is about giving a human
// time to react to a queue that's genuinely still in use — this window is
// purely about API propagation safety and applies even to queues that look
// empty from the very first check.
const deletionQuietWindow = 10 * time.Minute

type CleanupReason string

const (
	// CleanupReasonRetained means deletionPolicy is Retain - permanent by
	// design, not expected to ever auto-delete.
	CleanupReasonRetained CleanupReason = "Retained"
	// CleanupReasonPendingDeletion means deletionPolicy is Delete but the
	// queue is still non-empty, within the grace period.
	CleanupReasonPendingDeletion CleanupReason = "PendingDeletion"
	// CleanupReasonStuckPendingDeletion means the same as above but the
	// grace period has expired - needs a human to look at it.
	CleanupReasonStuckPendingDeletion CleanupReason = "StuckPendingDeletion"
)

// CleanupResult reports what happened to a ledger entry Cleanup did not
// delete, so status can surface *why* instead of one flat "orphaned"
// bucket that can't distinguish "retained on purpose" from "stuck."
type CleanupResult struct {
	Name   string
	Reason CleanupReason
}

// queueNameFromARN extracts the queue name (FIFO suffix included, if any)
// from a stored ARN instead of recomputing it from namespace/crName/key —
// more robust in general, and necessary for FIFO queues specifically,
// since reconstructing the name would need to know fifo-ness even after
// the spec entry (and that information) is long gone from spec.
func queueNameFromARN(arn string) (string, error) {
	idx := strings.LastIndex(arn, ":")
	if idx == -1 || idx == len(arn)-1 {
		return "", fmt.Errorf("unexpected queue ARN format: %s", arn)
	}
	return arn[idx+1:], nil
}

// Cleanup finds ledger entries for sqs resources no longer declared in
// spec (or every sqs entry, if deleting is true) and either deletes them,
// retains-and-relinquishes them, or marks them pending deletion, depending
// on their captured deletionPolicy and current state.
func Cleanup(
	ctx context.Context,
	client sqsAPI,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SQSSpec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, q := range spec.Resources {
			declared[q.Name] = true
			if q.DLQ {
				declared[q.Name+"-dlq"] = true
			}
		}
	}

	updatedLedger = ledger
	var firstErr error
	for _, entry := range ledger {
		if entry.Type != resourceType {
			continue
		}

		if declared[entry.Name] {
			if entry.PendingDeletionSince != nil {
				if clearErr := clearPendingDeletion(ctx, client, namespace, crName, entry); clearErr != nil {
					if firstErr == nil {
						firstErr = clearErr
					}
					continue
				}
				cleared := entry
				cleared.PendingDeletionSince = nil
				status.UpsertManagedResource(&updatedLedger, cleared)
			}
			continue
		}

		if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
			if relErr := relinquishIfStillTagged(ctx, client, namespace, crName, crUID, entry); relErr != nil {
				if firstErr == nil {
					firstErr = relErr
				}
				continue
			}
			results = append(results, CleanupResult{
				Name:   entry.Name,
				Reason: CleanupReasonRetained,
			})
			continue
		}

		queueName, nameErr := queueNameFromARN(entry.ARN)
		if nameErr != nil {
			if firstErr == nil {
				firstErr = nameErr
			}
			continue
		}
		urlOut, uErr := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: &queueName,
		})
		if uErr != nil {
			var notFound *types.QueueDoesNotExist
			if errors.As(uErr, &notFound) {
				// Already gone in AWS - just drop it from the ledger.
				status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
				continue
			}
			// Anything else (throttling, a permission gap, a network blip)
			// must NOT be treated as "gone" - doing so would silently drop
			// a queue that still exists from the ledger, abandoning it
			// without ever actually deleting or retaining it per policy.
			if firstErr == nil {
				firstErr = wrapAWSError(uErr, fmt.Sprintf("resolving queue URL for %q before delete", entry.Name))
			}
			continue
		}

		tagsOut, tErr := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
			QueueUrl: urlOut.QueueUrl,
		})
		if tErr != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of queue %q before delete", entry.Name))
			}
			continue
		}
		if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, crUID) {
			if firstErr == nil {
				firstErr = fmt.Errorf("queue %q no longer verified as owned by this CR — refusing to delete it", entry.Name)
			}
			continue
		}

		if !entry.Force {
			if entry.PendingDeletionSince == nil {
				// First time this queue has come up for deletion. Never
				// delete on the same pass it's first noticed, even if it
				// looks empty right now — GetQueueAttributes' counters are
				// approximate/eventually consistent, so a message sent
				// moments ago could still not be reflected. Deny new sends
				// and hold for the quiet window first.
				if denyErr := addPendingDeletionDeny(ctx, client, *urlOut.QueueUrl, entry.ARN); denyErr != nil {
					if firstErr == nil {
						firstErr = wrapAWSError(denyErr, fmt.Sprintf("blocking new sends to queue %q pending deletion", entry.Name))
					}
					continue
				}
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}

			if time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow {
				// Still inside the quiet window — the deny is already in
				// place from the first pass, nothing to do but keep waiting.
				results = append(results, CleanupResult{Name: entry.Name, Reason: pendingDeletionReason(entry.PendingDeletionSince.Time)})
				continue
			}

			attrs, aErr := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: urlOut.QueueUrl,
				AttributeNames: []types.QueueAttributeName{
					types.QueueAttributeNameApproximateNumberOfMessages,
					types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
					types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
				},
			})
			if aErr != nil {
				if firstErr == nil {
					firstErr = wrapAWSError(aErr, fmt.Sprintf("checking queue %q is empty", entry.Name))
				}
				continue
			}
			// A queue can show zero visible messages while still holding
			// in-flight messages a consumer is actively processing, or
			// delayed messages not yet readable - these are independent
			// counters, not aliases of each other, so all three have to be
			// zero for the queue to actually be empty.
			visible := attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
			inFlight := attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]
			delayed := attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)]
			if visible != "0" || inFlight != "0" || delayed != "0" {
				// Quiet window elapsed and it's genuinely in use — stays
				// denied and pending, now under the long human-reaction
				// grace period rather than the short propagation-safety one.
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}
		}

		if _, dErr := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: urlOut.QueueUrl,
		}); dErr != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(dErr, fmt.Sprintf("deleting queue %q", entry.Name))
			}
			continue
		}
		status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
	}

	return updatedLedger, results, firstErr
}

func pendingDeletionReason(since time.Time) CleanupReason {
	if time.Since(since) > PendingDeletionGracePeriod {
		return CleanupReasonStuckPendingDeletion
	}
	return CleanupReasonPendingDeletion
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

	return ledger, append(results, CleanupResult{
		Name:   entry.Name,
		Reason: pendingDeletionReason(since.Time),
	})
}

// clearPendingDeletion removes the send-blocking deny statement from a
// resource that's returned to spec after having been marked pending
// deletion — it's back in active use, nothing should still be blocking
// sends to it.
func clearPendingDeletion(ctx context.Context, client sqsAPI, namespace, crName string, entry depsv1alpha1.ManagedResource) error {
	queueName, nameErr := queueNameFromARN(entry.ARN)
	if nameErr != nil {
		return nameErr
	}
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		return nil // already gone, nothing to clean up
	}
	return removePendingDeletionDeny(ctx, client, *urlOut.QueueUrl)
}

// relinquishIfStillTagged removes our ownership tag (and any leftover
// pending-deletion deny statement) from a resource whose deletionPolicy is
// Retain and is no longer declared — we're explicitly saying we no longer
// manage it, so the AWS-side tag shouldn't keep claiming otherwise. The
// ledger keeps the entry for visibility; only the AWS-side ownership claim
// is relinquished. Idempotent — safe on every reconcile pass.
func relinquishIfStillTagged(ctx context.Context, client sqsAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource) error {
	queueName, nameErr := queueNameFromARN(entry.ARN)
	if nameErr != nil {
		return nameErr
	}
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		return nil // already gone, nothing to relinquish
	}

	tagsOut, tErr := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: urlOut.QueueUrl,
	})
	if tErr != nil {
		return wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained queue %q", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, crUID) {
		return nil // already relinquished, or never verified as ours - don't touch it
	}

	if _, uErr := client.UntagQueue(ctx, &sqs.UntagQueueInput{
		QueueUrl: urlOut.QueueUrl,
		TagKeys:  []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained queue %q", entry.Name))
	}

	return removePendingDeletionDeny(ctx, client, *urlOut.QueueUrl)
}
