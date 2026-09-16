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
	"fmt"
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
	for _, entry := range ledger {
		if entry.Type != resourceType {
			continue
		}

		if declared[entry.Name] {
			if entry.PendingDeletionSince != nil {
				if clearErr := clearPendingDeletion(ctx, client, namespace, crName, entry); clearErr != nil {
					return updatedLedger, results, clearErr
				}
				cleared := entry
				cleared.PendingDeletionSince = nil
				status.UpsertManagedResource(&updatedLedger, cleared)
			}
			continue
		}

		if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
			if relErr := relinquishIfStillTagged(ctx, client, namespace, crName, crUID, entry); relErr != nil {
				return updatedLedger, results, relErr
			}
			results = append(results, CleanupResult{
				Name:   entry.Name,
				Reason: CleanupReasonRetained,
			})
			continue
		}

		queueName := cloudctlaws.ResourceName(namespace, crName, entry.Name)
		urlOut, uErr := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: &queueName,
		})
		if uErr != nil {
			// Already gone in AWS - just drop it from the ledger.
			status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
			continue
		}

		tagsOut, tErr := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
			QueueUrl: urlOut.QueueUrl,
		})
		if tErr != nil {
			return updatedLedger, results, wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of queue %q before delete", entry.Name))
		}
		if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, crUID) {
			return updatedLedger, results, fmt.Errorf("queue %q no longer verified as owned by this CR — refusing to delete it", entry.Name)
		}

		if !entry.Force {
			attrs, aErr := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl:       urlOut.QueueUrl,
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
			})
			if aErr != nil {
				return updatedLedger, results, wrapAWSError(aErr, fmt.Sprintf("checking queue %q is empty", entry.Name))
			}
			if attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] != "0" {
				if denyErr := addPendingDeletionDeny(ctx, client, *urlOut.QueueUrl, entry.ARN); denyErr != nil {
					return updatedLedger, results, wrapAWSError(denyErr, fmt.Sprintf("blocking new sends to queue %q pending deletion", entry.Name))
				}
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}
		}

		if _, dErr := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: urlOut.QueueUrl,
		}); dErr != nil {
			return updatedLedger, results, wrapAWSError(dErr, fmt.Sprintf("deleting queue %q", entry.Name))
		}
		status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
	}

	return updatedLedger, results, nil
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

	reason := CleanupReasonPendingDeletion
	if time.Since(since.Time) > PendingDeletionGracePeriod {
		reason = CleanupReasonStuckPendingDeletion
	}
	return ledger, append(results, CleanupResult{
		Name:   entry.Name,
		Reason: reason,
	})
}

// clearPendingDeletion removes the send-blocking deny statement from a
// resource that's returned to spec after having been marked pending
// deletion — it's back in active use, nothing should still be blocking
// sends to it.
func clearPendingDeletion(ctx context.Context, client sqsAPI, namespace, crName string, entry depsv1alpha1.ManagedResource) error {
	queueName := cloudctlaws.ResourceName(namespace, crName, entry.Name)
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
	queueName := cloudctlaws.ResourceName(namespace, crName, entry.Name)
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
