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

package sns

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const PendingDeletionGracePeriod = 7 * 24 * time.Hour

// deletionQuietWindow is how long a topic sits denied-and-held before we
// ever trust an "empty" read enough to delete it. ListSubscriptionsByTopic
// carries no documented consistency guarantee, so a Subscribe call that
// landed moments before we decided to delete could still be invisible to us
// on the very next call. This is unrelated to PendingDeletionGracePeriod,
// which is about giving a human time to react to a topic that's genuinely
// still in use — this window is purely about API propagation safety and
// applies even to topics that look empty from the very first check.
const deletionQuietWindow = 10 * time.Minute

type CleanupReason string

const (
	CleanupReasonRetained             CleanupReason = "Retained"
	CleanupReasonPendingDeletion      CleanupReason = "PendingDeletion"
	CleanupReasonStuckPendingDeletion CleanupReason = "StuckPendingDeletion"
)

type CleanupResult struct {
	Name   string
	Reason CleanupReason
}

// Cleanup finds ledger entries for sns resources no longer declared in
// spec (or every sns entry, if deleting is true) and either deletes them,
// retains-and-relinquishes them, or marks them pending deletion if they
// still have active subscriptions — SNS's equivalent of a non-empty queue,
// since a topic holds no backlog of its own.
func Cleanup(
	ctx context.Context,
	client snsAPI,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SNSSpec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, t := range spec.Resources {
			declared[t.Name] = true
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
				if clearErr := removePendingDeletionDeny(ctx, client, entry.ARN); clearErr != nil {
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
			results = append(results, CleanupResult{Name: entry.Name, Reason: CleanupReasonRetained})
			continue
		}

		tagsOut, tErr := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &entry.ARN})
		if tErr != nil {
			var notFound *types.NotFoundException
			if errors.As(tErr, &notFound) {
				status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
				continue
			}
			if firstErr == nil {
				firstErr = wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of topic %q before delete", entry.Name))
			}
			continue
		}
		if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.Tags), namespace, crName, crUID) {
			if firstErr == nil {
				firstErr = fmt.Errorf("topic %q no longer verified as owned by this CR — refusing to delete it", entry.Name)
			}
			continue
		}

		if !entry.Force {
			if entry.PendingDeletionSince == nil {
				// First time this topic has come up for deletion. Never
				// delete on the same pass it's first noticed, even if it
				// looks empty right now — deny new activity and hold for
				// the quiet window first.
				if denyErr := addPendingDeletionDeny(ctx, client, entry.ARN); denyErr != nil {
					if firstErr == nil {
						firstErr = wrapAWSError(denyErr, fmt.Sprintf("blocking new activity on topic %q pending deletion", entry.Name))
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

			hasSubs, subErr := hasSubscriptions(ctx, client, entry.ARN)
			if subErr != nil {
				if firstErr == nil {
					firstErr = wrapAWSError(subErr, fmt.Sprintf("checking topic %q for active subscriptions", entry.Name))
				}
				continue
			}
			if hasSubs {
				// Quiet window elapsed and it's genuinely in use — stays
				// denied and pending, now under the long human-reaction
				// grace period rather than the short propagation-safety one.
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}
		}

		if _, dErr := client.DeleteTopic(ctx, &sns.DeleteTopicInput{
			TopicArn: &entry.ARN,
		}); dErr != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(dErr, fmt.Sprintf("deleting topic %q", entry.Name))
			}
			continue
		}
		status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
	}
	
	return updatedLedger, results, firstErr
}

func hasSubscriptions(ctx context.Context, client snsAPI, topicArn string) (bool, error) {
	var nextToken *string
	for {
		out, err := client.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
			TopicArn:  &topicArn,
			NextToken: nextToken,
		})
		if err != nil {
			return false, err
		}
		if len(out.Subscriptions) > 0 {
			return true, nil
		}
		if out.NextToken == nil {
			return false, nil
		}
		nextToken = out.NextToken
	}
}

func pendingDeletionReason(since time.Time) CleanupReason {
	if time.Since(since) > PendingDeletionGracePeriod {
		return CleanupReasonStuckPendingDeletion
	}
	return CleanupReasonPendingDeletion
}

func markPendingDeletion(ledger []depsv1alpha1.ManagedResource, results []CleanupResult, entry depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, []CleanupResult) {
	since := entry.PendingDeletionSince
	if since == nil {
		now := metav1.Now()
		since = &now
	}

	updated := entry
	updated.PendingDeletionSince = since
	status.UpsertManagedResource(&ledger, updated)

	return ledger, append(results, CleanupResult{Name: entry.Name, Reason: pendingDeletionReason(since.Time)})
}

// relinquishIfStillTagged removes our ownership tag (and any leftover
// pending-deletion deny statement) from a resource whose deletionPolicy is
// Retain and is no longer declared. Idempotent — safe on every reconcile.
func relinquishIfStillTagged(ctx context.Context, client snsAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource) error {
	tagsOut, tErr := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: &entry.ARN,
	})
	if tErr != nil {
		var notFound *types.NotFoundException
		if errors.As(tErr, &notFound) {
			return nil // already gone, nothing to relinquish
		}
		return wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained topic %q", entry.Name))
	}
	currentTags := tagsToMap(tagsOut.Tags)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return nil // already relinquished, or never verified as ours
	}

	if _, uErr := client.UntagResource(ctx, &sns.UntagResourceInput{
		ResourceArn: &entry.ARN,
		TagKeys:     []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained topic %q", entry.Name))
	}

	return removePendingDeletionDeny(ctx, client, entry.ARN)
}
