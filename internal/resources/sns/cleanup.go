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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
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
// since a topic holds no backlog of its own. forceDeleteAll overrides every
// entry's own Force value - meant only for the finalize path, see the sqs
// package's Cleanup for why this exists.
func Cleanup(
	ctx context.Context,
	client snsAPI,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SNSSpec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
	forceDeleteAll bool,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, t := range spec.Resources {
			declared[t.Name] = true
		}
	}

	c := &topicCleaner{client: client, namespace: namespace, crName: crName, crUID: crUID, forceDeleteAll: forceDeleteAll, recordEvent: recordEvent}
	updatedLedger = ledger
	var firstErr error
	for _, entry := range ledger {
		if entry.Type != resourceType {
			continue
		}

		var entryErr error
		switch {
		case declared[entry.Name]:
			updatedLedger, entryErr = c.restoreDeclared(ctx, entry, updatedLedger)
		case entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete:
			updatedLedger, results, entryErr = c.retain(ctx, entry, updatedLedger, results)
		default:
			updatedLedger, results, entryErr = c.deleteUndeclared(ctx, entry, updatedLedger, results)
		}
		if entryErr != nil && firstErr == nil {
			firstErr = entryErr
		}
	}

	return updatedLedger, results, firstErr
}

// topicCleaner carries what every entry in one Cleanup pass shares.
type topicCleaner struct {
	client                   snsAPI
	namespace, crName, crUID string
	forceDeleteAll           bool
	recordEvent              status.EventRecorder
}

// restoreDeclared lifts the deny from a topic that is declared again, and
// cancels its pending deletion.
func (c *topicCleaner) restoreDeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	if entry.PendingDeletionSince == nil {
		return ledger, nil
	}
	if err := removePendingDeletionDeny(ctx, c.client, entry.ARN); err != nil {
		return ledger, err
	}
	cleared := entry
	cleared.PendingDeletionSince = nil
	cleared.LastEmptyCheckAt = nil
	status.UpsertManagedResource(&ledger, cleared)
	if c.recordEvent != nil {
		c.recordEvent("Normal", "TopicDeletionCancelled", fmt.Sprintf("Canceled pending deletion of topic %s (%s); resource reappeared in spec", entry.Name, entry.ARN))
	}
	return ledger, nil
}

// retain lets go of an undeclared topic whose policy is not Delete, and
// rechecks a relinquished one about daily. An entry is kept only while it
// pins a KMS key.
func (c *topicCleaner) retain(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
	retained := CleanupResult{Name: entry.Name, Reason: CleanupReasonRetained}
	pinsKey := kms.PinsAKey(ledger, resourceType, entry)
	if entry.State == depsv1alpha1.ManagedResourceStateRelinquished {
		if !pinsKey {
			// Let go earlier and pins no key: nothing to remember.
			status.RemoveManagedResource(&ledger, resourceType, entry.Name)
			return ledger, append(results, retained), nil
		}
		if !status.NeedsRelinquishedRecheck(entry) {
			return ledger, append(results, retained), nil
		}
	}

	relinquished, gone, err := relinquishIfStillTagged(ctx, c.client, c.namespace, c.crName, c.crUID, entry, c.recordEvent)
	if err != nil {
		return ledger, results, err
	}
	if gone {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
		return ledger, results, nil
	}
	if relinquished && c.recordEvent != nil {
		c.recordEvent("Normal", "TopicOwnershipRelinquished", fmt.Sprintf("Relinquished ownership of retained topic %s (%s) - no longer declared in spec", entry.Name, entry.ARN))
	}
	if pinsKey {
		status.UpsertManagedResource(&ledger, status.Relinquished(entry))
	} else {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	}
	return ledger, append(results, retained), nil
}

// deleteUndeclared deletes an undeclared Delete-policy topic once its quiet
// window has passed and it has no subscriptions, unless force skips both.
func (c *topicCleaner) deleteUndeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
	forced := entry.Force || c.forceDeleteAll

	// A pending topic waiting out the quiet window or backoff reads nothing;
	// ownership is re-checked below, before anything changes it.
	if !forced && entry.PendingDeletionSince != nil &&
		(time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow || !status.NeedsEmptyCheck(entry)) {
		return ledger, append(results, CleanupResult{Name: entry.Name, Reason: pendingDeletionReason(entry.PendingDeletionSince.Time)}), nil
	}

	gone, err := c.verifyOwned(ctx, entry)
	if gone {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
		return ledger, results, nil
	}
	if err != nil {
		return ledger, results, err
	}

	if !forced {
		var proceed bool
		ledger, results, proceed, err = c.gateOnSubscriptions(ctx, entry, ledger, results)
		if err != nil || !proceed {
			return ledger, results, err
		}
	}

	ledger, err = c.deleteTopic(ctx, entry, ledger)
	return ledger, results, err
}

// verifyOwned re-reads the topic's tags before anything acts on the ledger's
// word. gone is true when the topic no longer exists.
func (c *topicCleaner) verifyOwned(ctx context.Context, entry depsv1alpha1.ManagedResource) (gone bool, err error) {
	tagsOut, err := c.client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &entry.ARN})
	if err != nil {
		// ListTagsForResource raises ResourceNotFoundException, not the
		// plain NotFoundException other topic operations use.
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return true, nil
		}
		return false, wrapAWSError(err, fmt.Sprintf("re-verifying ownership of topic %q before delete", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.Tags), c.namespace, c.crName, c.crUID) {
		return false, cloudctlaws.OwnershipLostError("topic", entry.Name, "delete it")
	}
	return false, nil
}

// gateOnSubscriptions holds a topic's deletion until it has been pending for
// the quiet window and has no subscriptions. proceed is false while it must
// still wait.
func (c *topicCleaner) gateOnSubscriptions(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) (updated []depsv1alpha1.ManagedResource, updatedResults []CleanupResult, proceed bool, err error) {
	if entry.PendingDeletionSince == nil {
		// First time this topic has come up for deletion. Never
		// delete on the same pass it's first noticed, even if it
		// looks empty right now — deny new activity and hold for
		// the quiet window first.
		if err := addPendingDeletionDeny(ctx, c.client, entry.ARN); err != nil {
			return ledger, results, false, wrapAWSError(err, fmt.Sprintf("blocking new activity on topic %q pending deletion", entry.Name))
		}
		ledger, results = markPendingDeletion(ledger, results, entry)
		if c.recordEvent != nil {
			c.recordEvent("Warning", "TopicDeletionPending", fmt.Sprintf("Topic %s (%s) is no longer declared and will be deleted in %s unless it reappears in spec", entry.Name, entry.ARN, deletionQuietWindow))
		}
		return ledger, results, false, nil
	}

	hasSubs, err := hasSubscriptions(ctx, c.client, entry.ARN)
	if err != nil {
		return ledger, results, false, wrapAWSError(err, fmt.Sprintf("checking topic %q for active subscriptions", entry.Name))
	}
	if hasSubs {
		// Quiet window elapsed and it's genuinely in use — stays
		// denied and pending, now under the long human-reaction
		// grace period rather than the short propagation-safety one.
		checked := entry
		now := metav1.Now()
		checked.LastEmptyCheckAt = &now
		ledger, results = markPendingDeletion(ledger, results, checked)
		return ledger, results, false, nil
	}
	return ledger, results, true, nil
}

// deleteTopic deletes the topic and drops its ledger entry.
func (c *topicCleaner) deleteTopic(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	if _, err := c.client.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: &entry.ARN,
	}); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("deleting topic %q", entry.Name))
	}
	if c.recordEvent != nil {
		c.recordEvent("Warning", "TopicDeleted", fmt.Sprintf("Deleted topic %s (%s)", entry.Name, entry.ARN))
	}
	status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	return ledger, nil
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
func relinquishIfStillTagged(ctx context.Context, client snsAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource, recordEvent status.EventRecorder) (relinquished, gone bool, err error) {
	tagsOut, tErr := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: &entry.ARN,
	})
	if tErr != nil {
		// ListTagsForResource raises ResourceNotFoundException, not the
		// plain NotFoundException other topic operations use.
		var notFound *types.ResourceNotFoundException
		if errors.As(tErr, &notFound) {
			return false, true, nil // already gone, nothing to relinquish
		}
		return false, false, wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained topic %q", entry.Name))
	}
	currentTags := tagsToMap(tagsOut.Tags)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return false, false, nil // already relinquished, or never verified as ours
	}

	if released := cloudctlaws.ReleaseTags(ctx, namespace, crName, time.Now()); released != nil {
		if _, tagErr := client.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &entry.ARN, Tags: mapToTags(released)}); tagErr != nil {
			cloudctlaws.ReleaseTagFailed(recordEvent, "topic", entry.Name, tagErr)
		}
	}
	if _, uErr := client.UntagResource(ctx, &sns.UntagResourceInput{
		ResourceArn: &entry.ARN,
		TagKeys:     []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return false, false, wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained topic %q", entry.Name))
	}

	if err := removePendingDeletionDeny(ctx, client, entry.ARN); err != nil {
		return false, false, err
	}
	return true, false, nil
}
