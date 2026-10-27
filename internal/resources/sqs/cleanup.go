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
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
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
// on their captured deletionPolicy and current state. forceDeleteAll
// overrides every entry's own (possibly stale, possibly unreachable now
// that it's left spec) Force value - meant only for the finalize path, to
// unstick a CR whose deletion can otherwise never complete once a
// non-empty-guarded resource has no spec entry left to flip force:true on.
func Cleanup(
	ctx context.Context,
	sqsClient sqsAPI,
	snsClient cloudctlaws.SNSClient,
	k8sClient client.Client,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SQSSpec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
	forceDeleteAll bool,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared, subscribesToByName := declaredQueues(spec, deleting)

	c := &queueCleaner{sqsClient: sqsClient, namespace: namespace, crName: crName, crUID: crUID, forceDeleteAll: forceDeleteAll, recordEvent: recordEvent}
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

	var subErr error
	updatedLedger, subErr = cleanupQueueSubscriptions(ctx, sqsClient, snsClient, k8sClient, namespace, crName, ledger, updatedLedger, subscribesToByName, recordEvent)
	if subErr != nil && firstErr == nil {
		firstErr = subErr
	}

	return updatedLedger, results, firstErr
}

// declaredQueues names the queues spec still declares, a DLQ included, and
// what each subscribes to. Both are empty when nothing is declared.
func declaredQueues(spec *depsv1alpha1.SQSSpec, deleting bool) (declared map[string]bool, subscribesToByName map[string][]depsv1alpha1.ConsumeRef) {
	declared = map[string]bool{}
	subscribesToByName = map[string][]depsv1alpha1.ConsumeRef{}
	if spec == nil || deleting {
		return declared, subscribesToByName
	}
	for _, q := range spec.Resources {
		declared[q.Name] = true
		subscribesToByName[q.Name] = q.SubscribesTo
		if q.DLQ {
			declared[cloudctlaws.DerivedKey(q.Name, "dlq")] = true
		}
	}
	return declared, subscribesToByName
}

// queueCleaner carries what every entry in one Cleanup pass shares.
type queueCleaner struct {
	sqsClient                sqsAPI
	namespace, crName, crUID string
	forceDeleteAll           bool
	recordEvent              status.EventRecorder
}

// restoreDeclared lifts the send-blocking deny from a queue that is declared
// again, and cancels its pending deletion.
func (c *queueCleaner) restoreDeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	if entry.PendingDeletionSince == nil {
		return ledger, nil
	}
	if err := clearPendingDeletion(ctx, c.sqsClient, entry); err != nil {
		return ledger, err
	}
	cleared := entry
	cleared.PendingDeletionSince = nil
	cleared.LastEmptyCheckAt = nil
	status.UpsertManagedResource(&ledger, cleared)
	if c.recordEvent != nil {
		c.recordEvent("Normal", "QueueDeletionCancelled", fmt.Sprintf("Canceled pending deletion of queue %s (%s); resource reappeared in spec", entry.Name, entry.ARN))
	}
	return ledger, nil
}

// retain lets go of an undeclared queue whose policy is not Delete, and
// rechecks a relinquished one about daily. An entry is kept only while it
// pins a KMS key.
func (c *queueCleaner) retain(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
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

	relinquished, gone, err := relinquishIfStillTagged(ctx, c.sqsClient, c.namespace, c.crName, c.crUID, entry, c.recordEvent)
	if err != nil {
		return ledger, results, err
	}
	if gone {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
		return ledger, results, nil
	}
	if relinquished && c.recordEvent != nil {
		c.recordEvent("Normal", "QueueOwnershipRelinquished", fmt.Sprintf("Relinquished ownership of retained queue %s (%s) - no longer declared in spec", entry.Name, entry.ARN))
	}
	if pinsKey {
		status.UpsertManagedResource(&ledger, status.Relinquished(entry))
	} else {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	}
	return ledger, append(results, retained), nil
}

// deleteUndeclared deletes an undeclared Delete-policy queue once its quiet
// window has passed and it is empty, unless force skips both.
func (c *queueCleaner) deleteUndeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
	forced := entry.Force || c.forceDeleteAll

	// A pending queue waiting out the quiet window or backoff reads nothing;
	// ownership is re-checked below, before anything changes it.
	if !forced && entry.PendingDeletionSince != nil &&
		(time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow || !status.NeedsEmptyCheck(entry)) {
		return ledger, append(results, CleanupResult{Name: entry.Name, Reason: pendingDeletionReason(entry.PendingDeletionSince.Time)}), nil
	}

	queueURL, gone, err := c.verifyOwned(ctx, entry)
	if gone {
		// Already gone in AWS - just drop it from the ledger.
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
		return ledger, results, nil
	}
	if err != nil {
		return ledger, results, err
	}

	if !forced {
		var proceed bool
		ledger, results, proceed, err = c.gateOnEmpty(ctx, entry, queueURL, ledger, results)
		if err != nil || !proceed {
			return ledger, results, err
		}
	}

	ledger, err = c.deleteQueue(ctx, entry, queueURL, ledger)
	return ledger, results, err
}

// verifyOwned resolves the queue's URL and re-reads its tags before anything
// acts on the ledger's word. gone is true when the queue no longer exists.
func (c *queueCleaner) verifyOwned(ctx context.Context, entry depsv1alpha1.ManagedResource) (queueURL string, gone bool, err error) {
	queueName, err := queueNameFromARN(entry.ARN)
	if err != nil {
		return "", false, err
	}
	urlOut, err := c.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		var notFound *types.QueueDoesNotExist
		if errors.As(err, &notFound) {
			return "", true, nil
		}
		// Anything else (throttling, a permission gap, a network blip)
		// must NOT be treated as "gone" - doing so would silently drop
		// a queue that still exists from the ledger, abandoning it
		// without ever actually deleting or retaining it per policy.
		return "", false, wrapAWSError(err, fmt.Sprintf("resolving queue URL for %q before delete", entry.Name))
	}

	tagsOut, err := c.sqsClient.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: urlOut.QueueUrl,
	})
	if err != nil {
		return "", false, wrapAWSError(err, fmt.Sprintf("re-verifying ownership of queue %q before delete", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, c.namespace, c.crName, c.crUID) {
		return "", false, cloudctlaws.OwnershipLostError("queue", entry.Name, "delete it")
	}
	return *urlOut.QueueUrl, false, nil
}

// gateOnEmpty holds a queue's deletion until it has been pending for the
// quiet window and is empty. proceed is false while it must still wait.
func (c *queueCleaner) gateOnEmpty(ctx context.Context, entry depsv1alpha1.ManagedResource, queueURL string, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) (updated []depsv1alpha1.ManagedResource, updatedResults []CleanupResult, proceed bool, err error) {
	if entry.PendingDeletionSince == nil {
		// First time this queue has come up for deletion. Never
		// delete on the same pass it's first noticed, even if it
		// looks empty right now — GetQueueAttributes' counters are
		// approximate/eventually consistent, so a message sent
		// moments ago could still not be reflected. Deny new sends
		// and hold for the quiet window first.
		if err := addPendingDeletionDeny(ctx, c.sqsClient, queueURL, entry.ARN); err != nil {
			return ledger, results, false, wrapAWSError(err, fmt.Sprintf("blocking new sends to queue %q pending deletion", entry.Name))
		}
		ledger, results = markPendingDeletion(ledger, results, entry)
		if c.recordEvent != nil {
			c.recordEvent("Warning", "QueueDeletionPending", fmt.Sprintf("Queue %s (%s) is no longer declared and will be deleted in %s unless it reappears in spec", entry.Name, entry.ARN, deletionQuietWindow))
		}
		return ledger, results, false, nil
	}

	attrs, err := c.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &queueURL,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return ledger, results, false, wrapAWSError(err, fmt.Sprintf("checking queue %q is empty", entry.Name))
	}
	if !queueIsEmpty(attrs.Attributes) {
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

// queueIsEmpty reads the queue's message counters. A queue can show zero
// visible messages while still holding in-flight messages a consumer is
// actively processing, or delayed messages not yet readable - these are
// independent counters, not aliases of each other, so all three have to be
// zero for the queue to actually be empty.
func queueIsEmpty(attrs map[string]string) bool {
	return attrs[string(types.QueueAttributeNameApproximateNumberOfMessages)] == "0" &&
		attrs[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] == "0" &&
		attrs[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)] == "0"
}

// deleteQueue deletes the queue and drops its ledger entry.
func (c *queueCleaner) deleteQueue(ctx context.Context, entry depsv1alpha1.ManagedResource, queueURL string, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	if _, err := c.sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: &queueURL,
	}); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("deleting queue %q", entry.Name))
	}
	if c.recordEvent != nil {
		c.recordEvent("Warning", "QueueDeleted", fmt.Sprintf("Deleted queue %s (%s)", entry.Name, entry.ARN))
	}
	status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	return ledger, nil
}

// cleanupQueueSubscriptions reconciles subscriptions as a second pass over
// the ledger, separate from the deletion state machine: a still-declared
// queue needs its subscriptions reasserted/pruned against its current
// subscribesTo every pass (not just on deletion), and a queue dropped from
// spec needs every subscription it ever had torn down regardless of whether
// it ends up Retained, pending deletion, or actually deleted this same pass
// - an active delivery path, not a grant, so it can't just be left to expire
// on its own. It walks the queues as the pass began (startLedger): a retained
// queue's entry may have just left the ledger, but its subscriptions still
// need tearing down.
func cleanupQueueSubscriptions(
	ctx context.Context,
	sqsClient sqsAPI,
	snsClient cloudctlaws.SNSClient,
	k8sClient client.Client,
	namespace, crName string,
	startLedger, ledger []depsv1alpha1.ManagedResource,
	subscribesToByName map[string][]depsv1alpha1.ConsumeRef,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	var firstErr error
	for _, entry := range startLedger {
		if entry.Type != resourceType {
			continue
		}
		queueName, nameErr := queueNameFromARN(entry.ARN)
		if nameErr != nil {
			if firstErr == nil {
				firstErr = nameErr
			}
			continue
		}
		// Without a tracked subscription there is nothing to tear down, and
		// the queue's URL is only needed to do that.
		if !hasSubscriptionEntries(ledger, queueName) {
			continue
		}
		urlOut, uErr := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
		queueURL := ""
		var gone *types.QueueDoesNotExist
		switch {
		case uErr == nil:
			queueURL = *urlOut.QueueUrl
		case errors.As(uErr, &gone):
			// SNS keeps a queue's subscriptions after the queue is deleted,
			// and every publish to the topic then fails delivery for them.
			// They are torn down by ARN, with no queue policy to edit.
		default:
			if firstErr == nil {
				firstErr = wrapAWSError(uErr, fmt.Sprintf("resolving queue URL to tear down the subscriptions of %q", queueName))
			}
			continue
		}

		subscribesTo, stillDeclared := subscribesToByName[entry.Name]
		if !stillDeclared {
			subscribesTo = nil // tear down every subscription this queue has
		}

		var subErr error
		ledger, subErr = CleanupSubscriptions(ctx, snsClient, sqsClient, k8sClient, namespace, crName, queueName, queueURL, subscribesTo, ledger, recordEvent)
		if subErr != nil && firstErr == nil {
			firstErr = subErr
		}
	}
	return ledger, firstErr
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
func clearPendingDeletion(ctx context.Context, sqsClient sqsAPI, entry depsv1alpha1.ManagedResource) error {
	queueName, nameErr := queueNameFromARN(entry.ARN)
	if nameErr != nil {
		return nameErr
	}
	urlOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		var notFound *types.QueueDoesNotExist
		if errors.As(err, &notFound) {
			return nil // already gone, nothing to clean up
		}
		return wrapAWSError(err, fmt.Sprintf("looking up queue %q to clear its pending-deletion deny", entry.Name))
	}
	return removePendingDeletionDeny(ctx, sqsClient, *urlOut.QueueUrl)
}

// relinquishIfStillTagged removes our ownership tag (and any leftover
// pending-deletion deny statement) from a resource whose deletionPolicy is
// Retain and is no longer declared — we're explicitly saying we no longer
// manage it, so the AWS-side tag shouldn't keep claiming otherwise. The
// ledger keeps the entry for visibility; only the AWS-side ownership claim
// is relinquished. Idempotent — safe on every reconcile pass.
func relinquishIfStillTagged(ctx context.Context, sqsClient sqsAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource, recordEvent status.EventRecorder) (relinquished, gone bool, err error) {
	queueName, nameErr := queueNameFromARN(entry.ARN)
	if nameErr != nil {
		return false, false, nameErr
	}
	urlOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		var notFound *types.QueueDoesNotExist
		if errors.As(err, &notFound) {
			return false, true, nil // already gone, nothing to relinquish
		}
		return false, false, wrapAWSError(err, fmt.Sprintf("looking up retained queue %q", entry.Name))
	}

	tagsOut, tErr := sqsClient.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: urlOut.QueueUrl,
	})
	if tErr != nil {
		return false, false, wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained queue %q", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, crUID) {
		return false, false, nil // already relinquished, or never verified as ours - don't touch it
	}

	if released := cloudctlaws.ReleaseTags(ctx, namespace, crName, time.Now()); released != nil {
		if _, tagErr := sqsClient.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: urlOut.QueueUrl, Tags: released}); tagErr != nil {
			cloudctlaws.ReleaseTagFailed(recordEvent, "queue", entry.Name, tagErr)
		}
	}
	if _, uErr := sqsClient.UntagQueue(ctx, &sqs.UntagQueueInput{
		QueueUrl: urlOut.QueueUrl,
		TagKeys:  []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return false, false, wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained queue %q", entry.Name))
	}

	if err := removePendingDeletionDeny(ctx, sqsClient, *urlOut.QueueUrl); err != nil {
		return false, false, err
	}
	return true, false, nil
}
