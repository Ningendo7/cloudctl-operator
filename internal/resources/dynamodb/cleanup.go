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

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const PendingDeletionGracePeriod = 7 * 24 * time.Hour

// deletionQuietWindow mirrors sqs/sns's: tableIsEmpty is a strongly
// consistent, real-time read, but "empty right now" still can't rule out an
// item written a moment after this reconcile reads it. The first time a
// table comes up for deletion, it's held rather than deleted immediately,
// regardless of what that first check shows.
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

// Cleanup finds ledger entries for dynamodb resources no longer declared in
// spec (or every dynamodb entry, if deleting is true) and either deletes
// them, retains-and-relinquishes them, or marks them pending deletion.
//
// A table waiting to be emptied gets a resource policy that denies the writes
// that add data, so it can be drained but not refilled; see
// addPendingDeletionDeny. The policy goes when the table is declared again or
// retained instead.
//
// forceDeleteAll overrides every entry's own Force value - meant only for
// the finalize path, see the sqs package's Cleanup for why this exists.
func Cleanup(
	ctx context.Context,
	client dynamodbAPI,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.DynamoDBSpec,
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

	c := &tableCleaner{client: client, namespace: namespace, crName: crName, crUID: crUID, forceDeleteAll: forceDeleteAll, recordEvent: recordEvent}
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

// tableCleaner carries what every entry in one Cleanup pass shares.
type tableCleaner struct {
	client                   dynamodbAPI
	namespace, crName, crUID string
	forceDeleteAll           bool
	recordEvent              status.EventRecorder
}

// restoreDeclared cancels a pending deletion for a table that is declared
// again, and lifts the write-blocking policy from it.
func (c *tableCleaner) restoreDeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
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
		c.recordEvent("Normal", "TableDeletionCancelled", fmt.Sprintf("Canceled pending deletion of table %s (%s); resource reappeared in spec", entry.Name, entry.ARN))
	}
	return ledger, nil
}

// retain lets go of an undeclared table whose policy is not Delete, and
// rechecks a relinquished one about daily. An entry is kept only while it
// pins a KMS key.
func (c *tableCleaner) retain(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
	retained := CleanupResult{Name: entry.Name, Reason: CleanupReasonRetained}
	// Retained after waiting to be deleted: it is no longer being emptied.
	if entry.PendingDeletionSince != nil {
		if err := removePendingDeletionDeny(ctx, c.client, entry.ARN); err != nil {
			return ledger, results, err
		}
	}
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
		// Deleted by hand after being retained: forget it, so
		// declaring it again creates a fresh table instead of
		// tripping over an entry for one that no longer exists.
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
		return ledger, results, nil
	}
	if relinquished && c.recordEvent != nil {
		c.recordEvent("Normal", "TableOwnershipRelinquished", fmt.Sprintf("Relinquished ownership of retained table %s (%s) - no longer declared in spec", entry.Name, entry.ARN))
	}
	if pinsKey {
		status.UpsertManagedResource(&ledger, status.Relinquished(entry))
	} else {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	}
	return ledger, append(results, retained), nil
}

// deleteUndeclared deletes an undeclared Delete-policy table once its quiet
// window has passed and it is empty, unless force skips both.
func (c *tableCleaner) deleteUndeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
	forced := entry.Force || c.forceDeleteAll

	// A pending table waiting out the quiet window or backoff reads nothing;
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
		ledger, results, proceed, err = c.gateOnEmpty(ctx, entry, ledger, results)
		if err != nil || !proceed {
			return ledger, results, err
		}
	}

	ledger, err = c.deleteTable(ctx, entry, ledger)
	return ledger, results, err
}

// verifyOwned re-reads the table's tags before anything acts on the ledger's
// word. gone is true when the table no longer exists.
func (c *tableCleaner) verifyOwned(ctx context.Context, entry depsv1alpha1.ManagedResource) (gone bool, err error) {
	tags, err := listAllTags(ctx, c.client, entry.ARN)
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return true, nil
		}
		return false, wrapAWSError(err, fmt.Sprintf("re-verifying ownership of table %q before delete", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), c.namespace, c.crName, c.crUID) {
		return false, cloudctlaws.OwnershipLostError("table", entry.Name, "delete it")
	}
	return false, nil
}

// gateOnEmpty holds a table's deletion until it has been pending for the
// quiet window and is empty. proceed is false while it must still wait.
func (c *tableCleaner) gateOnEmpty(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) (updated []depsv1alpha1.ManagedResource, updatedResults []CleanupResult, proceed bool, err error) {
	if entry.PendingDeletionSince == nil {
		// Never delete on the same pass a table is first noticed. Deny the
		// writes that add data first, so what is emptied stays empty, and hold
		// for the quiet window.
		if err := addPendingDeletionDeny(ctx, c.client, entry.ARN); err != nil {
			return ledger, results, false, err
		}
		ledger, results = markPendingDeletion(ledger, results, entry)
		if c.recordEvent != nil {
			c.recordEvent("Warning", "TableDeletionPending", fmt.Sprintf("Table %s (%s) is no longer declared and will be deleted in %s unless it reappears in spec", entry.Name, entry.ARN, deletionQuietWindow))
		}
		return ledger, results, false, nil
	}
	empty, err := tableIsEmpty(ctx, c.client, entry.ARN)
	if err != nil {
		return ledger, results, false, wrapAWSError(err, fmt.Sprintf("checking table %q is empty", entry.Name))
	}
	if !empty {
		checked := entry
		now := metav1.Now()
		checked.LastEmptyCheckAt = &now
		ledger, results = markPendingDeletion(ledger, results, checked)
		return ledger, results, false, nil
	}
	return ledger, results, true, nil
}

// releaseDeletionProtection switches deletion protection off just before the
// operator's own delete, which every gate has already approved. The table is
// briefly UPDATING afterwards, so the delete may be retried on the next pass.
func (c *tableCleaner) releaseDeletionProtection(ctx context.Context, entry depsv1alpha1.ManagedResource, tableName string) error {
	out, err := c.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return nil
		}
		return wrapAWSError(err, fmt.Sprintf("reading table %q before delete", entry.Name))
	}
	if out.Table == nil || !aws.ToBool(out.Table.DeletionProtectionEnabled) {
		return nil
	}
	if _, err := c.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: &tableName, DeletionProtectionEnabled: aws.Bool(false)}); err != nil {
		return wrapAWSError(err, fmt.Sprintf("turning off deletion protection of table %q", entry.Name))
	}
	return nil
}

// deleteTable deletes the table and drops its ledger entry.
func (c *tableCleaner) deleteTable(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	tableName, err := tableNameFromARN(entry.ARN)
	if err != nil {
		return ledger, err
	}
	if err := c.releaseDeletionProtection(ctx, entry, tableName); err != nil {
		return ledger, err
	}
	if _, err := c.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &tableName}); err != nil {
		var notFound *types.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			return ledger, wrapAWSError(err, fmt.Sprintf("deleting table %q", entry.Name))
		}
		// Already gone (e.g. a previous attempt succeeded but the
		// ledger write or a later step failed before recording it) —
		// treat like a successful delete rather than refusing to make
		// progress on a table that's already correctly cleaned up.
	}
	if c.recordEvent != nil {
		c.recordEvent("Warning", "TableDeleted", fmt.Sprintf("Deleted table %s (%s)", entry.Name, entry.ARN))
	}
	status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	return ledger, nil
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

// tableIsEmpty does a strongly-consistent, count-only Scan (Select: COUNT,
// Limit: 1) rather than trusting DescribeTable's ItemCount — AWS documents
// ItemCount as updated only approximately every six hours, which would let
// a table populated minutes ago sail through this check as if it were
// still empty. A one-item-limited count Scan costs one real read but gives
// a trustworthy, current answer, which matters far more here than the
// extra API call costs — DeleteTable itself has no non-empty guard of its
// own to fall back on.
func tableIsEmpty(ctx context.Context, client dynamodbAPI, tableArn string) (bool, error) {
	tableName, err := tableNameFromARN(tableArn)
	if err != nil {
		return false, err
	}
	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:      &tableName,
		Select:         types.SelectCount,
		Limit:          aws.Int32(1),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, err
	}
	return out.Count == 0, nil
}

// tableNameFromARN extracts the table name from a stored ARN
// (arn:aws:dynamodb:region:account:table/name) instead of recomputing it
// from namespace/crName/key, so cleanup doesn't depend on spec context that
// may already be gone.
func tableNameFromARN(arn string) (string, error) {
	idx := strings.LastIndex(arn, "/")
	if idx == -1 || idx == len(arn)-1 {
		return "", fmt.Errorf("unexpected table ARN format: %s", arn)
	}
	return arn[idx+1:], nil
}

// relinquishIfStillTagged removes our ownership tag from a resource whose
// deletionPolicy is Retain and is no longer declared. Idempotent — safe on
// every reconcile pass.
func relinquishIfStillTagged(ctx context.Context, client dynamodbAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource, recordEvent status.EventRecorder) (relinquished, gone bool, err error) {
	tags, tErr := listAllTags(ctx, client, entry.ARN)
	if tErr != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(tErr, &notFound) {
			return false, true, nil // already gone, nothing to relinquish
		}
		return false, false, wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained table %q", entry.Name))
	}
	currentTags := tagsToMap(tags)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return false, false, nil // already relinquished, or never verified as ours
	}

	if released := cloudctlaws.ReleaseTags(ctx, namespace, crName, time.Now()); released != nil {
		if _, tagErr := client.TagResource(ctx, &dynamodb.TagResourceInput{ResourceArn: &entry.ARN, Tags: mapToTags(released)}); tagErr != nil {
			cloudctlaws.ReleaseTagFailed(recordEvent, "table", entry.Name, tagErr)
		}
	}
	if _, uErr := client.UntagResource(ctx, &dynamodb.UntagResourceInput{
		ResourceArn: &entry.ARN,
		TagKeys:     []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return false, false, wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained table %q", entry.Name))
	}
	return true, false, nil
}
