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
// Known gap, deliberately deferred: unlike sqs/sns, this doesn't attach a
// resource policy denying new writes while a table sits in PendingDeletion.
// DynamoDB resource-based policies are a newer, less battle-tested feature
// than SQS/SNS's, and the quiet window plus the 7-day grace period already
// cover the realistic risk window without it. Revisit if that turns out
// insufficient in practice.
func Cleanup(
	ctx context.Context,
	client dynamodbAPI,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.DynamoDBSpec,
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

		tags, tErr := listAllTags(ctx, client, entry.ARN)
		if tErr != nil {
			var notFound *types.ResourceNotFoundException
			if errors.As(tErr, &notFound) {
				status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
				continue
			}
			if firstErr == nil {
				firstErr = wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of table %q before delete", entry.Name))
			}
			continue
		}
		if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, crUID) {
			if firstErr == nil {
				firstErr = fmt.Errorf("table %q no longer verified as owned by this CR — refusing to delete it", entry.Name)
			}
			continue
		}

		if !entry.Force {
			if entry.PendingDeletionSince == nil {
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}
			if time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow {
				results = append(results, CleanupResult{Name: entry.Name, Reason: pendingDeletionReason(entry.PendingDeletionSince.Time)})
				continue
			}

			empty, emptyErr := tableIsEmpty(ctx, client, entry.ARN)
			if emptyErr != nil {
				if firstErr == nil {
					firstErr = wrapAWSError(emptyErr, fmt.Sprintf("checking table %q is empty", entry.Name))
				}
				continue
			}
			if !empty {
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}
		}

		tableName, nameErr := tableNameFromARN(entry.ARN)
		if nameErr != nil {
			if firstErr == nil {
				firstErr = nameErr
			}
			continue
		}
		if _, dErr := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &tableName}); dErr != nil {
			var notFound *types.ResourceNotFoundException
			if !errors.As(dErr, &notFound) {
				if firstErr == nil {
					firstErr = wrapAWSError(dErr, fmt.Sprintf("deleting table %q", entry.Name))
				}
				continue
			}
			// Already gone (e.g. a previous attempt succeeded but the
			// ledger write or a later step failed before recording it) —
			// treat like a successful delete rather than refusing to make
			// progress on a table that's already correctly cleaned up.
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
func relinquishIfStillTagged(ctx context.Context, client dynamodbAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource) error {
	tags, tErr := listAllTags(ctx, client, entry.ARN)
	if tErr != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(tErr, &notFound) {
			return nil // already gone, nothing to relinquish
		}
		return wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained table %q", entry.Name))
	}
	currentTags := tagsToMap(tags)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return nil // already relinquished, or never verified as ours
	}

	if _, uErr := client.UntagResource(ctx, &dynamodb.UntagResourceInput{
		ResourceArn: &entry.ARN,
		TagKeys:     []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained table %q", entry.Name))
	}
	return nil
}
