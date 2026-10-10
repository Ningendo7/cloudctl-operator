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

package rds

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// PendingDeletionGracePeriod is RDS's own constant, deliberately much
// shorter than dynamodb/sqs/s3's shared 7 days - those reuse "stuck" as a
// label on an ongoing, cheap, maybe-self-resolving check, which doesn't
// exist for RDS. There's no automatic recourse here regardless of how
// long we wait, so a short threshold matters: every extra hour is an
// idle, possibly expensive instance billing with nothing watching it.
const PendingDeletionGracePeriod = 1 * time.Hour

// deletionQuietWindow mirrors sqs/sns/dynamodb's anti-flapping buffer.
// Also gates the proactive final snapshot here, since taking one is
// cheap and non-destructive - no reason to wait any longer than this.
const deletionQuietWindow = 10 * time.Minute

// snapshotResourceType is the ledger key a stuck instance's proactive
// final snapshot is tracked under - a separate ledger entry rather than a
// new ManagedResource field, the same reuse-the-ledger approach
// podNetworkIdentityResourceType already takes for the pod security group.
const snapshotResourceType = "rds-snapshot"

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

// Cleanup finds ledger entries for rds instances no longer declared in spec
// (or every rds entry, if deleting is true) and either retains-and-
// relinquishes them, or marks them pending deletion - it never deletes an
// instance itself. A Delete-policy instance past the quiet window and then
// past PendingDeletionGracePeriod escalates permanently to
// CleanupReasonStuckPendingDeletion and gets a proactive, verified final
// snapshot, so whoever eventually deletes it manually (console/CLI,
// entirely outside this operator) has a confirmed restore point waiting.
func Cleanup(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName, crUID string,
	spec *depsv1alpha1.RDSSpec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, r := range spec.Resources {
			declared[r.Name] = true
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
				if recordEvent != nil {
					recordEvent("Normal", "InstanceDeletionCancelled", fmt.Sprintf("Canceled pending deletion of RDS instance %s (%s); resource reappeared in spec", entry.Name, entry.ARN))
				}
			}
			// Back in use - any tracked snapshot has outlived its purpose.
			var retireErr error
			updatedLedger, retireErr = retireFinalSnapshot(ctx, awsClient, entry.Name, updatedLedger, "the instance reappeared in spec", recordEvent)
			if retireErr != nil && firstErr == nil {
				firstErr = retireErr
			}
			continue
		}

		if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
			relinquished, relErr := relinquishIfStillTagged(ctx, awsClient, namespace, crName, crUID, entry)
			if relErr != nil {
				if firstErr == nil {
					firstErr = relErr
				}
				continue
			}
			if relinquished && recordEvent != nil {
				recordEvent("Normal", "InstanceOwnershipRelinquished", fmt.Sprintf("Relinquished ownership of retained RDS instance %s (%s) - no longer declared in spec", entry.Name, entry.ARN))
			}
			results = append(results, CleanupResult{Name: entry.Name, Reason: CleanupReasonRetained})
			continue
		}

		if entry.PendingDeletionSince == nil {
			updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
			if recordEvent != nil {
				recordEvent("Warning", "InstanceDeletionRequiresManualAction", fmt.Sprintf(
					"RDS instance %s (%s) is no longer declared. A final snapshot will be taken proactively after %s, "+
						"and this will be flagged as needing manual deletion after %s - this operator never deletes a database automatically.",
					entry.Name, entry.ARN, deletionQuietWindow, PendingDeletionGracePeriod))
			}
			continue
		}

		reason := pendingDeletionReason(entry.PendingDeletionSince.Time)
		results = append(results, CleanupResult{Name: entry.Name, Reason: reason})

		if time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow {
			continue
		}

		updatedLedger, err = ensureFinalSnapshot(ctx, awsClient, namespace, crName, entry, updatedLedger, recordEvent)
		if err != nil && firstErr == nil {
			firstErr = err
		}
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

// ensureFinalSnapshot creates (once) and then polls a final snapshot for a
// removed instance, recording it under snapshotResourceType. If a tracked
// snapshot belongs to an older episode (entry.Name matches but the
// derived ID doesn't - this instance was redeclared and removed again
// since), it's superseded: retire it first and let the fresh one start on
// a later pass once that's done, rather than tracking two at once.
func ensureFinalSnapshot(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName string,
	entry depsv1alpha1.ManagedResource,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	// entry.PendingDeletionSince is guaranteed non-nil here - reaching
	// this function at all requires it. Folded into the ID so a later,
	// separate episode never collides with an earlier one's snapshot.
	episodeID := strconv.FormatInt(entry.PendingDeletionSince.Unix(), 10)
	expectedSnapshotID := cloudctlaws.DerivedResourceName(namespace, crName, resourceType, 255, entry.Name, "final-snapshot", episodeID)

	existing := status.FindManagedResource(ledger, snapshotResourceType, entry.Name)
	if existing != nil && existing.ARN != expectedSnapshotID {
		return retireFinalSnapshot(ctx, awsClient, entry.Name, ledger, "superseded by a newer episode", recordEvent)
	}
	if existing != nil && existing.State == depsv1alpha1.ManagedResourceStateVerified {
		return ledger, nil
	}

	instanceID, err := instanceIDFromARN(entry.ARN)
	if err != nil {
		return ledger, err
	}

	if existing == nil {
		snapshotID := expectedSnapshotID
		_, createErr := awsClient.CreateDBSnapshot(ctx, &rds.CreateDBSnapshotInput{
			DBInstanceIdentifier: &instanceID,
			DBSnapshotIdentifier: &snapshotID,
		})
		var alreadyExists *types.DBSnapshotAlreadyExistsFault
		if createErr != nil && !errors.As(createErr, &alreadyExists) {
			return ledger, wrapAWSError(createErr, fmt.Sprintf("creating final snapshot for stuck instance %q", entry.Name))
		}
		status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
			Type:           snapshotResourceType,
			Name:           entry.Name,
			ARN:            snapshotID,
			State:          depsv1alpha1.ManagedResourceStateCreating,
			DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt:      metav1.Now(),
		})
		if recordEvent != nil {
			recordEvent("Normal", "FinalSnapshotCreating", fmt.Sprintf("Taking a final snapshot %q of stuck RDS instance %s before any manual deletion", snapshotID, entry.Name))
		}
		return ledger, nil
	}

	describeOut, err := awsClient.DescribeDBSnapshots(ctx, &rds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: &existing.ARN})
	if err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("checking final snapshot %q", existing.ARN))
	}
	if len(describeOut.DBSnapshots) == 0 {
		return ledger, nil
	}

	switch aws.ToString(describeOut.DBSnapshots[0].Status) {
	case "available":
		updated := *existing
		now := metav1.Now()
		updated.State = depsv1alpha1.ManagedResourceStateVerified
		updated.LastVerifiedAt = &now
		status.UpsertManagedResource(&ledger, updated)
		if recordEvent != nil {
			recordEvent("Normal", "FinalSnapshotReady", fmt.Sprintf("Final snapshot %q of stuck RDS instance %s is available", existing.ARN, entry.Name))
		}
	case "failed":
		if recordEvent != nil {
			recordEvent("Warning", "FinalSnapshotFailed", fmt.Sprintf("Final snapshot %q of stuck RDS instance %s failed - manual intervention needed before deleting it", existing.ARN, entry.Name))
		}
	default:
		// still in progress - leave the ledger entry as Creating, checked
		// again next reconcile.
	}
	return ledger, nil
}

// retireFinalSnapshot deletes a tracked final snapshot once it's no
// longer needed (superseded, or the instance is back in use) and drops
// its ledger entry. A no-op if nothing is tracked. One still mid-creation
// is left tracked and rechecked next pass rather than deleted early -
// DeleteDBSnapshot only works on an available (or failed) snapshot.
func retireFinalSnapshot(
	ctx context.Context,
	awsClient rdsAPI,
	resourceName string,
	ledger []depsv1alpha1.ManagedResource,
	why string,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	existing := status.FindManagedResource(ledger, snapshotResourceType, resourceName)
	if existing == nil {
		return ledger, nil
	}

	if existing.State != depsv1alpha1.ManagedResourceStateVerified {
		describeOut, err := awsClient.DescribeDBSnapshots(ctx, &rds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: &existing.ARN})
		if err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("checking snapshot %q before retiring it", existing.ARN))
		}
		if len(describeOut.DBSnapshots) == 0 {
			status.RemoveManagedResource(&ledger, snapshotResourceType, resourceName)
			return ledger, nil
		}
		switch aws.ToString(describeOut.DBSnapshots[0].Status) {
		case "available":
			// proceed to delete below
		case "failed":
			status.RemoveManagedResource(&ledger, snapshotResourceType, resourceName)
			return ledger, nil
		default:
			return ledger, nil // still creating - recheck next pass
		}
	}

	if _, err := awsClient.DeleteDBSnapshot(ctx, &rds.DeleteDBSnapshotInput{DBSnapshotIdentifier: &existing.ARN}); err != nil {
		var notFound *types.DBSnapshotNotFoundFault
		if !errors.As(err, &notFound) {
			return ledger, wrapAWSError(err, fmt.Sprintf("deleting retired snapshot %q", existing.ARN))
		}
	}
	status.RemoveManagedResource(&ledger, snapshotResourceType, resourceName)
	if recordEvent != nil {
		recordEvent("Normal", "FinalSnapshotRetired", fmt.Sprintf("Deleted final snapshot %q for %q - %s", existing.ARN, resourceName, why))
	}
	return ledger, nil
}

// instanceIDFromARN extracts the instance identifier from a stored RDS
// instance ARN (arn:aws:rds:region:account:db:instance-id), so cleanup
// doesn't depend on spec context that may already be gone.
func instanceIDFromARN(arn string) (string, error) {
	idx := strings.LastIndex(arn, ":")
	if idx == -1 || idx == len(arn)-1 {
		return "", fmt.Errorf("unexpected instance ARN format: %s", arn)
	}
	return arn[idx+1:], nil
}

// relinquishIfStillTagged removes this operator's ownership tags from an
// instance whose deletionPolicy is Retain and is no longer declared.
// Idempotent - safe on every reconcile pass.
func relinquishIfStillTagged(ctx context.Context, awsClient rdsAPI, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource) (relinquished bool, err error) {
	tagsOut, tErr := awsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: &entry.ARN})
	if tErr != nil {
		var notFound *types.DBInstanceNotFoundFault
		if errors.As(tErr, &notFound) {
			return false, nil // already gone, nothing to relinquish
		}
		return false, wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained instance %q", entry.Name))
	}
	currentTags := tagsToMap(tagsOut.TagList)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return false, nil // already relinquished, or never verified as ours
	}

	if _, uErr := awsClient.RemoveTagsFromResource(ctx, &rds.RemoveTagsFromResourceInput{
		ResourceName: &entry.ARN,
		TagKeys:      []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
	}); uErr != nil {
		return false, wrapAWSError(uErr, fmt.Sprintf("relinquishing ownership tag on retained instance %q", entry.Name))
	}
	return true, nil
}
