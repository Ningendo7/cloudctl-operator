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

package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const PendingDeletionGracePeriod = 7 * 24 * time.Hour

// deletionQuietWindow mirrors sqs/sns/dynamodb's: bucketIsEmpty is a
// real-time read, but "empty right now" still can't rule out an object
// written a moment after this reconcile reads it. The first time a bucket
// comes up for deletion, it's held rather than deleted immediately,
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

// Cleanup finds ledger entries for s3 resources no longer declared in spec
// (or every s3 entry, if deleting is true) and either deletes them,
// retains-and-relinquishes them, or marks them pending deletion.
func Cleanup(
	ctx context.Context,
	client s3API,
	namespace, crName, crUID string,
	spec *depsv1alpha1.S3Spec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, b := range spec.Resources {
			declared[b.Name] = true
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

		bucket, nameErr := bucketNameFromARN(entry.ARN)
		if nameErr != nil {
			if firstErr == nil {
				firstErr = nameErr
			}
			continue
		}

		tagsOut, tErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
		currentTags := map[string]string{}
		if tErr != nil {
			if isNotFoundError(tErr) {
				// Already gone - just drop it from the ledger.
				status.RemoveManagedResource(&updatedLedger, resourceType, entry.Name)
				continue
			}
			if !isNoSuchTagSet(tErr) {
				if firstErr == nil {
					firstErr = wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of bucket %q before delete", entry.Name))
				}
				continue
			}
			// NoSuchTagSet: bucket exists but is untagged - not owned by
			// anyone, falls through to the IsOwnedBy check below, which
			// will correctly refuse to delete it.
		} else {
			currentTags = tagsToMap(tagsOut.TagSet)
		}
		if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
			if firstErr == nil {
				firstErr = fmt.Errorf("bucket %q no longer verified as owned by this CR — refusing to delete it", entry.Name)
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

			empty, emptyErr := bucketIsEmpty(ctx, client, bucket)
			if emptyErr != nil {
				if firstErr == nil {
					firstErr = wrapAWSError(emptyErr, fmt.Sprintf("checking bucket %q is empty", entry.Name))
				}
				continue
			}
			if !empty {
				updatedLedger, results = markPendingDeletion(updatedLedger, results, entry)
				continue
			}
		}

		// Run the full cleanup sequence even on the "confirmed empty" path:
		// a bucket can show zero current objects yet still carry delete
		// markers or old versions (if versioning was ever turned on), and
		// this is idempotent either way.
		if err := deleteAllObjectVersions(ctx, client, bucket); err != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(err, fmt.Sprintf("deleting objects in bucket %q", entry.Name))
			}
			continue
		}
		if err := abortMultipartUploads(ctx, client, bucket); err != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(err, fmt.Sprintf("aborting multipart uploads in bucket %q", entry.Name))
			}
			continue
		}
		if _, dErr := client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: &bucket}); dErr != nil {
			if !isNotFoundError(dErr) {
				if firstErr == nil {
					firstErr = wrapAWSError(dErr, fmt.Sprintf("deleting bucket %q", entry.Name))
				}
				continue
			}
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

// bucketIsEmpty checks for any object version or delete marker at all, not
// just current objects - a bucket with backup enabled can have zero
// current objects but many old versions retained for point-in-time
// recovery, and deleting the bucket would permanently destroy that
// retained data too.
func bucketIsEmpty(ctx context.Context, client s3API, bucket string) (bool, error) {
	out, err := client.ListObjectVersions(ctx, &s3sdk.ListObjectVersionsInput{
		Bucket:  &bucket,
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	return len(out.Versions) == 0 && len(out.DeleteMarkers) == 0, nil
}

// deleteAllObjectVersions paginates every object version and delete marker
// in the bucket and removes them in batches of 1000 (DeleteObjects' own
// limit). Required before DeleteBucket will succeed on anything that ever
// held data - unlike SQS/SNS/DynamoDB, S3 has no "delete everything at
// once" option.
func deleteAllObjectVersions(ctx context.Context, client s3API, bucket string) error {
	var keyMarker, versionIDMarker *string
	for {
		out, err := client.ListObjectVersions(ctx, &s3sdk.ListObjectVersionsInput{
			Bucket:          &bucket,
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIDMarker,
		})
		if err != nil {
			if isNotFoundError(err) {
				return nil
			}
			return fmt.Errorf("listing object versions: %w", err)
		}

		var toDelete []types.ObjectIdentifier
		for _, v := range out.Versions {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}

		const batchSize = 1000
		for i := 0; i < len(toDelete); i += batchSize {
			end := i + batchSize
			if end > len(toDelete) {
				end = len(toDelete)
			}
			delOut, err := client.DeleteObjects(ctx, &s3sdk.DeleteObjectsInput{
				Bucket: &bucket,
				Delete: &types.Delete{Objects: toDelete[i:end], Quiet: aws.Bool(true)},
			})
			if err != nil {
				return fmt.Errorf("batch-deleting objects: %w", err)
			}
			if len(delOut.Errors) > 0 {
				return fmt.Errorf("failed to delete %d object(s), first error: %s", len(delOut.Errors), aws.ToString(delOut.Errors[0].Message))
			}
		}

		if out.IsTruncated == nil || !*out.IsTruncated {
			return nil
		}
		keyMarker = out.NextKeyMarker
		versionIDMarker = out.NextVersionIdMarker
	}
}

// abortMultipartUploads cleans up any in-progress multipart uploads left in
// the bucket - if skipped, DeleteBucket fails with an opaque BucketNotEmpty
// much later with no indication why, since these don't show up as objects
// or versions at all.
func abortMultipartUploads(ctx context.Context, client s3API, bucket string) error {
	uploads, err := client.ListMultipartUploads(ctx, &s3sdk.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil {
		if isNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("listing multipart uploads: %w", err)
	}

	var failed int
	var firstErr error
	for _, u := range uploads.Uploads {
		_, err := client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{
			Bucket:   &bucket,
			Key:      u.Key,
			UploadId: u.UploadId,
		})
		// An upload already aborted/completed by the time we get to it
		// (e.g. a concurrent retry) is fine, not a failure.
		if err != nil && !isNotFoundError(err) {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("failed to abort %d multipart upload(s), first error: %w", failed, firstErr)
	}
	return nil
}

// bucketNameFromARN extracts the bucket name from a stored
// arn:aws:s3:::bucket-name ARN instead of recomputing it from
// namespace/crName/key, so cleanup doesn't depend on spec context (or the
// account ID) that may already be gone.
func bucketNameFromARN(arn string) (string, error) {
	const prefix = "arn:aws:s3:::"
	if !strings.HasPrefix(arn, prefix) || len(arn) == len(prefix) {
		return "", fmt.Errorf("unexpected bucket ARN format: %s", arn)
	}
	return strings.TrimPrefix(arn, prefix), nil
}

// relinquishIfStillTagged removes our ownership tag from a resource whose
// deletionPolicy is Retain and is no longer declared. Idempotent - safe on
// every reconcile pass.
func relinquishIfStillTagged(ctx context.Context, client s3API, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource) error {
	bucket, nameErr := bucketNameFromARN(entry.ARN)
	if nameErr != nil {
		return nameErr
	}

	tagsOut, tErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if tErr != nil {
		if isNotFoundError(tErr) || isNoSuchTagSet(tErr) {
			return nil // already gone, or nothing to relinquish
		}
		return wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained bucket %q", entry.Name))
	}
	currentTags := tagsToMap(tagsOut.TagSet)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return nil // already relinquished, or never verified as ours
	}

	remaining := map[string]string{}
	for k, v := range currentTags {
		if k != cloudctlaws.OwnerTagKey && k != cloudctlaws.OwnerUIDTagKey {
			remaining[k] = v
		}
	}
	// PutBucketTagging replaces the whole tag set - an empty TagSet is
	// rejected by the API, so clear tagging entirely if nothing else
	// remains rather than sending an empty set.
	if len(remaining) == 0 {
		_, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
			Bucket:  &bucket,
			Tagging: &types.Tagging{TagSet: []types.Tag{}},
		})
		if err != nil {
			return wrapAWSError(err, fmt.Sprintf("relinquishing ownership tag on retained bucket %q", entry.Name))
		}
		return nil
	}

	if _, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  &bucket,
		Tagging: &types.Tagging{TagSet: mapToTags(remaining)},
	}); err != nil {
		return wrapAWSError(err, fmt.Sprintf("relinquishing ownership tag on retained bucket %q", entry.Name))
	}
	return nil
}
