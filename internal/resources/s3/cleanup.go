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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
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
// forceDeleteAll overrides every entry's own Force value - meant only for
// the finalize path, see the sqs package's Cleanup for why this exists.
func Cleanup(
	ctx context.Context,
	client s3API,
	namespace, crName, crUID string,
	spec *depsv1alpha1.S3Spec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
	forceDeleteAll bool,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, results []CleanupResult, err error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, b := range spec.Resources {
			declared[b.Name] = true
		}
	}

	c := &bucketCleaner{client: client, namespace: namespace, crName: crName, crUID: crUID, forceDeleteAll: forceDeleteAll, recordEvent: recordEvent}
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

// bucketCleaner carries what every entry in one Cleanup pass shares.
type bucketCleaner struct {
	client                   s3API
	namespace, crName, crUID string
	forceDeleteAll           bool
	recordEvent              status.EventRecorder
}

// restoreDeclared lifts the write deny from a bucket that is declared again,
// and cancels its pending deletion.
func (c *bucketCleaner) restoreDeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	if entry.PendingDeletionSince == nil {
		return ledger, nil
	}
	if bucket, nameErr := bucketNameFromARN(entry.ARN); nameErr == nil {
		if err := removePendingDeletionDeny(ctx, c.client, bucket); err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("removing pending-deletion deny from bucket %q", entry.Name))
		}
	}
	cleared := entry
	cleared.PendingDeletionSince = nil
	cleared.LastEmptyCheckAt = nil
	status.UpsertManagedResource(&ledger, cleared)
	if c.recordEvent != nil {
		c.recordEvent("Normal", "BucketDeletionCancelled", fmt.Sprintf("Canceled pending deletion of bucket %s (%s); resource reappeared in spec", entry.Name, entry.ARN))
	}
	return ledger, nil
}

// retain lets go of an undeclared bucket whose policy is not Delete, and
// rechecks a relinquished one about daily. An entry is kept only while it
// pins a KMS key.
func (c *bucketCleaner) retain(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
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
		c.recordEvent("Normal", "BucketOwnershipRelinquished", fmt.Sprintf("Relinquished ownership of retained bucket %s (%s) - no longer declared in spec", entry.Name, entry.ARN))
	}
	if pinsKey {
		status.UpsertManagedResource(&ledger, status.Relinquished(entry))
	} else {
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	}
	return ledger, append(results, retained), nil
}

// deleteUndeclared deletes an undeclared Delete-policy bucket once its quiet
// window has passed and it is empty, unless force skips both.
func (c *bucketCleaner) deleteUndeclared(ctx context.Context, entry depsv1alpha1.ManagedResource, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) ([]depsv1alpha1.ManagedResource, []CleanupResult, error) {
	forced := entry.Force || c.forceDeleteAll

	// A pending bucket waiting out the quiet window or backoff reads nothing;
	// ownership is re-checked below, before anything changes it.
	if !forced && entry.PendingDeletionSince != nil &&
		(time.Since(entry.PendingDeletionSince.Time) < deletionQuietWindow || !status.NeedsEmptyCheck(entry)) {
		return ledger, append(results, CleanupResult{Name: entry.Name, Reason: pendingDeletionReason(entry.PendingDeletionSince.Time)}), nil
	}

	bucket, nameErr := bucketNameFromARN(entry.ARN)
	if nameErr != nil {
		return ledger, results, nameErr
	}
	gone, err := c.verifyOwned(ctx, entry, bucket)
	if gone {
		// Already gone - just drop it from the ledger.
		status.RemoveManagedResource(&ledger, resourceType, entry.Name)
		return ledger, results, nil
	}
	if err != nil {
		return ledger, results, err
	}

	if !forced {
		var proceed bool
		ledger, results, proceed, err = c.gateOnEmpty(ctx, entry, bucket, ledger, results)
		if err != nil || !proceed {
			return ledger, results, err
		}
	}

	ledger, err = c.deleteBucket(ctx, entry, bucket, ledger)
	return ledger, results, err
}

// verifyOwned re-reads the bucket's tags before anything acts on the
// ledger's word. gone is true when the bucket no longer exists.
func (c *bucketCleaner) verifyOwned(ctx context.Context, entry depsv1alpha1.ManagedResource, bucket string) (gone bool, err error) {
	currentTags := map[string]string{}
	tagsOut, err := c.client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	switch {
	case err == nil:
		currentTags = tagsToMap(tagsOut.TagSet)
	case isNotFoundError(err):
		return true, nil
	case !isNoSuchTagSet(err):
		return false, wrapAWSError(err, fmt.Sprintf("re-verifying ownership of bucket %q before delete", entry.Name))
	}
	// NoSuchTagSet: bucket exists but is untagged - not owned by anyone,
	// falls through to the IsOwnedBy check below, which will correctly
	// refuse to delete it.
	if !cloudctlaws.IsOwnedBy(currentTags, c.namespace, c.crName, c.crUID) {
		return false, cloudctlaws.OwnershipLostError("bucket", entry.Name, "delete it")
	}
	return false, nil
}

// gateOnEmpty holds a bucket's deletion until it has been pending for the
// quiet window and is empty. proceed is false while it must still wait.
func (c *bucketCleaner) gateOnEmpty(ctx context.Context, entry depsv1alpha1.ManagedResource, bucket string, ledger []depsv1alpha1.ManagedResource, results []CleanupResult) (updated []depsv1alpha1.ManagedResource, updatedResults []CleanupResult, proceed bool, err error) {
	if entry.PendingDeletionSince == nil {
		// Never delete on the same pass a bucket is first noticed as
		// eligible, even if it looks empty right now - a moment-old
		// write might not be reflected yet. Deny new writes and hold
		// for the quiet window first, the same active-enforcement
		// mechanism SQS/SNS already use for their own equivalent
		// window.
		if err := addPendingDeletionDeny(ctx, c.client, bucket); err != nil {
			return ledger, results, false, wrapAWSError(err, fmt.Sprintf("blocking new writes to bucket %q pending deletion", entry.Name))
		}
		ledger, results = markPendingDeletion(ledger, results, entry)
		if c.recordEvent != nil {
			c.recordEvent("Warning", "BucketDeletionPending", fmt.Sprintf("Bucket %s (%s) is no longer declared and will be deleted in %s unless it reappears in spec", entry.Name, entry.ARN, deletionQuietWindow))
		}
		return ledger, results, false, nil
	}
	empty, err := bucketIsEmpty(ctx, c.client, bucket)
	if err != nil {
		return ledger, results, false, wrapAWSError(err, fmt.Sprintf("checking bucket %q is empty", entry.Name))
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

// deleteBucket empties the bucket and deletes it, dropping its ledger entry.
// The full sequence runs even on the "confirmed empty" path: a bucket can
// show zero current objects yet still carry delete markers or old versions
// (if versioning was ever turned on), and this is idempotent either way.
func (c *bucketCleaner) deleteBucket(ctx context.Context, entry depsv1alpha1.ManagedResource, bucket string, ledger []depsv1alpha1.ManagedResource) ([]depsv1alpha1.ManagedResource, error) {
	if err := deleteAllObjectVersions(ctx, c.client, bucket); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("deleting objects in bucket %q", entry.Name))
	}
	if err := abortMultipartUploads(ctx, c.client, bucket); err != nil {
		return ledger, abortUploadsError(err, entry.Name)
	}
	_, err := c.client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: &bucket})
	switch {
	case err == nil:
		if c.recordEvent != nil {
			c.recordEvent("Warning", "BucketDeleted", fmt.Sprintf("Deleted bucket %s (%s)", entry.Name, entry.ARN))
		}
	case !isNotFoundError(err):
		return ledger, wrapAWSError(err, fmt.Sprintf("deleting bucket %q", entry.Name))
	}
	status.RemoveManagedResource(&ledger, resourceType, entry.Name)
	return ledger, nil
}

// abortUploadsError wraps a failure to abort multipart uploads. Uploads that
// remain after the abort are still in flight, so waiting and trying again
// can succeed.
func abortUploadsError(err error, name string) error {
	wrapped := wrapAWSError(err, fmt.Sprintf("aborting multipart uploads in bucket %q", name))
	var reconcileErr *cloudctlaws.ReconcileError
	if errors.As(wrapped, &reconcileErr) && errors.Is(err, errUploadsRemain) {
		reconcileErr.Retryable = true
	}
	return wrapped
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
			end := min(i+batchSize, len(toDelete))
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

// errUploadsRemain marks a pass that ran out of time with uploads still to
// abort - progress is kept, so the next pass simply continues.
var errUploadsRemain = errors.New("deadline reached with multipart uploads still to abort")

// abortConcurrency bounds parallel aborts. The S3 rate limiter, not this, is
// the real ceiling; this just keeps a single pass from being latency-bound.
const abortConcurrency = 8

// abortMultipartUploads cleans up any in-progress multipart uploads left in
// the bucket - if skipped, DeleteBucket fails with an opaque BucketNotEmpty
// much later with no indication why, since these don't show up as objects
// or versions at all. Pages through every upload; if ctx expires first it
// returns errUploadsRemain.
func abortMultipartUploads(ctx context.Context, client s3API, bucket string) error {
	var keyMarker, uploadIDMarker *string
	var failed int
	var firstErr error
	for {
		page, err := client.ListMultipartUploads(ctx, &s3sdk.ListMultipartUploadsInput{
			Bucket: &bucket, KeyMarker: keyMarker, UploadIdMarker: uploadIDMarker,
		})
		if err != nil {
			if isNotFoundError(err) {
				return nil
			}
			if ctx.Err() != nil {
				return errUploadsRemain
			}
			return fmt.Errorf("listing multipart uploads: %w", err)
		}

		var mu sync.Mutex
		var wg sync.WaitGroup
		slots := make(chan struct{}, abortConcurrency)
		for _, u := range page.Uploads {
			slots <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-slots; wg.Done() }()
				if ctx.Err() != nil {
					return
				}
				_, err := client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{
					Bucket: &bucket, Key: u.Key, UploadId: u.UploadId,
				})
				// An upload already aborted/completed by the time we get to
				// it (e.g. a concurrent retry) is fine, not a failure.
				if err != nil && !isNotFoundError(err) && ctx.Err() == nil {
					mu.Lock()
					failed++
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		if ctx.Err() != nil {
			return errUploadsRemain
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		keyMarker, uploadIDMarker = page.NextKeyMarker, page.NextUploadIdMarker
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
func relinquishIfStillTagged(ctx context.Context, client s3API, namespace, crName, crUID string, entry depsv1alpha1.ManagedResource, recordEvent status.EventRecorder) (relinquished, gone bool, err error) {
	bucket, nameErr := bucketNameFromARN(entry.ARN)
	if nameErr != nil {
		return false, false, nameErr
	}

	tagsOut, tErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if tErr != nil {
		if isNotFoundError(tErr) {
			return false, true, nil // already gone
		}
		if isNoSuchTagSet(tErr) {
			return false, false, nil // exists, nothing to relinquish
		}
		return false, false, wrapAWSError(tErr, fmt.Sprintf("checking ownership tags on retained bucket %q", entry.Name))
	}
	currentTags := tagsToMap(tagsOut.TagSet)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		return false, false, nil // already relinquished, or never verified as ours
	}

	remaining := map[string]string{}
	for k, v := range currentTags {
		if k != cloudctlaws.OwnerTagKey && k != cloudctlaws.OwnerUIDTagKey {
			remaining[k] = v
		}
	}
	// PutBucketTagging replaces the whole tag set, so the released tags go in
	// the same write. A bucket allows only ten tags: if they do not fit,
	// letting go still has to succeed, so the write is repeated without them.
	released := cloudctlaws.ReleaseTags(ctx, namespace, crName, time.Now())
	if released != nil {
		err := putBucketTags(ctx, client, bucket, cloudctlaws.MergeTags(remaining, released))
		if err == nil {
			return true, false, nil
		}
		cloudctlaws.ReleaseTagFailed(recordEvent, "bucket", entry.Name, err)
	}
	if err := putBucketTags(ctx, client, bucket, remaining); err != nil {
		return false, false, wrapAWSError(err, fmt.Sprintf("relinquishing ownership tag on retained bucket %q", entry.Name))
	}
	return true, false, nil
}

// putBucketTags writes the whole tag set; an empty one clears it, and
// GetBucketTagging then reports NoSuchTagSet.
func putBucketTags(ctx context.Context, client s3API, bucket string, tags map[string]string) error {
	set := []types.Tag{}
	if len(tags) > 0 {
		set = mapToTags(tags)
	}
	_, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{Bucket: &bucket, Tagging: &types.Tagging{TagSet: set}})
	return err
}
