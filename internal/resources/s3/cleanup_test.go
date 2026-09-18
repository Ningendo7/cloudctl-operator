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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func setupBucket(t *testing.T, client *fakeS3, namespace, crName, name string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	t.Helper()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: name, DeletionPolicy: deletionPolicy, Force: force},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, namespace, crName, "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
	return ledger
}

func findResult(results []CleanupResult, name string) *CleanupResult {
	for i := range results {
		if results[i].Name == name {
			return &results[i]
		}
	}
	return nil
}

func advancePastQuietWindow(t *testing.T, ledger []depsv1alpha1.ManagedResource, name string) {
	t.Helper()
	entry := status.FindManagedResource(ledger, "s3", name)
	if entry == nil {
		t.Fatalf("test setup broken: no ledger entry named %q", name)
	}
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)
}

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyRetain, false)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected receipts to be reported Retained, got %+v", results)
	}
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Error("expected Retain policy to leave the AWS bucket in place")
	}
	if status.FindManagedResource(updated, "s3", "receipts") == nil {
		t.Error("expected retained entry to stay in the ledger")
	}
}

func TestCleanup_TreatsUnsetDeletionPolicyAsRetain(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
		cloudctlaws.OwnerUIDTagKey: "uid-1",
	}}
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "receipts", ARN: "arn:aws:s3:::" + bucket},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected an unset DeletionPolicy to be treated as Retained, got %+v", results)
	}
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Error("expected the bucket to survive - an unset DeletionPolicy must never be treated as Delete")
	}
	if status.FindManagedResource(updated, "s3", "receipts") == nil {
		t.Error("expected the retained entry to stay in the ledger")
	}
}

func TestCleanup_RelinquishesOwnershipTagForRetainedResource(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyRetain, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)

	if !cloudctlaws.IsOwnedBy(client.buckets[bucket].tags, "default", "checkout-service", "uid-1") {
		t.Fatal("test setup broken: expected the bucket to start out owned by us")
	}

	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if cloudctlaws.IsOwnedBy(client.buckets[bucket].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the ownership tag to be relinquished once a Retain resource is no longer declared")
	}
}

func TestCleanup_HoldsNewlyEligibleBucketForQuietWindowBeforeDeleting(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected receipts to be held PendingDeletion on first encounter, got %+v", results)
	}
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Fatal("expected the bucket to survive the first pass even though it's empty right now")
	}

	advancePastQuietWindow(t, updated, "receipts")

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, updated, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r != nil {
		t.Errorf("expected no pending result once actually deleted, got %+v", results)
	}
	if _, stillExists := client.buckets[bucket]; stillExists {
		t.Error("expected the bucket to be deleted once the quiet window elapsed and it's confirmed empty")
	}
	if status.FindManagedResource(updated, "s3", "receipts") != nil {
		t.Error("expected the ledger entry to be removed after deletion")
	}
}

func TestCleanup_BlocksDeletingNonEmptyBucketWithoutForce(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket].versions = []fakeObjectVersion{{key: "file.txt", versionID: "v1"}}

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "receipts")

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected receipts to be PendingDeletion, got %+v", results)
	}
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Error("expected the non-empty bucket to NOT be deleted")
	}
	if entry := status.FindManagedResource(updated, "s3", "receipts"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected PendingDeletionSince to remain recorded")
	}
}

func TestCleanup_ConsidersOldVersionsAsNonEmptyEvenWithNoCurrentObjects(t *testing.T) {
	// A bucket with backup enabled can have zero current objects but still
	// carry old versions/delete markers retained for point-in-time
	// recovery - deleting it would destroy that retained data, so this
	// must count as non-empty even though nothing "current" remains.
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket].versions = []fakeObjectVersion{{key: "file.txt", versionID: "v1", isDeleteMarker: true}}

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "receipts")

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected a bucket with only a delete marker to still be treated as non-empty, got %+v", results)
	}
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Error("expected the bucket to survive")
	}
}

func TestCleanup_DeletesAllObjectVersionsAndAbortsMultipartUploadsBeforeDeletingBucket(t *testing.T) {
	// The fake's DeleteBucket rejects a bucket that still has versions or
	// uploads (mirroring real S3's BucketNotEmpty) - this test's real
	// value is proving Cleanup actually empties the bucket first, not just
	// that the end state looks right.
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, true) // force: skip the quiet window/guard for this ordering-focused test
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket].versions = []fakeObjectVersion{
		{key: "a.txt", versionID: "v1"},
		{key: "b.txt", versionID: "v1", isDeleteMarker: true},
	}
	client.buckets[bucket].uploads = []fakeUpload{{key: "c.txt", uploadID: "upload-1"}}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, stillExists := client.buckets[bucket]; stillExists {
		t.Error("expected the bucket to actually be deleted once objects and uploads are cleared")
	}
}

func TestCleanup_ForceDeletesNonEmptyBucket(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, true)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket].versions = []fakeObjectVersion{{key: "file.txt", versionID: "v1"}}

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected force to override the non-empty guard, got %v", results)
	}
	if _, stillExists := client.buckets[bucket]; stillExists {
		t.Error("expected force:true to delete the non-empty bucket immediately, skipping the quiet window entirely")
	}
}

func TestCleanup_RefusesDeletingUnverifiedOwnership(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket].tags = map[string]string{"team": "someone-else"}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err == nil {
		t.Fatal("expected Cleanup to refuse deleting a bucket whose ownership tags no longer verify")
	}
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Error("expected the bucket to NOT be deleted when ownership can't be re-verified")
	}
}

func TestCleanup_EscalatesToStuckAfterGracePeriod(t *testing.T) {
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket].versions = []fakeObjectVersion{{key: "file.txt", versionID: "v1"}}

	longAgo := metav1.NewTime(time.Now().Add(-2 * PendingDeletionGracePeriod))
	entry := status.FindManagedResource(ledger, "s3", "receipts")
	entry.PendingDeletionSince = &longAgo
	status.UpsertManagedResource(&ledger, *entry)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "receipts"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Errorf("expected receipts to escalate to StuckPendingDeletion, got %+v", results)
	}
	if _, stillExists := client.buckets[bucket]; !stillExists {
		t.Error("expected the stuck bucket to still NOT be force-deleted automatically")
	}
}

func TestCleanup_TreatsAlreadyDeletedBucketAsSuccess(t *testing.T) {
	// Regression test for the isNotFoundError bug found while building
	// this package: GetBucketTagging signals a missing bucket via
	// NoSuchBucket, a genuinely different typed exception from HeadBucket's
	// NotFound - without checking for it specifically, retrying cleanup on
	// a bucket a previous attempt had already deleted would wrongly report
	// a failure for a bucket that was, in fact, correctly cleaned up
	// already (the exact bug class a sibling project's S3 controller hit
	// before too).
	client := newFakeS3()
	ledger := setupBucket(t, client, "default", "checkout-service", "receipts", depsv1alpha1.DeletionPolicyDelete, false)
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)

	advancePastQuietWindow(t, ledger, "receipts")
	delete(client.buckets, bucket) // simulate it already having been deleted out from under us

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected an already-gone bucket to be treated as already cleaned up, not a failure", err)
	}
	if status.FindManagedResource(updated, "s3", "receipts") != nil {
		t.Error("expected the ledger entry to be removed for an already-gone bucket")
	}
}

func TestAbortMultipartUploads_TreatsAlreadyGoneUploadAsSuccess(t *testing.T) {
	// Regression test directly validating the NoSuchUpload fix: an upload
	// already aborted/completed by the time we get to it (e.g. a
	// concurrent retry) must not be treated as a failure.
	client := newFakeS3()
	bucket := "receipts-test"
	client.buckets[bucket] = &fakeBucket{uploads: []fakeUpload{{key: "file.txt", uploadID: "upload-1"}}}
	client.abortMultipartUploadErr = &types.NoSuchUpload{}

	if err := abortMultipartUploads(context.Background(), client, bucket); err != nil {
		t.Errorf("expected an already-gone upload to be treated as success, got error: %v", err)
	}
}

func TestAbortMultipartUploads_PropagatesRealFailures(t *testing.T) {
	client := newFakeS3()
	bucket := "receipts-test"
	client.buckets[bucket] = &fakeBucket{uploads: []fakeUpload{{key: "file.txt", uploadID: "upload-1"}}}
	client.abortMultipartUploadErr = &fakeAWSError{code: "AccessDenied"}

	if err := abortMultipartUploads(context.Background(), client, bucket); err == nil {
		t.Error("expected a genuine abort failure to be reported, not silently swallowed")
	}
}

func TestDeleteAllObjectVersions_ChunksBatchesOver1000Objects(t *testing.T) {
	client := newFakeS3()
	bucket := "receipts-test"
	versions := make([]fakeObjectVersion, 0, 1500)
	for i := 0; i < 1500; i++ {
		versions = append(versions, fakeObjectVersion{key: fmt.Sprintf("file-%d.txt", i), versionID: "v1"})
	}
	client.buckets[bucket] = &fakeBucket{versions: versions}

	if err := deleteAllObjectVersions(context.Background(), client, bucket); err != nil {
		t.Fatalf("deleteAllObjectVersions() error = %v", err)
	}
	if len(client.buckets[bucket].versions) != 0 {
		t.Errorf("expected all 1500 versions across two batches to be deleted, %d remain", len(client.buckets[bucket].versions))
	}
}

func TestDeleteAllObjectVersions_PropagatesPerObjectDeleteErrors(t *testing.T) {
	// DeleteObjects can return a 200 OK overall while individual objects
	// within the batch failed - that has to be surfaced as a real failure,
	// not treated as a blanket success just because the call itself didn't
	// error.
	client := newFakeS3()
	bucket := "receipts-test"
	client.buckets[bucket] = &fakeBucket{versions: []fakeObjectVersion{
		{key: "good.txt", versionID: "v1"},
		{key: "bad.txt", versionID: "v1"},
	}}
	client.failToDeleteKey = "bad.txt"

	err := deleteAllObjectVersions(context.Background(), client, bucket)
	if err == nil {
		t.Fatal("expected a per-object delete failure to be surfaced, not silently swallowed")
	}

	remaining := client.buckets[bucket].versions
	if len(remaining) != 1 || remaining[0].key != "bad.txt" {
		t.Errorf("expected only the successfully-deleted object to be removed, got %+v", remaining)
	}
}

func TestCleanup_ContinuesToOtherResourcesAfterOneFails(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Name: "logs", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	receiptsBucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[receiptsBucket].tags = map[string]string{"team": "someone-else"}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.S3Spec{}, ledger, false)
	if err == nil {
		t.Fatal("expected an error reported for the corrupted-ownership receipts bucket")
	}

	if entry := status.FindManagedResource(updated, "s3", "logs"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected logs to still be processed into its own pending-deletion window despite receipts failing")
	}
	if status.FindManagedResource(updated, "s3", "receipts") == nil {
		t.Error("expected the failed receipts entry to remain in the ledger for retry")
	}
}
