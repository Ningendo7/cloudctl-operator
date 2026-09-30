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
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kmstest"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func newSchemeForKMSKeyRefTest(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func TestEnsure_CreatesBucketWithAccountHashSuffix(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	expected := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, ok := client.buckets[expected]; !ok {
		t.Fatalf("expected bucket %q to be created", expected)
	}
	prefix := "default-checkout-service-receipts-"
	if !strings.HasPrefix(expected, prefix) {
		t.Errorf("expected the base name to be preserved before the hash suffixes, got %q", expected)
	}
	const wantIdentityHashLen = 12
	suffixes := strings.Split(strings.TrimPrefix(expected, prefix), "-")
	if len(suffixes) != 2 || len(suffixes[0]) != wantIdentityHashLen || len(suffixes[1]) != accountHashLen {
		t.Errorf("expected <identity-hash>-<account-hash> after the base name, got %q", expected)
	}

	entry := status.FindManagedResource(ledger, "s3", "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the bucket to reach Verified after a successful tag write, got %+v", entry)
	}
}

func TestEnsure_ProvisionsDedicatedKeyWhenEncryptionEnabled(t *testing.T) {
	client := newFakeS3()
	kmsClient := kmstest.NewFakeKMSClient()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}

	ledger, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", kms.DedicatedKeyLedgerName("s3", "receipts"))
	if keyEntry == nil {
		t.Fatal("expected a dedicated KMS key ledger entry named \"receipts-key\"")
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	b, ok := client.buckets[bucket]
	if !ok {
		t.Fatal("expected the bucket to have been created")
	}
	if b.kmsKeyARN != keyEntry.ARN {
		t.Errorf("bucket's KMS key ARN = %q, want %q", b.kmsKeyARN, keyEntry.ARN)
	}
}

func TestEnsure_CorrectsKMSKeyDriftOnExistingBucket(t *testing.T) {
	client := newFakeS3()
	kmsClient := kmstest.NewFakeKMSClient()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
	}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}

	ledger, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", kms.DedicatedKeyLedgerName("s3", "receipts"))
	if client.buckets[bucket].kmsKeyARN != keyEntry.ARN {
		t.Errorf("expected drift correction to set the bucket's KMS key to %q, got %q", keyEntry.ARN, client.buckets[bucket].kmsKeyARN)
	}
}

func TestEnsure_KMSKeyRefRetriesWhenNotYetAuthorized(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{
			KMSKeyRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"},
		}},
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).Build()

	_, err := Ensure(context.Background(), client, nil, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error - the producer CR doesn't exist yet")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError (self-resolving forward reference), got %v", err)
	}
}

func TestEnsure_KMSKeyRefResolvesWhenAuthorized(t *testing.T) {
	client := newFakeS3()
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
				{Name: "shared-key", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "default", Name: "checkout-service"},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "kms", Name: "shared-key", ARN: "arn:aws:kms:us-east-1:123456789012:key/shared-id"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{
			KMSKeyRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"},
		}},
	}}

	_, err := Ensure(context.Background(), client, nil, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	b, ok := client.buckets[bucket]
	if !ok {
		t.Fatal("expected the bucket to have been created")
	}
	if b.kmsKeyARN != "arn:aws:kms:us-east-1:123456789012:key/shared-id" {
		t.Errorf("bucket's KMS key ARN = %q, want the shared key's ARN", b.kmsKeyARN)
	}
}

func TestEnsure_CreatesBucketOnGenericHTTP404(t *testing.T) {
	// HeadBucket's own docs confirm it doesn't always return a typed
	// NotFound - a generic HTTP 404 with no message body is a documented,
	// real possibility, not a hypothetical edge case.
	client := newFakeS3()
	client.headBucketErr = fakeHTTPStatusError(404)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v — expected a generic HTTP 404 to be treated the same as a typed NotFound", err)
	}
}

func TestEnsure_ReturnsErrorOn403WithoutAttemptingCreate(t *testing.T) {
	// A 403 means the bucket exists but we can't see it (e.g. owned by a
	// different account) - must be a hard error, never misread as "doesn't
	// exist, let's create it here."
	client := newFakeS3()
	client.headBucketErr = fakeHTTPStatusError(403)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected 403 to be reported as an error")
	}
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, exists := client.buckets[bucket]; exists {
		t.Error("expected no CreateBucket attempt on a 403")
	}
	// A bare HTTP 403 with no smithy error code (HeadBucket's actual shape)
	// must still be classified correctly - this is the one call site the
	// awshttp.ResponseError fallback in internal/aws/errors.go exists for.
	if !cloudctlaws.IsPermissionDenied(err) {
		t.Error("expected a bare HTTP 403 to be classified as permission-denied")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected a bare HTTP 403 to be classified as not retryable")
	}
}

func TestEnsure_ClassifiesPermissionErrorsAsNotRetryable(t *testing.T) {
	client := newFakeS3()
	client.createBucketErr = &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected a permission-denied error to be classified as not retryable")
	}
}

func TestEnsure_ClassifiesTransientErrorsAsRetryable(t *testing.T) {
	client := newFakeS3()
	client.createBucketErr = &fakeAWSError{code: "InternalError", fault: smithy.FaultServer}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !cloudctlaws.IsRetryable(err) {
		t.Error("expected a server-fault error to be classified as retryable")
	}
}

func TestEnsure_ReturnsErrorOn301WithoutAttemptingCreate(t *testing.T) {
	// A 301 means the bucket exists in a different region - also a hard
	// error, not "doesn't exist."
	client := newFakeS3()
	client.headBucketErr = fakeHTTPStatusError(301)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected 301 to be reported as an error")
	}
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, exists := client.buckets[bucket]; exists {
		t.Error("expected no CreateBucket attempt on a 301")
	}
}

// CreateBucket now tags atomically (AWS added CreateBucketConfiguration.Tags),
// so a freshly created bucket must never trigger the separate
// GetBucketTagging/PutBucketTagging round trip the adopt path still needs.
func TestEnsure_CreatesBucketTaggedAtomically(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if !cloudctlaws.IsOwnedBy(client.buckets[bucket].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the bucket to be tagged as owned by this CR immediately after creation")
	}
	if client.putBucketTaggingCalls != 0 {
		t.Errorf("expected no PutBucketTagging call for a bucket tagged atomically at creation, got %d", client.putBucketTaggingCalls)
	}
	entry := status.FindManagedResource(ledger, "s3", "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected Verified directly, with no TagPending intermediate state, got %+v", entry)
	}
}

// The whole reason S3 needed a checkpoint (fix #1) was the gap between a
// non-atomic CreateBucket and PutBucketTagging. With tagging atomic, that
// gap no longer exists for the create path, so no checkpoint call should
// ever fire for a plain creation.
func TestEnsure_CreatingBucket_NeverCallsCheckpoint(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	checkpointCalled := false
	checkpoint := func(context.Context, []depsv1alpha1.ManagedResource) error {
		checkpointCalled = true
		return nil
	}

	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, checkpoint, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if checkpointCalled {
		t.Error("expected no checkpoint call for a plain bucket creation - atomic tagging means there's no gap left to checkpoint")
	}
}

// A crash immediately after CreateBucket succeeds - before this process
// records anything at all - must still self-heal on the next reconcile
// without requiring adopt:true, since the bucket is already tagged as ours
// the instant it exists. Simulates the crash by discarding the ledger
// entirely between calls, the same way the KMS/S3 fix #1 tests did for the
// old two-step gap - the difference here is there's no checkpoint involved,
// because there's no longer a window that needs one.
func TestEnsure_RecoversAfterCrashRightAfterCreate_WithoutAdopt(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	// Simulate total ledger loss: the second call knows nothing about the
	// first, same as a crash right after CreateBucket returned.
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v (should not require adopt:true for our own atomically-tagged bucket)", err)
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if len(client.buckets) != 1 {
		t.Errorf("expected exactly one bucket in AWS, not a duplicate, got %d", len(client.buckets))
	}
	entry := status.FindManagedResource(ledger, resourceType, "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the bucket to reach Verified without adopt:true, got %+v", entry)
	}
	if _, ok := client.buckets[bucket]; !ok {
		t.Fatal("expected the bucket to still exist under its deterministic name")
	}
}

func TestEnsure_RefusesUnownedBucketWithoutAdopt(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error for a pre-existing, differently-tagged bucket without adopt:true")
	}
}

func TestEnsure_AdoptsUntaggedBucketWhenRequested(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", Adopt: true}}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if !cloudctlaws.IsOwnedBy(client.buckets[bucket].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the bucket to be tagged as owned by this CR after adoption")
	}
	if client.buckets[bucket].tags["team"] != "someone-else" {
		t.Error("expected pre-existing tags to be preserved during adoption")
	}
}

func TestEnsure_RejectsBucketOwnedByDifferentCREvenWithAdopt(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "other-service"),
		cloudctlaws.OwnerUIDTagKey: "uid-2",
	}}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", Adopt: true}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a bucket owned by a different AppDependencies CR")
	}
}

func TestEnsure_RejectsReplicationAsNotYetSupported(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Replication: &depsv1alpha1.S3ReplicationSpec{Enabled: true, Region: "us-west-2"}},
	}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected replication to be explicitly rejected as unsupported")
	}
}

func TestEnsure_EnablesVersioningWhenBackupEnabled(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Backup: &depsv1alpha1.S3BackupSpec{Enabled: true}},
	}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if client.buckets[bucket].versioningStatus != types.BucketVersioningStatusEnabled {
		t.Errorf("expected versioning to be enabled, got %s", client.buckets[bucket].versioningStatus)
	}
}

func TestEnsure_AppliesDefaultLifecycleWhenBackupEnabledWithNoOverride(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Backup: &depsv1alpha1.S3BackupSpec{Enabled: true}},
	}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	rules := client.buckets[bucket].lifecycleRules
	if len(rules) != 1 {
		t.Fatalf("expected exactly one default lifecycle rule, got %d", len(rules))
	}
	if rules[0].Expiration != nil {
		t.Error("expected the default backup lifecycle to never expire the current object - that would delete live data")
	}
	if rules[0].NoncurrentVersionExpiration == nil {
		t.Error("expected the default backup lifecycle to clean up noncurrent versions")
	}
}

func TestEnsure_UsesOverrideLifecycleRulesInsteadOfDefault(t *testing.T) {
	client := newFakeS3()
	customDays := int32(90)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{
			Name:   "receipts",
			Backup: &depsv1alpha1.S3BackupSpec{Enabled: true},
			Overrides: &depsv1alpha1.S3Overrides{
				LifecycleRules: []depsv1alpha1.S3LifecycleRule{
					{ID: "custom-rule", ExpirationAfterDays: &customDays},
				},
			},
		},
	}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	rules := client.buckets[bucket].lifecycleRules
	if len(rules) != 1 || rules[0].ID == nil || *rules[0].ID != "custom-rule" {
		t.Errorf("expected the override rule to replace the default entirely, got %+v", rules)
	}
}

func TestEnsure_EmptyOverrideListDisablesLifecycleEvenWithBackupEnabled(t *testing.T) {
	// Regression test for the omitempty fix: an explicitly empty override
	// list must opt out of the default lifecycle policy entirely, not fall
	// back to it - this only works because S3Overrides.LifecycleRules has
	// no omitempty, letting an empty (non-nil) slice survive as distinct
	// from the field being absent.
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{
		tags:         map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
		hasLifecycle: true,
		lifecycleRules: []types.LifecycleRule{
			{ID: strPtr(defaultLifecycleRuleID)},
		},
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{
			Name:      "receipts",
			Backup:    &depsv1alpha1.S3BackupSpec{Enabled: true},
			Overrides: &depsv1alpha1.S3Overrides{LifecycleRules: []depsv1alpha1.S3LifecycleRule{}},
		},
	}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if client.buckets[bucket].hasLifecycle {
		t.Error("expected an explicitly empty override list to remove the lifecycle policy entirely, not apply the default")
	}
}

func TestEnsure_TruncatesBucketNameExceedingS3Limit(t *testing.T) {
	client := newFakeS3()
	longKey := strings.Repeat("a", 60)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: longKey}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	bucket := bucketName("default", "checkout-service", longKey, testAccountID)
	if len(bucket) > 63 {
		t.Errorf("computed bucket name is %d characters, want <= 63", len(bucket))
	}
	if status.FindManagedResource(ledger, resourceType, longKey) == nil {
		t.Error("expected a ledger entry for the bucket despite the over-length name")
	}
}

func TestEnsure_ContinuesToOtherBucketsAfterOneFails(t *testing.T) {
	client := newFakeS3()
	badBucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[badBucket] = &fakeBucket{tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts"}, // fails: untagged, no adopt
		{Name: "logs"},     // should still succeed
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error reported for the unowned receipts bucket")
	}
	if status.FindManagedResource(ledger, "s3", "logs") == nil {
		t.Error("expected logs to still be created despite receipts failing")
	}
}

func TestEnsure_SkipsRevalidationWithinTrustWindow(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	// The bucket must actually exist for reconcileBucketAttributes' own
	// calls (GetBucketVersioning etc.) to succeed - what's under test is
	// that HeadBucket/GetBucketTagging specifically are never called, not
	// that no AWS calls happen at all.
	client.buckets[bucket] = &fakeBucket{}
	client.headBucketErr = errors.New("should not be called: trust window should have skipped this")
	client.getBucketTaggingErr = errors.New("should not be called: trust window should have skipped this")

	bucketArn := "arn:aws:s3:::" + bucket
	fresh := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type:           resourceType,
			Name:           "receipts",
			ARN:            bucketArn,
			State:          depsv1alpha1.ManagedResourceStateVerified,
			DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt:      fresh,
			LastVerifiedAt: &fresh,
		},
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
	}}
	updatedLedger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v — expected the trust window to skip the AWS calls entirely", err)
	}

	entry := status.FindManagedResource(updatedLedger, "s3", "receipts")
	if entry == nil {
		t.Fatal("expected the ledger entry to survive the skip path")
	}
	if entry.ARN != bucketArn {
		t.Errorf("expected the cached ARN to be preserved, got %s", entry.ARN)
	}
	if entry.LastVerifiedAt == nil || !entry.LastVerifiedAt.Equal(&fresh) {
		t.Error("expected LastVerifiedAt to stay unchanged since no real verification occurred")
	}
}

func TestEnsure_UpdatesLocalFieldsEvenWhenSkippingRevalidation(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{}
	client.headBucketErr = errors.New("should not be called: trust window should have skipped this")
	client.getBucketTaggingErr = errors.New("should not be called: trust window should have skipped this")

	bucketArn := "arn:aws:s3:::" + bucket
	fresh := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type:           resourceType,
			Name:           "receipts",
			ARN:            bucketArn,
			State:          depsv1alpha1.ManagedResourceStateVerified,
			DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			Force:          false,
			CreatedAt:      fresh,
			LastVerifiedAt: &fresh,
		},
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	updatedLedger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(updatedLedger, "s3", "receipts")
	if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
		t.Errorf("expected deletionPolicy to update to Delete even while skipping revalidation, got %s", entry.DeletionPolicy)
	}
	if !entry.Force {
		t.Error("expected force to update to true even while skipping revalidation")
	}
}

func TestEnsure_RevalidatesAfterTrustWindowExpires(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	entry := status.FindManagedResource(ledger, "s3", "receipts")
	entry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, *entry)

	updatedLedger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	updatedEntry := status.FindManagedResource(updatedLedger, "s3", "receipts")
	if updatedEntry.LastVerifiedAt.Equal(&stale) {
		t.Error("expected LastVerifiedAt to be refreshed once the trust window expired and revalidation ran")
	}
}

type recordedEvent struct {
	eventType, reason, message string
}

func newEventCollector() (status.EventRecorder, *[]recordedEvent) {
	events := []recordedEvent{}
	return func(eventType, reason, message string) {
		events = append(events, recordedEvent{eventType, reason, message})
	}, &events
}

func TestEnsure_CreatingBucket_EmitsCreatedEvent(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "BucketCreated" {
		t.Errorf("expected exactly one BucketCreated event, got %+v", *events)
	}
}

func TestEnsure_AdoptingBucket_EmitsAdoptedEvent(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{"team": "someone-else"}}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", Adopt: true}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "BucketAdopted" {
		t.Errorf("expected exactly one BucketAdopted event, got %+v", *events)
	}
}
