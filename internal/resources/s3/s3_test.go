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

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	expected := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, ok := client.buckets[expected]; !ok {
		t.Fatalf("expected bucket %q to be created", expected)
	}
	if !strings.HasPrefix(expected, "default-checkout-service-receipts-") {
		t.Errorf("expected the base name to be preserved before the hash suffix, got %q", expected)
	}
	if len(expected) != len("default-checkout-service-receipts-")+8 {
		t.Errorf("expected an 8-character account hash suffix, got %q", expected)
	}

	entry := status.FindManagedResource(ledger, "s3", "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the bucket to reach Verified after a successful tag write, got %+v", entry)
	}
}

func TestEnsure_ProvisionsDedicatedKeyWhenEncryptionEnabled(t *testing.T) {
	client := newFakeS3()
	kmsClient := newFakeKMSClient()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}

	ledger, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", "receipts-key")
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
	kmsClient := newFakeKMSClient()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
	}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}

	ledger, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", "receipts-key")
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

	_, err := Ensure(context.Background(), client, nil, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
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

	_, err := Ensure(context.Background(), client, nil, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
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

	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil); err != nil {
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

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
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

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
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

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
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

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected 301 to be reported as an error")
	}
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, exists := client.buckets[bucket]; exists {
		t.Error("expected no CreateBucket attempt on a 301")
	}
}

func TestEnsure_TagsBucketAfterCreationNotAtomically(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if !cloudctlaws.IsOwnedBy(client.buckets[bucket].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the bucket to end up tagged as owned by this CR")
	}
	entry := status.FindManagedResource(ledger, "s3", "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected Verified once tagging succeeds, got %+v", entry)
	}
}

func TestEnsure_RecordsTagPendingWhenTaggingFailsAfterCreate(t *testing.T) {
	client := newFakeS3()
	client.putBucketTaggingErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultServer}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error when tagging fails after creation")
	}

	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	if _, ok := client.buckets[bucket]; !ok {
		t.Fatal("expected the bucket to still exist - creation itself succeeded, only tagging failed")
	}
	entry := status.FindManagedResource(ledger, "s3", "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateTagPending {
		t.Errorf("expected the ledger to record TagPending after create-succeeded-but-tag-failed, got %+v", entry)
	}
}

func TestEnsure_RetriesTaggingOnNextReconcileWithinClaimWindow(t *testing.T) {
	client := newFakeS3()
	client.putBucketTaggingErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultServer}
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected the first Ensure() to fail while tagging is broken")
	}

	client.putBucketTaggingErr = nil // simulate the transient failure clearing up
	ledger, err = Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, "s3", "receipts")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the retry to succeed and reach Verified, got %+v", entry)
	}
}

func TestEnsure_RefusesStaleTagPendingClaimAfterWindowExpires(t *testing.T) {
	// Security-critical bound: an untagged bucket found under our expected
	// name is only trusted as "probably ours, tag write just failed" for a
	// bounded window after creation - not forever. Past that window, it
	// must be treated the same as any other foreign, untagged bucket.
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{} // exists, untagged

	longAgo := metav1.NewTime(time.Now().Add(-2 * tagRetryClaimWindow))
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type:      resourceType,
			Name:      "receipts",
			ARN:       "arn:aws:s3:::" + bucket,
			State:     depsv1alpha1.ManagedResourceStateTagPending,
			CreatedAt: longAgo,
		},
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger)
	if err == nil {
		t.Fatal("expected Ensure to refuse claiming an untagged bucket once the claim window has expired")
	}
}

func TestEnsure_RefusesUnownedBucketWithoutAdopt(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error for a pre-existing, differently-tagged bucket without adopt:true")
	}
}

func TestEnsure_AdoptsUntaggedBucketWhenRequested(t *testing.T) {
	client := newFakeS3()
	bucket := bucketName("default", "checkout-service", "receipts", testAccountID)
	client.buckets[bucket] = &fakeBucket{tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", Adopt: true}}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil); err != nil {
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
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a bucket owned by a different AppDependencies CR")
	}
}

func TestEnsure_RejectsReplicationAsNotYetSupported(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Replication: &depsv1alpha1.S3ReplicationSpec{Enabled: true, Region: "us-west-2"}},
	}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected replication to be explicitly rejected as unsupported")
	}
}

func TestEnsure_EnablesVersioningWhenBackupEnabled(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", Backup: &depsv1alpha1.S3BackupSpec{Enabled: true}},
	}}
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil); err != nil {
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
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil); err != nil {
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
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil); err != nil {
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
	if _, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if client.buckets[bucket].hasLifecycle {
		t.Error("expected an explicitly empty override list to remove the lifecycle policy entirely, not apply the default")
	}
}

func TestEnsure_RejectsBucketNameExceedingS3Limit(t *testing.T) {
	client := newFakeS3()
	longKey := strings.Repeat("a", 60)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: longKey}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error for a computed bucket name exceeding S3's 63-character limit")
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
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error reported for the unowned receipts bucket")
	}
	if status.FindManagedResource(ledger, "s3", "logs") == nil {
		t.Error("expected logs to still be created despite receipts failing")
	}
}
