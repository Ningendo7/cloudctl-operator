//go:build live

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

// Live tests run against a real AWS account - no LocalStack, no Kubernetes
// cluster. This package's own comments already flag several AWS behaviors
// it depends on but that the SDK gives no typed guarantee for (multiple
// distinct "bucket not found" exception types depending on which operation
// raised it, string-matched error codes for NoSuchTagSet and
// ServerSideEncryptionConfigurationNotFoundError, PutBucketTagging's
// full-replace semantics, and versioning's can-suspend-never-disable
// quirk) - each of those is exactly the kind of assumption this tier
// exists to check against the real service rather than LocalStack's
// simulation of it. Skipped entirely unless real credentials resolve via
// the standard AWS credential chain. Run explicitly with whatever already
// authenticates your AWS CLI:
//
//	go test -tags=live ./internal/resources/s3/... -v
package s3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/livetest"
	"github.com/Ningendo7/cloudctl-operator/internal/policycheck"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with a
// genuine, harmless STS call.
func skipUnlessLiveAWSCredentials(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}); err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}
	return cfg
}

func liveRegionAndAccount(t *testing.T, cfg aws.Config) (region, accountID string) {
	t.Helper()
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	return cfg.Region, *out.Account
}

// liveUniqueSuffix keeps each test's bucket name distinct so repeated runs
// against the same real account never collide. Unlike sqs/sns's own
// version of this helper, the test name has to be sanitized before use -
// S3 bucket names must be all lowercase with no underscores, a stricter
// character set than SQS/SNS resource names allow, and Go test names carry
// both.
func liveUniqueSuffix(t *testing.T) string {
	name := strings.ToLower(strings.ReplaceAll(t.Name()[len("TestLive_"):], "_", "-"))
	return "live-" + name + "-" + time.Now().UTC().Format("150405")
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

// deleteBucketIfExists reuses this package's own real cleanup helpers
// (object-version and multipart-upload teardown) rather than a separate,
// possibly-diverging implementation of the same "actually empty a real
// bucket" logic a raw DeleteBucket call needs first.
func deleteBucketIfExists(t *testing.T, client *s3sdk.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err != nil {
		return
	}
	_ = deleteAllObjectVersions(ctx, client, bucket)
	_ = abortMultipartUploads(ctx, client, bucket)
	_, _ = client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: &bucket})
}

// createRealBucket sets up a bucket outside of Ensure, for tests that need
// to control its starting tags/state directly.
func createRealBucket(t *testing.T, client *s3sdk.Client, bucket, region string) {
	t.Helper()
	input := &s3sdk.CreateBucketInput{Bucket: &bucket}
	if region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(region)}
	}
	if _, err := client.CreateBucket(context.Background(), input); err != nil {
		t.Fatalf("real CreateBucket() (setup) error = %v", err)
	}
}

// TestLive_Ensure_CreatesRealBucketInUsEast1AndTags exercises the
// us-east-1-has-no-LocationConstraint special case in ensureBucket's create
// path - the one region where AWS rejects the same CreateBucketConfiguration
// shape every other region requires - together with the account-hash
// bucket-naming scheme actually being accepted as a real, globally-unique
// name.
func TestLive_Ensure_CreatesRealBucketInUsEast1AndTags(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	if region != "us-east-1" {
		t.Skipf("this test exercises the us-east-1 no-LocationConstraint branch specifically, configured region is %q", region)
	}
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if findEntry(ledger, "receipts") == nil {
		t.Fatal("expected a ledger entry for receipts")
	}

	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("real HeadBucket() error = %v — bucket wasn't actually created", err)
	}
	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketTagging() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.TagSet), namespace, crName, "uid-1") {
		t.Errorf("real bucket tags don't satisfy IsOwnedBy: %+v", tagsOut.TagSet)
	}
}

// TestLive_Ensure_MissingBucketErrorsDeserializeAcrossOperations guards
// isNotFoundError's own documented claim that a missing bucket surfaces as
// three genuinely distinct typed exceptions depending on which operation
// raised it (NotFound from HeadBucket, NoSuchBucket from the tagging/
// lifecycle family) - unlike sns's ListTagsForResource mismatch, this
// package's code already accounts for the split, but that assumption had
// never been checked against the real service before this test.
func TestLive_Ensure_MissingBucketErrorsDeserializeAcrossOperations(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	_, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	bucket := bucketName("live", liveUniqueSuffix(t), "never-created", accountID)

	_, headErr := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket})
	if !isNotFoundError(headErr) {
		t.Errorf("real HeadBucket on a nonexistent bucket: isNotFoundError(%v) = false, want true", headErr)
	}
	_, tagErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if !isNotFoundError(tagErr) {
		t.Errorf("real GetBucketTagging on a nonexistent bucket: isNotFoundError(%v) = false, want true", tagErr)
	}
	_, lifecycleErr := client.DeleteBucketLifecycle(ctx, &s3sdk.DeleteBucketLifecycleInput{Bucket: &bucket})
	if !isNotFoundError(lifecycleErr) {
		t.Errorf("real DeleteBucketLifecycle on a nonexistent bucket: isNotFoundError(%v) = false, want true", lifecycleErr)
	}
}

// TestLive_Ensure_AdoptsUntaggedRealBucketWithNoTagSet guards
// isNoSuchTagSet's string-matched error code - the SDK has no typed
// exception for "bucket exists but has never been tagged," so this is the
// one path in the package with no compiler-checked guarantee it's matching
// the right thing at all.
func TestLive_Ensure_AdoptsUntaggedRealBucketWithNoTagSet(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	createRealBucket(t, client, bucket, region)

	_, tagErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if !isNoSuchTagSet(tagErr) {
		t.Fatalf("expected a real untagged bucket's GetBucketTagging error to satisfy isNoSuchTagSet, got %v", tagErr)
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketTagging() after adopt error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.TagSet), namespace, crName, "uid-1") {
		t.Errorf("expected adopt:true to tag the untagged real bucket as owned, got %+v", tagsOut.TagSet)
	}
}

// TestLive_Ensure_AdoptingBucketPreservesPreExistingTagsViaMergeWrite
// guards the read-merge-write discipline PutBucketTagging needs: unlike
// SQS/SNS's additive TagResource, PutBucketTagging replaces the entire tag
// set in one call, so writing only the owner tag would silently destroy any
// tag a human or another tool had already set.
func TestLive_Ensure_AdoptingBucketPreservesPreExistingTagsViaMergeWrite(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	createRealBucket(t, client, bucket, region)

	if _, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  &bucket,
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: strPtr("team"), Value: strPtr("payments")}}},
	}); err != nil {
		t.Fatalf("real PutBucketTagging() (setup) error = %v", err)
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketTagging() error = %v", err)
	}
	got := tagsToMap(tagsOut.TagSet)
	if got["team"] != "payments" {
		t.Errorf("expected the pre-existing human tag to survive adoption, got tags %+v", got)
	}
	if !cloudctlaws.IsOwnedBy(got, namespace, crName, "uid-1") {
		t.Errorf("expected the owner tag to also be present after adoption, got tags %+v", got)
	}
}

// TestLive_Ensure_ServerSideEncryptionNotFoundThenSharedKeyApplies
// guards reconcileBucketAttributes' encryption-drift comparison against a
// real bucket's actual starting state. Contrary to
// isServerSideEncryptionConfigurationNotFound's own doc comment (accurate
// for a bucket predating AWS's default-SSE-S3-for-every-new-bucket
// rollout), a freshly created bucket's GetBucketEncryption call doesn't
// error at all - it returns a real default AES256 rule with no
// KMSMasterKeyID, which currentKeyARN's extraction loop has to correctly
// treat as "no KMS key set yet" rather than mistaking the default rule
// itself for a match. Then confirms the dedicated-KMS-key path (already
// proven against real AWS via sqs's own live tier, sharing the same kms
// package code with a different resourceType) applies correctly through
// S3's own GetBucketEncryption/PutBucketEncryption calls specifically.
func TestLive_Ensure_ServerSideEncryptionNotFoundThenSharedKeyApplies(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	createRealBucket(t, client, bucket, region)

	initialEnc, encErr := client.GetBucketEncryption(ctx, &s3sdk.GetBucketEncryptionInput{Bucket: &bucket})
	if encErr != nil {
		t.Fatalf("real GetBucketEncryption() on a freshly created bucket error = %v", encErr)
	}
	for _, rule := range initialEnc.ServerSideEncryptionConfiguration.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil && rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID != nil {
			t.Fatalf("expected a freshly created bucket's default encryption rule to carry no KMS key, got %q", *rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID)
		}
	}

	keyEntry := livetest.SharedKey(t, cfg)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: livetest.SharedKeyName}},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, []depsv1alpha1.ManagedResource{keyEntry}, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	t.Cleanup(func() {
		if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, true, false, nil); err != nil {
			t.Logf("cleanup warning: %v", err)
		}
	})

	encOut, err := client.GetBucketEncryption(ctx, &s3sdk.GetBucketEncryptionInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketEncryption() error = %v", err)
	}
	var gotKeyARN string
	var bucketKey bool
	for _, rule := range encOut.ServerSideEncryptionConfiguration.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil && rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID != nil {
			gotKeyARN = *rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID
			bucketKey = aws.ToBool(rule.BucketKeyEnabled)
		}
	}
	if gotKeyARN != keyEntry.ARN {
		t.Errorf("real bucket's SSE-KMS key = %q, want the shared key %q", gotKeyARN, keyEntry.ARN)
	}
	if !bucketKey {
		t.Error("expected the bucket's encryption rule to have Bucket Keys on")
	}

	// A write and a read prove the key policy lets S3 use the key with a
	// Bucket Key, whose encryption context is the bucket ARN.
	object := "live-probe"
	if _, err := client.PutObject(ctx, &s3sdk.PutObjectInput{Bucket: &bucket, Key: &object, Body: strings.NewReader("hello")}); err != nil {
		t.Fatalf("real PutObject() into the encrypted bucket error = %v", err)
	}
	got, err := client.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: &bucket, Key: &object})
	if err != nil {
		t.Fatalf("real GetObject() from the encrypted bucket error = %v", err)
	}
	_ = got.Body.Close()
	if got.ServerSideEncryption != types.ServerSideEncryptionAwsKms || aws.ToString(got.SSEKMSKeyId) != keyEntry.ARN {
		t.Errorf("object encrypted with %q / %q, want aws:kms under %q", got.ServerSideEncryption, aws.ToString(got.SSEKMSKeyId), keyEntry.ARN)
	}
}

// TestLive_RelinquishIfStillTagged_TellsGoneFromUntaggedFromOwned checks that
// NoSuchBucket means gone, NoSuchTagSet means exists, and letting go leaves
// the released-from tag.
func TestLive_RelinquishIfStillTagged_TellsGoneFromUntaggedFromOwned(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	entryFor := func(name string) depsv1alpha1.ManagedResource {
		return depsv1alpha1.ManagedResource{Type: resourceType, Name: name, ARN: "arn:aws:s3:::" + bucketName(namespace, crName, name, accountID)}
	}

	t.Run("missing bucket is gone", func(t *testing.T) {
		_, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, "uid-1", entryFor("missing"), nil)
		if err != nil || !gone {
			t.Errorf("relinquishIfStillTagged() gone=%v err=%v, want gone", gone, err)
		}
	})

	t.Run("bucket without a tag set exists", func(t *testing.T) {
		bucket := bucketName(namespace, crName, "untagged", accountID)
		t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
		createRealBucket(t, client, bucket, region)
		relinquished, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, "uid-1", entryFor("untagged"), nil)
		if err != nil || gone || relinquished {
			t.Errorf("relinquishIfStillTagged() relinquished=%v gone=%v err=%v, want none of them", relinquished, gone, err)
		}
	})

	t.Run("owned bucket gives up its last tag", func(t *testing.T) {
		bucket := bucketName(namespace, crName, "owned", accountID)
		t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
		createRealBucket(t, client, bucket, region)
		if _, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
			Bucket:  &bucket,
			Tagging: &types.Tagging{TagSet: mapToTags(ownerTags(namespace, crName, "uid-1"))},
		}); err != nil {
			t.Fatalf("real PutBucketTagging() (setup) error = %v", err)
		}
		relinquished, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, "uid-1", entryFor("owned"), nil)
		if err != nil || gone || !relinquished {
			t.Fatalf("relinquishIfStillTagged() relinquished=%v gone=%v err=%v, want relinquished", relinquished, gone, err)
		}
		tagsOut, tagErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
		if tagErr != nil {
			t.Fatalf("GetBucketTagging() after relinquishing error = %v", tagErr)
		}
		got := tagsToMap(tagsOut.TagSet)
		if _, owned := got[cloudctlaws.OwnerTagKey]; owned || got[cloudctlaws.ReleasedFromTagKey] != namespace+"/"+crName {
			t.Errorf("expected the ownership tags gone and released-from left, got %v", got)
		}
	})
}

// TestLive_Ensure_VersioningIsSuspendedOnlyByAnExplicitOverride guards the
// raise-only rule against a real bucket - turning backup off, or dropping it
// from the spec, must leave versioning enabled, since AWS Backup and other
// tools may depend on it - and the well-known AWS quirk behind the one way to
// turn it off: once versioning has been enabled it can never go back to
// "unset", only to Suspended.
func TestLive_Ensure_VersioningIsSuspendedOnlyByAnExplicitOverride(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	versioning := func() types.BucketVersioningStatus {
		out, err := client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{Bucket: &bucket})
		if err != nil {
			t.Fatalf("real GetBucketVersioning() error = %v", err)
		}
		return out.Status
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Backup: &depsv1alpha1.S3BackupSpec{Enabled: true}},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if got := versioning(); got != types.BucketVersioningStatusEnabled {
		t.Fatalf("real versioning status = %q, want Enabled", got)
	}

	spec.Resources[0].Backup.Enabled = false
	if ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() with backup turned off error = %v", err)
	}
	if got := versioning(); got != types.BucketVersioningStatusEnabled {
		t.Errorf("real versioning status after turning backup off = %q, want Enabled", got)
	}

	spec.Resources[0].Backup = nil
	if ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() with backup dropped error = %v", err)
	}
	if got := versioning(); got != types.BucketVersioningStatusEnabled {
		t.Errorf("real versioning status after dropping backup = %q, want Enabled", got)
	}

	spec.Resources[0].Overrides = &depsv1alpha1.S3Overrides{VersioningEnabled: aws.Bool(false)}
	if _, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() with versioning explicitly off error = %v", err)
	}
	if got := versioning(); got != types.BucketVersioningStatusSuspended {
		t.Errorf("real versioning status with the explicit override = %q, want Suspended (AWS can never fully un-version a bucket)", got)
	}
}

// TestLive_Cleanup_BlocksDeletingBucketWithARealObjectThenSucceedsOnceEmpty
// exercises bucketIsEmpty's real ListObjectVersions round trip, not a
// fake's hand-set field.
func TestLive_Cleanup_BlocksDeletingBucketWithARealObjectThenSucceedsOnceEmpty(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: false},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	key := "keep-this-bucket-non-empty.txt"
	if _, err := client.PutObject(ctx, &s3sdk.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("hello")}); err != nil {
		t.Fatalf("real PutObject() error = %v", err)
	}

	// First pass only marks pending (quiet window); backdate it so the next
	// pass evaluates the real emptiness check instead of waiting.
	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	entry, _ := status.FindManagedResource(ledger, "s3", "receipts")
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, entry)

	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal("expected the real bucket with a genuine object still in it to survive Cleanup, but it's gone")
	}

	if _, err := client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("real DeleteObject() error = %v", err)
	}
	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("third Cleanup() error = %v", err)
	}
	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err == nil {
		t.Error("expected the real bucket to be deleted once emptied")
	}
}

// TestLive_Ensure_RefusesBucketOwnedByADifferentRealCR confirms the
// stale-UID/ownership check against real, round-tripped tags read via S3's
// own GetBucketTagging shape.
func TestLive_Ensure_RefusesBucketOwnedByADifferentRealCR(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	ownerSpec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "owner-uid", region, accountID, ownerSpec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (establishing the real owner) error = %v", err)
	}

	intruderSpec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "intruder-uid", region, accountID, intruderSpec, nil, nil, nil); err == nil {
		t.Error("expected Ensure to refuse a real bucket already owned by a different CR's UID, even with adopt:true")
	}
}

// TestLive_Cleanup_DenyPolicyActuallyBlocksPutObjectThenClearingRestoresIt
// confirms real enforcement of the pending-deletion deny policy -
// LocalStack's community edition doesn't evaluate bucket policies at all
// unless ENFORCE_IAM=1 is set, so the integration tier can only confirm the
// policy JSON is written correctly, not that it actually blocks anything.
// Also confirms s3:PutObject is a real, recognized action for a bucket
// policy - the same class of bug already found once in sqs's own live tier
// (sqs:SendMessageBatch turning out not to be a real action).
func TestLive_Cleanup_DenyPolicyActuallyBlocksPutObjectThenClearingRestoresIt(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	key := "hello.txt"
	if _, err := client.PutObject(ctx, &s3sdk.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("hello")}); err != nil {
		t.Fatalf("real PutObject() before any deny policy error = %v — expected it to succeed", err)
	}

	if err := addPendingDeletionDeny(ctx, client, bucket); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	if _, err := client.PutObject(ctx, &s3sdk.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("hello")}); err == nil {
		t.Fatal("expected real PutObject to be denied after adding the pending-deletion deny policy")
	}

	if err := removePendingDeletionDeny(ctx, client, bucket); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}
	if _, err := client.PutObject(ctx, &s3sdk.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("hello")}); err != nil {
		t.Errorf("real PutObject() after removing the deny policy error = %v — expected it to succeed again", err)
	}
}

// TestLive_Ensure_TakesOverABucketFromAPreviousUID checks a restored CR is
// refused a bucket tagged with its old UID, and takes it over when told which.
func TestLive_Ensure_TakesOverABucketFromAPreviousUID(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true}}}

	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-2", region, accountID, spec, nil, nil, nil); err == nil {
		t.Fatal("expected a new UID to be refused without adopt-from-uid")
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, "uid-1")
	if _, err := Ensure(takeover, client, nil, nil, namespace, crName, "uid-2", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("GetBucketTagging() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.TagSet), namespace, crName, "uid-2") {
		t.Errorf("expected the bucket to be tagged with the new UID, got %+v", tagsOut.TagSet)
	}
}

// TestLive_Ensure_AdoptingAgainRemovesOnlyTheReleasedTags checks against real
// S3, which replaces the whole tag set on every write, that taking a released
// bucket back drops the two released tags and keeps someone else's tag.
func TestLive_Ensure_AdoptingAgainRemovesOnlyTheReleasedTags(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}}}

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	current, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("GetBucketTagging() error = %v", err)
	}
	withTeam := cloudctlaws.MergeTags(tagsToMap(current.TagSet), map[string]string{"team": "payments"})
	if _, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{Bucket: &bucket, Tagging: &types.Tagging{TagSet: mapToTags(withTeam)}}); err != nil {
		t.Fatalf("PutBucketTagging() error = %v", err)
	}
	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	spec.Resources[0].Adopt = true
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() with adopt error = %v", err)
	}
	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("GetBucketTagging() error = %v", err)
	}
	got := tagsToMap(tagsOut.TagSet)
	if cloudctlaws.HasReleaseTags(got) || got["team"] != "payments" || !cloudctlaws.IsOwnedBy(got, namespace, crName, "uid-1") {
		t.Errorf("expected ownership back, the released tags gone and the other tag kept, got %v", got)
	}
}

// plainHTTPClient talks to S3 over HTTP, which the transport statement must
// refuse.
func plainHTTPClient(cfg aws.Config) *s3sdk.Client {
	return s3sdk.NewFromConfig(cfg, func(o *s3sdk.Options) {
		o.BaseEndpoint = aws.String("http://s3." + cfg.Region + ".amazonaws.com")
		o.UsePathStyle = true
		o.HTTPClient = livetest.FreshConnections()
	})
}

func waitUntil(within time.Duration, check func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// countingS3 counts the policy writes Ensure makes.
type countingS3 struct {
	s3API
	policyWrites int
}

func (c *countingS3) PutBucketPolicy(ctx context.Context, in *s3sdk.PutBucketPolicyInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketPolicyOutput, error) {
	c.policyWrites++
	return c.s3API.PutBucketPolicy(ctx, in, optFns...)
}

// policyText reads the policy as S3 hands it back, empty when there is none.
func policyText(t *testing.T, client *s3sdk.Client, bucket string) string {
	t.Helper()
	out, err := client.GetBucketPolicy(context.Background(), &s3sdk.GetBucketPolicyInput{Bucket: &bucket})
	if isNoSuchBucketPolicy(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("real GetBucketPolicy() error = %v", err)
	}
	return aws.ToString(out.Policy)
}

func adoptedSpec() *depsv1alpha1.S3Spec {
	return &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
}

// TestLive_Ensure_TheTransportStatementIsAcceptedEnforcedAndNotRewritten
// confirms S3 accepts the statement and Access Analyzer finds nothing wrong
// with it, that a request over plain HTTP is refused while HTTPS is not, that
// a second pass writes nothing (S3 hands the policy back reformatted, which
// must still compare equal), that a bucket without a Block Public Access
// configuration is read without error, and that taking the statement away
// lets HTTP through again, so the refusal was the statement's.
func TestLive_Ensure_TheTransportStatementIsAcceptedEnforcedAndNotRewritten(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	recordEvent, events := newEventCollector()
	counted := &countingS3{s3API: client}
	ledger, err := Ensure(ctx, counted, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	policy := policyText(t, client, bucket)
	if got := strings.Count(policy, awspolicy.InsecureTransportSid); got != 1 {
		t.Fatalf("expected the transport statement once in the bucket policy, found %d in %s", got, policy)
	}
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.S3BucketPolicy, policy)
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("expected one TransportSecurityEnforced event, got %v", *events)
	}
	for _, reason := range []string{"PublicAccessCheckFailed", "BucketPubliclyAccessible"} {
		if countEvents(*events, reason) != 0 {
			t.Errorf("a fresh private bucket raised %s: %v", reason, *events)
		}
	}

	plain := plainHTTPClient(cfg)
	listOver := func(c *s3sdk.Client) error {
		_, err := c.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{Bucket: &bucket})
		return err
	}
	if err := listOver(client); err != nil {
		t.Fatalf("real ListObjectsV2 over HTTPS error = %v: the statement must not refuse TLS", err)
	}
	if !waitUntil(2*time.Minute, func() bool { return hasErrorCode(listOver(plain), "AccessDenied") }) {
		t.Fatal("a request over plain HTTP was still accepted: the statement did not take effect")
	}

	writes := counted.policyWrites
	if _, err := Ensure(ctx, counted, nil, nil, namespace, crName, "uid-1", region, accountID, spec, expireTrust(ledger), nil, recordEvent); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if counted.policyWrites != writes {
		t.Errorf("the second pass wrote the bucket policy %d more time(s): the stored policy reads as different from the desired one", counted.policyWrites-writes)
	}

	if _, err := client.DeletePublicAccessBlock(ctx, &s3sdk.DeletePublicAccessBlockInput{Bucket: &bucket}); err != nil {
		t.Fatalf("real DeletePublicAccessBlock() error = %v", err)
	}
	if _, err := Ensure(ctx, counted, nil, nil, namespace, crName, "uid-1", region, accountID, spec, expireTrust(ledger), nil, recordEvent); err != nil {
		t.Fatalf("Ensure() with no Block Public Access configuration error = %v", err)
	}
	if countEvents(*events, "PublicAccessCheckFailed") != 0 {
		t.Errorf("a bucket with no Block Public Access configuration could not be checked: %v", *events)
	}

	if changed, err := syncTransportPolicy(ctx, client, bucket, false); err != nil || !changed {
		t.Fatalf("removing the statement: changed = %v, error = %v", changed, err)
	}
	var lastErr error
	if !waitUntil(2*time.Minute, func() bool { lastErr = listOver(plain); return lastErr == nil }) {
		t.Errorf("plain HTTP is still refused after the statement was removed (%v); the policy now reads %q", lastErr, policyText(t, client, bucket))
	}
}

// TestLive_Ensure_AWebsiteBucketIsNotGivenTheTransportStatement confirms the
// website check reads S3's real answers both ways (a configuration, and
// NoSuchWebsiteConfiguration), that a website bucket is left without the
// statement and reported, and that the statement arrives once the website goes.
func TestLive_Ensure_AWebsiteBucketIsNotGivenTheTransportStatement(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	createRealBucket(t, client, bucket, region)
	if _, err := client.PutBucketWebsite(ctx, &s3sdk.PutBucketWebsiteInput{
		Bucket: &bucket,
		WebsiteConfiguration: &types.WebsiteConfiguration{
			IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		},
	}); err != nil {
		t.Fatalf("real PutBucketWebsite() (setup) error = %v", err)
	}

	recordEvent, events := newEventCollector()
	spec := adoptedSpec()
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() on a website bucket error = %v", err)
	}
	if policy := policyText(t, client, bucket); strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("a website bucket was given the transport statement, which would take the site down: %s", policy)
	}
	if countEvents(*events, "TransportSecurityNotEnforced") != 1 {
		t.Errorf("expected one TransportSecurityNotEnforced warning, got %v", *events)
	}

	if _, err := client.DeleteBucketWebsite(ctx, &s3sdk.DeleteBucketWebsiteInput{Bucket: &bucket}); err != nil {
		t.Fatalf("real DeleteBucketWebsite() error = %v", err)
	}
	if !waitUntil(time.Minute, func() bool {
		_, err := client.GetBucketWebsite(ctx, &s3sdk.GetBucketWebsiteInput{Bucket: &bucket})
		return hasErrorCode(err, errCodeNoSuchWebsite)
	}) {
		t.Fatal("S3 still reports the website after it was deleted")
	}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, expireTrust(ledger), nil, recordEvent); err != nil {
		t.Fatalf("Ensure() after the website was removed error = %v", err)
	}
	if policy := policyText(t, client, bucket); !strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("the transport statement did not arrive once the website was removed: %s", policy)
	}
}

// createOpenableBucket makes a bucket public access can be given to: ACLs
// enabled and no Block Public Access. It skips the test when the account
// blocks public access for all its buckets, which a bucket cannot override.
func createOpenableBucket(t *testing.T, client *s3sdk.Client, bucket, region string) {
	t.Helper()
	input := &s3sdk.CreateBucketInput{Bucket: &bucket, ObjectOwnership: types.ObjectOwnershipBucketOwnerPreferred}
	if region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(region)}
	}
	if _, err := client.CreateBucket(context.Background(), input); err != nil {
		t.Fatalf("real CreateBucket() (setup) error = %v", err)
	}
	setPublicAccessBlock(t, client, bucket, types.PublicAccessBlockConfiguration{
		BlockPublicAcls: aws.Bool(false), IgnorePublicAcls: aws.Bool(false),
		BlockPublicPolicy: aws.Bool(false), RestrictPublicBuckets: aws.Bool(false),
	})
}

func setPublicAccessBlock(t *testing.T, client *s3sdk.Client, bucket string, block types.PublicAccessBlockConfiguration) {
	t.Helper()
	if _, err := client.PutPublicAccessBlock(context.Background(), &s3sdk.PutPublicAccessBlockInput{
		Bucket: &bucket, PublicAccessBlockConfiguration: &block,
	}); err != nil {
		t.Fatalf("real PutPublicAccessBlock() error = %v", err)
	}
}

// TestLive_Ensure_ReportsABucketOpenThroughItsPolicyUnlessBlockPublicAccessIgnoresIt
// confirms GetBucketPolicyStatus and GetPublicAccessBlock tell, on a real
// bucket, a policy that exposes it from one the block ignores, and that a
// policy someone else wrote survives next to the operator's statement.
func TestLive_Ensure_ReportsABucketOpenThroughItsPolicyUnlessBlockPublicAccessIgnoresIt(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	createOpenableBucket(t, client, bucket, region)

	public := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Sid":"SomeoneElsesPublicRead","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}`, bucket)
	if _, err := client.PutBucketPolicy(ctx, &s3sdk.PutBucketPolicyInput{Bucket: &bucket, Policy: &public}); err != nil {
		t.Skipf("the account does not allow a public bucket policy, so there is nothing to detect: %v", err)
	}
	status, err := client.GetBucketPolicyStatus(ctx, &s3sdk.GetBucketPolicyStatusInput{Bucket: &bucket})
	if err != nil || !boolValue(status.PolicyStatus.IsPublic) {
		t.Fatalf("setup: expected S3 to call the policy public, got %+v, error = %v", status, err)
	}

	recordEvent, events := newEventCollector()
	spec := adoptedSpec()
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if countEvents(*events, "BucketPubliclyAccessible") != 1 {
		t.Fatalf("expected one BucketPubliclyAccessible warning, got %v", *events)
	}
	for _, e := range *events {
		if e.reason == "BucketPubliclyAccessible" && !strings.Contains(e.message, "its bucket policy") {
			t.Errorf("the warning should name the policy: %s", e.message)
		}
	}
	policy := policyText(t, client, bucket)
	if !strings.Contains(policy, "SomeoneElsesPublicRead") || !strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("expected the other statement kept and the operator's added, got %s", policy)
	}
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.S3BucketPolicy, policy)

	setPublicAccessBlock(t, client, bucket, types.PublicAccessBlockConfiguration{
		BlockPublicAcls: aws.Bool(false), IgnorePublicAcls: aws.Bool(false),
		BlockPublicPolicy: aws.Bool(false), RestrictPublicBuckets: aws.Bool(true),
	})
	recordEvent, events = newEventCollector()
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, expireTrust(ledger), nil, recordEvent); err != nil {
		t.Fatalf("Ensure() with the policy restricted error = %v", err)
	}
	if countEvents(*events, "BucketPubliclyAccessible") != 0 {
		t.Errorf("a policy that Block Public Access ignores was reported as public: %v", *events)
	}
}

// TestLive_Ensure_ReportsABucketOpenThroughItsACLUnlessBlockPublicAccessIgnoresIt
// is the same for an ACL, read with GetBucketAcl.
func TestLive_Ensure_ReportsABucketOpenThroughItsACLUnlessBlockPublicAccessIgnoresIt(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })
	createOpenableBucket(t, client, bucket, region)

	if _, err := client.PutBucketAcl(ctx, &s3sdk.PutBucketAclInput{Bucket: &bucket, ACL: types.BucketCannedACLPublicRead}); err != nil {
		t.Skipf("the account does not allow a public ACL, so there is nothing to detect: %v", err)
	}

	recordEvent, events := newEventCollector()
	spec := adoptedSpec()
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if countEvents(*events, "BucketPubliclyAccessible") != 1 {
		t.Fatalf("expected one BucketPubliclyAccessible warning, got %v", *events)
	}
	for _, e := range *events {
		if e.reason == "BucketPubliclyAccessible" && !strings.Contains(e.message, "its ACL") {
			t.Errorf("the warning should name the ACL: %s", e.message)
		}
	}

	setPublicAccessBlock(t, client, bucket, types.PublicAccessBlockConfiguration{
		BlockPublicAcls: aws.Bool(false), IgnorePublicAcls: aws.Bool(true),
		BlockPublicPolicy: aws.Bool(false), RestrictPublicBuckets: aws.Bool(false),
	})
	recordEvent, events = newEventCollector()
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, expireTrust(ledger), nil, recordEvent); err != nil {
		t.Fatalf("Ensure() with ACLs ignored error = %v", err)
	}
	if countEvents(*events, "BucketPubliclyAccessible") != 0 {
		t.Errorf("an ACL that Block Public Access ignores was reported as public: %v", *events)
	}
}

// TestLive_CreatingATaggedBucketNeedsTagResource checks the permissions
// table's "docs, verify" entry for s3:TagResource on a role that has only
// what CreateBucket itself needs, with and without it.
func TestLive_CreatingATaggedBucketNeedsTagResource(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	admin := s3sdk.NewFromConfig(cfg)
	region, _ := liveRegionAndAccount(t, cfg)
	ctx := context.Background()

	create := func(actions []string, label string) error {
		limited := s3sdk.NewFromConfig(livetest.LimitedConfig(t, cfg, actions))
		var probeErr error
		if !livetest.WaitUntil(90*time.Second, func() bool {
			_, probeErr = limited.ListBuckets(ctx, &s3sdk.ListBucketsInput{})
			return probeErr == nil
		}) {
			t.Fatalf("the limited role's policy never took effect: %v", probeErr)
		}
		bucket := fmt.Sprintf("live-tagged-%s-%d", label, time.Now().UnixNano()%1_000_000_000)
		t.Cleanup(func() { deleteBucketIfExists(t, admin, bucket) })
		input := &s3sdk.CreateBucketInput{Bucket: &bucket, CreateBucketConfiguration: &types.CreateBucketConfiguration{
			Tags: []types.Tag{{Key: aws.String("cloudctl.io/owner"), Value: aws.String("live/test")}},
		}}
		if region != "us-east-1" {
			input.CreateBucketConfiguration.LocationConstraint = types.BucketLocationConstraint(region)
		}
		_, err := limited.CreateBucket(ctx, input)
		return err
	}
	base := []string{"s3:CreateBucket", "s3:ListAllMyBuckets"}
	with := create(append(append([]string{}, base...), "s3:TagResource"), "with")
	without := create(base, "without")
	livetest.CheckActionNeeded(t, "s3:TagResource", with, without)
}

// expireTrust returns the ledger as it would be after the trust window has
// passed, so the next Ensure verifies everything instead of only what is
// checked within the window.
func expireTrust(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
	past := metav1.NewTime(time.Now().Add(-(status.TrustWindow + time.Minute)))
	out := append([]depsv1alpha1.ManagedResource{}, ledger...)
	for i := range out {
		out[i].LastVerifiedAt = &past
	}
	return out
}
