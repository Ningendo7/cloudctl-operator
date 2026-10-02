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
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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

// TestLive_Ensure_ServerSideEncryptionNotFoundThenDedicatedKeyApplies
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
func TestLive_Ensure_ServerSideEncryptionNotFoundThenDedicatedKeyApplies(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	kmsClient := kms.NewFromConfig(cfg)
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

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true, Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}
	ledger, err := Ensure(ctx, client, kmsClient, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	t.Cleanup(func() {
		if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, true, false, nil); err != nil {
			t.Logf("key cleanup warning: %v", err)
		}
		// Cleanup only tears down "s3"-type ledger entries, and the kms
		// package's own Cleanup won't schedule deletion within a single
		// pass (it only starts a quiet window) - delete the real key
		// directly instead of leaving it dangling.
		if keyEntry := status.FindManagedResource(ledger, "kms", cloudctlaws.DedicatedKeyLedgerName(resourceType, "receipts")); keyEntry != nil {
			windowDays := int32(7)
			if _, err := kmsClient.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: &keyEntry.ARN, PendingWindowInDays: &windowDays}); err != nil {
				t.Logf("key cleanup warning: %v", err)
			}
		}
	})

	keyEntry := status.FindManagedResource(ledger, "kms", cloudctlaws.DedicatedKeyLedgerName(resourceType, "receipts"))
	if keyEntry == nil {
		t.Fatal("expected a dedicated KMS key ledger entry for the encrypted bucket")
	}

	encOut, err := client.GetBucketEncryption(ctx, &s3sdk.GetBucketEncryptionInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketEncryption() error = %v", err)
	}
	var gotKeyARN string
	for _, rule := range encOut.ServerSideEncryptionConfiguration.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil && rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID != nil {
			gotKeyARN = *rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID
		}
	}
	if gotKeyARN != keyEntry.ARN {
		t.Errorf("real bucket's SSE-KMS key = %q, want the dedicated key's real ARN %q", gotKeyARN, keyEntry.ARN)
	}
}

// TestLive_Ensure_VersioningSuspendedNotDisabledOnceEnabled guards a
// well-known but easy-to-get-wrong AWS quirk: once a bucket's versioning has
// ever been enabled, it can never go back to "unset" - only Suspended. The
// boolean comparison reconcileBucketAttributes uses happens to tolerate
// both states, but that's only correct if AWS genuinely never reports
// anything else once versioning has been turned on.
func TestLive_Ensure_VersioningSuspendedNotDisabledOnceEnabled(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := s3sdk.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	bucket := bucketName(namespace, crName, "receipts", accountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Backup: &depsv1alpha1.S3BackupSpec{Enabled: true}},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	verOut, err := client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketVersioning() error = %v", err)
	}
	if verOut.Status != types.BucketVersioningStatusEnabled {
		t.Fatalf("real versioning status = %q, want Enabled", verOut.Status)
	}

	spec.Resources[0].Backup.Enabled = false
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	verOut, err = client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketVersioning() error = %v", err)
	}
	if verOut.Status != types.BucketVersioningStatusSuspended {
		t.Errorf("real versioning status after disabling backup = %q, want Suspended (AWS can never fully un-version a bucket)", verOut.Status)
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
	entry := status.FindManagedResource(ledger, "s3", "receipts")
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)

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
// own GetBucketTagging shape - this exact bug class was real and found live
// once already in sqs's own ownership check before IsStaleUID existed
// everywhere.
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
