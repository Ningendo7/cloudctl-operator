//go:build integration

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

// Integration tests run against a real AWS SDK client pointed at a
// LocalStack container instead of this package's own hand-written fakes —
// see sqs_integration_test.go's package doc for the full rationale.
// S3 in particular is where our own assumptions have already been the
// shakiest (isServerSideEncryptionConfigurationNotFoundError and
// isNoSuchTagSet are both string-code checks with no typed SDK exception
// to verify against), which is exactly the gap this tier exists to catch.
//
// One real gap this tier can't close: Ensure's own CreateBucket call sets
// tags atomically via CreateBucketConfiguration.Tags, which this pinned
// LocalStack version can't parse (it rejects the request with a real
// MalformedXML error) — a LocalStack bug, not a bug in our code; confirmed
// working correctly against real AWS by the live tier instead (see
// docs/testing.md). Rather than drop S3 from this tier entirely the way
// KMS/IAM already are, tests below that don't specifically need to exercise
// that one call pre-create and pre-tag their bucket directly via
// createTaggedBucket, exercising every other real code path (tag/ownership
// checks, drift correction, cleanup) against LocalStack as normal.
package s3

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const (
	integrationRegion    = "us-east-1"
	integrationAccountID = "000000000000" // LocalStack's fixed default account ID.
)

// newIntegrationClient builds a real S3 client pointed at LocalStack.
// Deliberately never uses config.LoadDefaultConfig or picks up the
// environment's own AWS credentials/profile — an integration test must be
// structurally incapable of ever reaching real AWS by accident.
// UsePathStyle is required for LocalStack: virtual-hosted-style bucket
// URLs (bucket.s3.amazonaws.com) don't resolve to localhost.
func newIntegrationClient(t *testing.T) *s3sdk.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return s3sdk.New(s3sdk.Options{
		Region:       integrationRegion,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
	})
}

// createTaggedBucket creates and tags a bucket directly via the real SDK,
// bypassing Ensure's own atomic CreateBucket+Tags call entirely — see this
// file's package doc for why.
func createTaggedBucket(t *testing.T, client *s3sdk.Client, namespace, crName, crUID, resourceName string) string {
	t.Helper()
	ctx := context.Background()
	bucket := bucketName(namespace, crName, resourceName, integrationAccountID)
	if _, err := client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("real CreateBucket() (setup) error = %v", err)
	}
	if _, err := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
		Bucket:  &bucket,
		Tagging: &types.Tagging{TagSet: mapToTags(ownerTags(namespace, crName, crUID))},
	}); err != nil {
		t.Fatalf("real PutBucketTagging() (setup) error = %v", err)
	}
	return bucket
}

// TestIntegration_Ensure_ReconcilesVersioningLifecycleAndTagsOnExistingBucket
// exercises the same ownership-check-then-reconcile path Ensure takes for a
// bucket that already exists (regardless of who created it) — the ARN
// construction, tag/ownership verification, and versioning/lifecycle
// reconciliation are all identical whichever branch got here.
func TestIntegration_Ensure_ReconcilesVersioningLifecycleAndTagsOnExistingBucket(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", "bucket-create"
	bucket := createTaggedBucket(t, client, namespace, crName, "uid-1", "receipts")
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true,
			Backup: &depsv1alpha1.S3BackupSpec{Enabled: true}},
	}}

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := status.FindManagedResource(ledger, resourceType, "receipts")
	if entry == nil {
		t.Fatal("expected a ledger entry for receipts")
	}

	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("real HeadBucket() error = %v — bucket wasn't actually created against LocalStack", err)
	}

	verOut, err := client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketVersioning() error = %v", err)
	}
	if verOut.Status != types.BucketVersioningStatusEnabled {
		t.Errorf("real versioning status = %q, want Enabled — backup.enabled should turn versioning on", verOut.Status)
	}

	lifecycleOut, err := client.GetBucketLifecycleConfiguration(ctx, &s3sdk.GetBucketLifecycleConfigurationInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketLifecycleConfiguration() error = %v — expected the default backup lifecycle policy to exist", err)
	}
	if len(lifecycleOut.Rules) != 1 || aws.ToString(lifecycleOut.Rules[0].ID) != defaultLifecycleRuleID {
		t.Errorf("unexpected real lifecycle rules: %+v", lifecycleOut.Rules)
	}

	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketTagging() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.TagSet), namespace, crName, "uid-1") {
		t.Errorf("real bucket tags don't reflect ownership: %+v", tagsOut.TagSet)
	}
}

func TestIntegration_Ensure_IsIdempotentAgainstRealAWS(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", "bucket-idempotent"
	bucket := createTaggedBucket(t, client, namespace, crName, "uid-1", "receipts")
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected exactly one ledger entry after two reconciles against real AWS, got %d", len(ledger))
	}
}

func TestIntegration_Ensure_AdoptsRealUntaggedBucket(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", "bucket-adopt"
	bucket := bucketName(namespace, crName, "receipts", integrationAccountID)
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	// Create the bucket directly via the real SDK, with no ownership tags
	// at all — simulating a resource that already existed before this CR
	// ever reconciled.
	if _, err := client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("setting up pre-existing real bucket: %v", err)
	}

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	tagsOut, err := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketTagging() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.TagSet), namespace, crName, "uid-1") {
		t.Errorf("expected adopt:true to tag the pre-existing real bucket as owned, got tags %+v", tagsOut.TagSet)
	}
}

func TestIntegration_Ensure_CorrectsVersioningDriftOnRealBucket(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", "bucket-drift"
	bucket := createTaggedBucket(t, client, namespace, crName, "uid-1", "receipts")
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	enabled := true
	spec.Resources[0].Overrides = &depsv1alpha1.S3Overrides{VersioningEnabled: &enabled}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, ledger, nil, nil); err != nil {
		t.Fatalf("drift-correcting Ensure() error = %v", err)
	}

	verOut, err := client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketVersioning() error = %v", err)
	}
	if verOut.Status != types.BucketVersioningStatusEnabled {
		t.Errorf("real versioning status after drift correction = %q, want Enabled", verOut.Status)
	}
}

func TestIntegration_Cleanup_DeletesRealBucketImmediatelyWhenForced(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", "bucket-cleanup"
	bucket := createTaggedBucket(t, client, namespace, crName, "uid-1", "receipts")
	t.Cleanup(func() { deleteBucketIfExists(t, client, bucket) })

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", spec, ledger, true, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger entry to be removed, got %+v", ledger)
	}

	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err == nil {
		t.Error("expected the real bucket to be gone after Cleanup, but HeadBucket succeeded")
	}
}

// TestIntegration_Cleanup_WritesDenyPolicyWhilePendingDeletion stops short
// of asserting the deny is actually enforced - LocalStack's community
// edition doesn't evaluate bucket/IAM policies at all unless ENFORCE_IAM=1
// is set (which this project doesn't, to avoid risking unrelated failures
// elsewhere this suite doesn't expect strict enforcement for), so a real
// PutObject against a denied bucket would silently succeed here regardless
// of whether the policy itself is correct. Real enforcement of this exact
// policy is confirmed instead by sqs/sns's own live-AWS deny tests, which
// use the identical pattern. What LocalStack CAN confirm: GetBucketPolicy
// round-trips real policy JSON with the exact Sid/Action/Resource this
// package wrote.
func TestIntegration_Cleanup_WritesDenyPolicyWhilePendingDeletion(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", "bucket-pending"
	bucket := createTaggedBucket(t, client, namespace, crName, "uid-1", "receipts")
	t.Cleanup(func() {
		_, _ = client.DeleteBucketPolicy(ctx, &s3sdk.DeleteBucketPolicyInput{Bucket: &bucket})
		deleteBucketIfExists(t, client, bucket)
	})

	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		// Not force, and Retain would never enter the pending-deletion path
		// at all — Delete without force is required to exercise it.
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: false},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	// Removing it from spec (empty S3Spec) makes it eligible for deletion;
	// the first Cleanup pass should hold it for the quiet window rather
	// than deleting outright, and write the deny policy in the meantime.
	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.S3Spec{}, ledger, false, nil); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("expected the bucket to still exist during the quiet window, HeadBucket error = %v", err)
	}

	policyOut, err := client.GetBucketPolicy(ctx, &s3sdk.GetBucketPolicyInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("real GetBucketPolicy() error = %v — expected a deny policy to have been written", err)
	}
	policy := aws.ToString(policyOut.Policy)
	if !strings.Contains(policy, pendingDeletionDenySid) || !strings.Contains(policy, "s3:PutObject") || !strings.Contains(policy, "\"Deny\"") {
		t.Errorf("real bucket policy doesn't contain the expected deny statement: %s", policy)
	}
}

func deleteBucketIfExists(t *testing.T, client *s3sdk.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	if _, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: &bucket}); err != nil {
		return
	}
	listOut, err := client.ListObjectVersions(ctx, &s3sdk.ListObjectVersionsInput{Bucket: &bucket})
	if err == nil {
		var toDelete []types.ObjectIdentifier
		for _, v := range listOut.Versions {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range listOut.DeleteMarkers {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(toDelete) > 0 {
			_, _ = client.DeleteObjects(ctx, &s3sdk.DeleteObjectsInput{Bucket: &bucket, Delete: &types.Delete{Objects: toDelete}})
		}
	}
	_, _ = client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: &bucket})
}
