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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// s3API is the subset of the S3 client this package needs. Stays private
// here — like sqsAPI/snsAPI/dynamodbAPI originally did — until a second
// real consumer (controller-level envtest fakes) needs to substitute a
// fake here too.
type s3API interface {
	HeadBucket(ctx context.Context, in *s3sdk.HeadBucketInput, optFns ...func(*s3sdk.Options)) (*s3sdk.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, in *s3sdk.CreateBucketInput, optFns ...func(*s3sdk.Options)) (*s3sdk.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, in *s3sdk.DeleteBucketInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketOutput, error)
	GetBucketTagging(ctx context.Context, in *s3sdk.GetBucketTaggingInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketTaggingOutput, error)
	PutBucketTagging(ctx context.Context, in *s3sdk.PutBucketTaggingInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketTaggingOutput, error)
	PutBucketVersioning(ctx context.Context, in *s3sdk.PutBucketVersioningInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketVersioningOutput, error)
	GetBucketVersioning(ctx context.Context, in *s3sdk.GetBucketVersioningInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketVersioningOutput, error)
	PutBucketLifecycleConfiguration(ctx context.Context, in *s3sdk.PutBucketLifecycleConfigurationInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketLifecycleConfigurationOutput, error)
	DeleteBucketLifecycle(ctx context.Context, in *s3sdk.DeleteBucketLifecycleInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketLifecycleOutput, error)
	ListObjectVersions(ctx context.Context, in *s3sdk.ListObjectVersionsInput, optFns ...func(*s3sdk.Options)) (*s3sdk.ListObjectVersionsOutput, error)
	DeleteObjects(ctx context.Context, in *s3sdk.DeleteObjectsInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectsOutput, error)
	ListMultipartUploads(ctx context.Context, in *s3sdk.ListMultipartUploadsInput, optFns ...func(*s3sdk.Options)) (*s3sdk.ListMultipartUploadsOutput, error)
	AbortMultipartUpload(ctx context.Context, in *s3sdk.AbortMultipartUploadInput, optFns ...func(*s3sdk.Options)) (*s3sdk.AbortMultipartUploadOutput, error)
}

const resourceType = "s3"

// tagRetryClaimWindow bounds how long a TagPending ledger entry is trusted
// as proof we created this bucket ourselves, for retrying the tag write
// after a transient failure. S3 is the one AWS service among the ones this
// operator manages where creation and tagging aren't atomic — CreateBucket
// doesn't accept a Tags parameter, unlike SQS/SNS/DynamoDB's CreateX calls.
// Bounded so this can only ever recover from a transient tag-write failure
// shortly after creation, never stand in as a permanent claim on the name —
// a bucket that was deleted and its name later reused by something else
// entirely (S3 names are globally released back to AWS on deletion) would
// otherwise be silently reclaimed. Matches the value a sibling project's
// own S3 controller settled on for the identical problem.
const tagRetryClaimWindow = time.Hour

type bucketOptions struct {
	deletionPolicy       depsv1alpha1.DeletionPolicy
	force, adopt         bool
	backupEnabled        bool
	replicationRequested bool
	versioningOverride   *bool
	lifecycleOverride    []depsv1alpha1.S3LifecycleRule
	lifecycleOverrideSet bool
}

// bucketName derives the actual AWS bucket name. Unlike SQS/SNS/DynamoDB,
// S3 bucket names are unique across every AWS account globally, not just
// this one, so the deterministic namespace-crName-key scheme alone has a
// real chance of colliding with an unrelated AWS customer's bucket. A short
// hash of this account's own ID (already globally unique to us) is always
// appended — not just on overflow — to stay effectively unique to this
// account while remaining fully deterministic (no persisted random state
// needed to recompute it, which the ownership/ledger model depends on).
func bucketName(namespace, crName, resourceKey, accountID string) string {
	base := cloudctlaws.ResourceName(namespace, crName, resourceKey)
	sum := sha256.Sum256([]byte(accountID))
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(sum[:])[:8])
}

// Ensure reconciles every declared S3 bucket against AWS, updating the
// ownership ledger as it goes.
func Ensure(
	ctx context.Context,
	client s3API,
	namespace, crName, crUID, region, accountID string,
	spec *depsv1alpha1.S3Spec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, b := range spec.Resources {
		opts := bucketOptions{
			deletionPolicy: b.DeletionPolicy,
			force:          b.Force,
			adopt:          b.Adopt,
		}
		if b.Backup != nil {
			opts.backupEnabled = b.Backup.Enabled
		}
		if b.Replication != nil {
			opts.replicationRequested = b.Replication.Enabled
		}
		if b.Overrides != nil {
			opts.versioningOverride = b.Overrides.VersioningEnabled
			if b.Overrides.LifecycleRules != nil {
				opts.lifecycleOverride = b.Overrides.LifecycleRules
				opts.lifecycleOverrideSet = true
			}
		}

		var err error
		ledger, err = ensureBucket(ctx, client, namespace, crName, crUID, region, accountID, b.Name, opts, ledger)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("bucket %q: %w", b.Name, err)
		}
	}
	return ledger, firstErr
}

// ErrReplicationNotSupported is returned for any bucket requesting
// replication. Cross-region replication needs a bucket and IAM role in a
// different region — this operator's Clients holds exactly one region for
// the whole controller process, the same single-region assumption that
// currently blocks DynamoDB Global Tables. Left unimplemented rather than
// half-built until there's a real design for multi-region client support.
var ErrReplicationNotSupported = errors.New("replication is not implemented yet - it needs cross-region client support this operator doesn't have")

func ensureBucket(
	ctx context.Context,
	client s3API,
	namespace, crName, crUID, region, accountID, resourceName string,
	opts bucketOptions,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if opts.replicationRequested {
		return ledger, ErrReplicationNotSupported
	}

	bucket := bucketName(namespace, crName, resourceName, accountID)
	if err := cloudctlaws.ValidateNameLength(bucket, 63, "S3 bucket"); err != nil {
		return ledger, err
	}
	bucketArn := "arn:aws:s3:::" + bucket

	_, err := client.HeadBucket(ctx, &s3sdk.HeadBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		if !isNotFoundError(err) {
			return ledger, wrapAWSError(err, "checking bucket existence")
		}

		input := &s3sdk.CreateBucketInput{
			Bucket: &bucket,
		}
		if region != "us-east-1" {
			// us-east-1 is the one region where CreateBucketConfiguration
			// must NOT be set at all - specifying it (even naming
			// us-east-1 explicitly) is rejected.
			input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
				LocationConstraint: types.BucketLocationConstraint(region),
			}
		}
		if _, cErr := client.CreateBucket(ctx, input); cErr != nil {
			return ledger, wrapAWSError(cErr, "creating bucket")
		}
		// Tagging isn't atomic with creation here (unlike SQS/SNS/DynamoDB) -
		// record this durably before attempting to tag, so a transient
		// failure in that next step is recoverable (retry against a bucket
		// the ledger already proves is ours) rather than ambiguous.
		ledger = recordTagPending(ledger, resourceName, bucketArn, opts.deletionPolicy, opts.force)
	}

	// Whether just created above or already existing, both paths converge
	// here: read current tags, and either confirm ownership or decide
	// whether claiming it is justified (adopt:true for a foreign bucket,
	// or our own recent TagPending record for one we just created).
	tagsOut, tErr := client.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: &bucket})
	currentTags := map[string]string{}
	if tErr != nil {
		if !isNoSuchTagSet(tErr) {
			return ledger, wrapAWSError(tErr, "reading bucket tags")
		}
		// NoSuchTagSet: bucket exists but carries no tags at all - nothing
		// to preserve, not an error.
	} else {
		currentTags = tagsToMap(tagsOut.TagSet)
	}

	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		if existingOwner, ok := currentTags[cloudctlaws.OwnerTagKey]; ok && existingOwner != cloudctlaws.OwnerTagValue(namespace, crName) {
			return ledger, fmt.Errorf("bucket %q is already owned by a different AppDependencies CR (%s) — this looks like a naming collision, not adopting", bucket, existingOwner)
		}
		if !opts.adopt && !recentlyCreatedByUs(ledger, resourceName) {
			return ledger, fmt.Errorf("bucket %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", bucket)
		}

		merged := cloudctlaws.MergeTags(currentTags, ownerTags(namespace, crName, crUID))
		if _, tagErr := client.PutBucketTagging(ctx, &s3sdk.PutBucketTaggingInput{
			Bucket:  &bucket,
			Tagging: &types.Tagging{TagSet: mapToTags(merged)},
		}); tagErr != nil {
			return ledger, wrapAWSError(tagErr, "tagging bucket")
		}
	}

	if err := reconcileBucketAttributes(ctx, client, bucket, opts); err != nil {
		return ledger, wrapAWSError(err, "reconciling bucket attributes")
	}

	return recordVerified(ledger, resourceName, bucketArn, opts.deletionPolicy, opts.force), nil
}

// reconcileBucketAttributes corrects drift on versioning and the lifecycle
// policy. Lifecycle is written unconditionally when non-empty rather than
// diffed against the current configuration first - PutBucketLifecycleConfiguration
// is itself fully idempotent, and a deep structural comparison of nested
// transition/expiration rules isn't worth the complexity it would add for
// avoiding one harmless redundant call per reconcile.
func reconcileBucketAttributes(ctx context.Context, client s3API, bucket string, opts bucketOptions) error {
	desiredVersioning := opts.backupEnabled
	if opts.versioningOverride != nil {
		desiredVersioning = *opts.versioningOverride
	}

	current, err := client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{
		Bucket: &bucket,
	})
	if err != nil {
		return fmt.Errorf("reading versioning status: %w", err)
	}
	if (current.Status == types.BucketVersioningStatusEnabled) != desiredVersioning {
		status := types.BucketVersioningStatusSuspended
		if desiredVersioning {
			status = types.BucketVersioningStatusEnabled
		}
		if _, err := client.PutBucketVersioning(ctx, &s3sdk.PutBucketVersioningInput{
			Bucket:                  &bucket,
			VersioningConfiguration: &types.VersioningConfiguration{Status: status},
		}); err != nil {
			return fmt.Errorf("correcting versioning: %w", err)
		}
	}

	rules := desiredLifecycleRules(opts)
	if len(rules) == 0 {
		_, err := client.DeleteBucketLifecycle(ctx, &s3sdk.DeleteBucketLifecycleInput{Bucket: &bucket})
		if err != nil && !isNotFoundError(err) {
			return fmt.Errorf("removing lifecycle policy: %w", err)
		}
		return nil
	}

	_, err = client.PutBucketLifecycleConfiguration(ctx, &s3sdk.PutBucketLifecycleConfigurationInput{
		Bucket: &bucket,
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: buildLifecycleRules(rules),
		},
	})
	if err != nil {
		return fmt.Errorf("setting lifecycle policy: %w", err)
	}
	return nil
}

// desiredLifecycleRules resolves which rules to apply this reconcile: an
// explicit override (even an empty list, meaning "no lifecycle policy at
// all" - see S3Overrides.LifecycleRules' own doc comment for why this only
// works because that field has no omitempty), otherwise the default when
// backup is enabled, otherwise none.
func desiredLifecycleRules(opts bucketOptions) []depsv1alpha1.S3LifecycleRule {
	if opts.lifecycleOverrideSet {
		return opts.lifecycleOverride
	}
	if opts.backupEnabled {
		return defaultLifecycleRules()
	}
	return nil
}

const defaultLifecycleRuleID = "cloudctl-backup-default"

// defaultLifecycleRules is deliberately minimal: it only cleans up
// superseded (noncurrent) object versions after 30 days, bounding the
// storage growth versioning otherwise causes unboundedly. It never expires
// or transitions the *current* object - backup is meant to protect live
// data, not put a deletion timer on it. Storage-class transitions and
// multipart-upload cleanup are left to spec.overrides.lifecycleRules for
// anyone who wants them; bundling them into the default would be an
// opinion about cost optimization, not the safety property backup exists
// for.
func defaultLifecycleRules() []depsv1alpha1.S3LifecycleRule {
	days := int32(30)
	return []depsv1alpha1.S3LifecycleRule{
		{ID: defaultLifecycleRuleID, NoncurrentVersionExpirationAfterDays: &days},
	}
}

func buildLifecycleRules(rules []depsv1alpha1.S3LifecycleRule) []types.LifecycleRule {
	out := make([]types.LifecycleRule, 0, len(rules))
	for _, r := range rules {
		id := r.ID
		sdkRule := types.LifecycleRule{
			ID:     &id,
			Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: strPtr("")},
		}
		if r.TransitionAfterDays != nil {
			sdkRule.Transitions = []types.Transition{{
				Days:         r.TransitionAfterDays,
				StorageClass: types.TransitionStorageClass(r.TransitionStorageClass),
			}}
		}
		if r.ExpirationAfterDays != nil {
			sdkRule.Expiration = &types.LifecycleExpiration{Days: r.ExpirationAfterDays}
		}
		if r.NoncurrentVersionExpirationAfterDays != nil {
			sdkRule.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{
				NoncurrentDays: r.NoncurrentVersionExpirationAfterDays,
			}
		}
		out = append(out, sdkRule)
	}
	return out
}

func recordTagPending(ledger []depsv1alpha1.ManagedResource, ledgerName, arn string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           ledgerName,
		ARN:            arn,
		State:          depsv1alpha1.ManagedResourceStateTagPending,
		DeletionPolicy: deletionPolicy,
		Force:          force,
		CreatedAt:      now,
	})
	return ledger
}

func recordVerified(ledger []depsv1alpha1.ManagedResource, ledgerName, arn string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	if existing := status.FindManagedResource(ledger, resourceType, ledgerName); existing != nil {
		createdAt = existing.CreatedAt
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           ledgerName,
		ARN:            arn,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		DeletionPolicy: deletionPolicy,
		Force:          force,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
	})
	return ledger
}

// recentlyCreatedByUs reports whether the ledger durably records this
// operator creating this exact bucket recently enough (within
// tagRetryClaimWindow) that a still-untagged bucket found under its name
// should be trusted as "ours, the tag write just hasn't succeeded yet"
// rather than treated as a foreign resource requiring adopt:true.
func recentlyCreatedByUs(ledger []depsv1alpha1.ManagedResource, ledgerName string) bool {
	existing := status.FindManagedResource(ledger, resourceType, ledgerName)
	return existing != nil &&
		existing.State == depsv1alpha1.ManagedResourceStateTagPending &&
		time.Since(existing.CreatedAt.Time) < tagRetryClaimWindow
}

// isNotFoundError checks for a missing-bucket error across its several AWS
// SDK forms. HeadBucket's own docs confirm it returns only generic HTTP
// status codes (400/403/404) with no message body distinguishing the
// cause, so a typed exception isn't always what comes back - both forms
// have to be handled.
func isNotFoundError(err error) bool {
	// HeadBucket returns NotFound; GetBucketTagging, DeleteBucket,
	// ListObjectVersions, ListMultipartUploads, and DeleteBucketLifecycle
	// all return NoSuchBucket instead for the identical condition (a
	// missing bucket); AbortMultipartUpload returns NoSuchUpload when the
	// upload it's targeting is already gone (aborted or completed by a
	// concurrent retry). All three are genuinely distinct typed exceptions
	// with different ErrorCode() values, not different names for the same
	// thing, so all three have to be checked to correctly recognize
	// "already gone" across every call site that uses this.
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchBucket *types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return true
	}
	var noSuchUpload *types.NoSuchUpload
	if errors.As(err, &noSuchUpload) {
		return true
	}
	var responseErr *awshttp.ResponseError
	if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == 404 {
		return true
	}
	return false
}

// isNoSuchTagSet reports whether err is GetBucketTagging's "bucket exists
// but has no tags at all" response - distinct from the bucket not existing
// at all, which is a different error code entirely.
func isNoSuchTagSet(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchTagSet"
}

func ownerTags(namespace, crName, crUID string) map[string]string {
	return map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}
}

func mapToTags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{Key: &k, Value: &v})
	}
	return tags
}

func tagsToMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func strPtr(s string) *string { return &s }

func wrapAWSError(err error, context string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", context, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
