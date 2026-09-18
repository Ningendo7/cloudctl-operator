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

package kms

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// kmsAPI is the subset of the KMS client this package needs. It's a type
// alias to the exported cloudctlaws.KMSClient rather than its own separate
// interface, since that interface now has two consumers — this package's
// own unit tests, and controller-level envtest suites that need to fake
// the whole AWS backend to exercise Reconcile() end-to-end. Aliasing keeps
// every reference to kmsAPI in this file unchanged (same pattern as
// sqsAPI/snsAPI/dynamodbAPI/s3API/iamAPI).
type kmsAPI = cloudctlaws.KMSClient

const resourceType = "kms"

// aliasMaxLen is AWS's own limit on a full alias name, including the
// mandatory "alias/" prefix this package always prepends.
const aliasMaxLen = 256

type keyOptions struct {
	deletionPolicy depsv1alpha1.DeletionPolicy
	adopt          bool
}

// aliasName is this CR's deterministic KMS alias for one key entry — the
// only name-based handle a KMS key has. Unlike every other resource type,
// CreateKey itself accepts no name at all; only CreateAlias, a required
// second call, does.
func aliasName(namespace, crName, resourceName string) string {
	return "alias/" + cloudctlaws.ResourceName(namespace, crName, resourceName)
}

// Ensure reconciles every declared KMS key against AWS, updating the
// ownership ledger as it goes.
func Ensure(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID string,
	spec *depsv1alpha1.KMSSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, k := range spec.Resources {
		opts := keyOptions{
			deletionPolicy: k.DeletionPolicy,
			adopt:          k.Adopt,
		}
		var err error
		ledger, err = ensureKey(ctx, client, namespace, crName, crUID, k.Name, opts, ledger)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("key %q: %w", k.Name, err)
		}
	}
	return ledger, firstErr
}

// dedicatedKeyLedgerName derives the ledger entry name for a dedicated key
// belonging to a resource in another section — resourceName + "-key",
// mirroring how SQS's own DLQ derives its ledger name (resourceName +
// "-dlq") from its owning queue. Kept as a named function (not just
// inlined at each call site) since both this package's own Cleanup and
// every calling section need to agree on the exact same derivation.
func dedicatedKeyLedgerName(resourceName string) string {
	return resourceName + "-key"
}

// EnsureDedicatedKey ensures a dedicated, operator-owned KMS key exists
// for a single resource belonging to another resource package (sqs, sns,
// s3, dynamodb) — for encryption.enabled:true, the common case that never
// requires touching a kms.resources section at all. Returns the key's
// ARN, for the caller to pass into its own create/attribute call.
//
// The ledger entry is recorded under dedicatedKeyLedgerName(resourceName)
// — distinct from any name a user's own kms.resources entry might use, so
// the two can never collide even if a section happens to share the same
// base resourceName as an explicit kms.resources entry. deletionPolicy
// should mirror the owning resource's own current policy (Retain if the
// resource itself is retained — its data still needs to stay decryptable
// — Delete otherwise), not be fixed at creation, so it's re-supplied and
// re-recorded on every call rather than only set once.
func EnsureDedicatedKey(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID, resourceName string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	ledger []depsv1alpha1.ManagedResource,
) (arn string, updatedLedger []depsv1alpha1.ManagedResource, err error) {
	ledgerName := dedicatedKeyLedgerName(resourceName)
	opts := keyOptions{deletionPolicy: deletionPolicy}
	updatedLedger, err = ensureKey(ctx, client, namespace, crName, crUID, ledgerName, opts, ledger)
	if err != nil {
		return "", updatedLedger, err
	}
	entry := status.FindManagedResource(updatedLedger, resourceType, ledgerName)
	return entry.ARN, updatedLedger, nil
}

// ResolveSharedKeyARN resolves an EncryptionSpec.KMSKeyRef to the ARN of
// the KMS key it points at — for encryption.kmsKeyRef, the deliberate-reuse
// half of the hybrid design (as opposed to EnsureDedicatedKey's
// per-resource key). Uses the exact same cross-CR authorization check
// every other consumes reference goes through: the producer's own
// kms.resources entry must list this CR in its sharedWith. Returns
// ("", false) if that isn't true yet — the caller should treat this as a
// normal, self-resolving-forward-reference state (the same reasoning
// collectGrants itself already applies to every other resource type's
// consumes), not a hard, permanent failure.
//
// Takes namespace/crName rather than the whole CR, unlike
// iam.ResolveConsumeARN's own signature, since none of sqs/sns/dynamodb/s3
// carry the full CR object through their Ensure calls the way iam and
// configmap do — only what identifies the consumer for the sharedWith
// match is actually needed.
func ResolveSharedKeyARN(ctx context.Context, k8sClient client.Client, namespace, crName string, ref depsv1alpha1.ConsumeRef) (arn string, ok bool) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName}}
	return iam.ResolveConsumeARN(ctx, k8sClient, consumer, resourceType, ref)
}

// ensureKey is a thin orchestrator: the ledger, not an alias lookup, is
// this package's primary source of truth for "do we already have a key
// for this resource." Unlike every other resource type, name-based lookup
// here can lag real creation — CreateKey accepts no name at all, so a key
// can exist (and already be durably ours via atomic Tags) before its
// alias, the only thing that makes it findable by name, exists. Trusting
// a not-yet-aliased lookup miss as "never created" would risk creating a
// second, orphaned key underneath the first.
func ensureKey(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID, resourceName string,
	opts keyOptions,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	alias := aliasName(namespace, crName, resourceName)
	if err := cloudctlaws.ValidateNameLength(alias, aliasMaxLen, "KMS alias"); err != nil {
		return ledger, err
	}

	if entry := status.FindManagedResource(ledger, resourceType, resourceName); entry != nil {
		return resumeKey(ctx, client, namespace, crName, crUID, alias, opts, *entry, ledger)
	}

	descirbeOut, err := client.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: &alias,
	})
	var notFound *types.NotFoundException
	if errors.As(err, &notFound) {
		return createKey(ctx, client, namespace, crName, crUID, alias, resourceName, opts, ledger)
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up KMS key alias")
	}

	return adoptKey(ctx, client, namespace, crName, crUID, alias, resourceName, opts, *descirbeOut.KeyMetadata, ledger)
}

// createKey makes a brand new key: CreateKey (Tags set atomically; Policy
// deliberately omitted entirely — that gets AWS's own minimal default
// policy, which already includes the one statement that makes
// IAM-policy-based grants effective at all, so there's no key-policy JSON
// for this package to get wrong), record the ledger entry immediately
// after (before the alias exists, so a crash here is recoverable from the
// ledger alone — same reasoning as S3's TagPending, just for a different
// non-atomic second step), enable rotation, then create the alias.
func createKey(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID, alias, resourceName string,
	opts keyOptions,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Tags: mapToTags(ownerTags(namespace, crName, crUID)),
	})
	if err != nil {
		return ledger, wrapAWSError(err, "creating KMS key")
	}
	arn := *createOut.KeyMetadata.Arn
	keyID := *createOut.KeyMetadata.KeyId

	ledger = recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateTagPending)

	// Automatic rotation is a fire-and-forget, default-on decision — set
	// once at creation, never exposed as spec config, never re-verified on
	// later reconciles (AWS doesn't silently turn it off on its own).
	if _, err := client.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{
		KeyId: &keyID,
	}); err != nil {
		return ledger, wrapAWSError(err, "enabling automatic key rotation")
	}

	if err := ensureAlias(ctx, client, alias, arn, keyID); err != nil {
		return ledger, err
	}

	return recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateVerified), nil
}

// resumeKey handles a resourceName this CR's own ledger already has an
// entry for, regardless of that entry's state — re-verifying ownership,
// finishing alias creation if that's what didn't complete last time, and
// reviving the key out of a scheduled deletion if Cleanup's own quiet
// window scheduled one and the resource has since reappeared in spec.
func resumeKey(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID, alias string,
	opts keyOptions,
	entry depsv1alpha1.ManagedResource,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	describeOut, err := client.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: &entry.ARN,
	})
	var notFound *types.NotFoundException
	if errors.As(err, &notFound) {
		// Our own ledger's ARN doesn't resolve anymore — the key is
		// genuinely gone (fully deleted, not just scheduled: a scheduled
		// key still resolves, with KeyState PendingDeletion). Everything
		// ever encrypted under it is already permanently unrecoverable;
		// silently creating a same-named replacement would paper over
		// that, not fix it. Surface it loudly instead.
		return ledger, fmt.Errorf(
			"KMS key %q (%s) no longer exists in AWS — it was deleted out-of-band, and everything encrypted under it is unrecoverable; remove and re-add this entry to provision a new key once you've confirmed that's acceptable",
			entry.Name, entry.ARN,
		)
	}
	if err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("looking up KMS key %q", entry.Name))
	}
	keyID := *describeOut.KeyMetadata.KeyId

	tags, tErr := listAllResourceTags(ctx, client, entry.ARN)
	if tErr != nil {
		return ledger, wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of KMS key %q", entry.Name))
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, crUID) {
		return ledger, fmt.Errorf("KMS key %q is no longer tagged as owned by this CR - refusing to manage it further", entry.Name)
	}

	if describeOut.KeyMetadata.KeyState == types.KeyStatePendingDeletion {
		if _, err := client.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{
			KeyId: &entry.ARN,
		}); err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("canceling scheduled deletion of KMS key %q now that it's declared again", entry.Name))
		}
	}

	if err := ensureAlias(ctx, client, alias, entry.ARN, keyID); err != nil {
		return ledger, err
	}

	return recordKey(ledger, entry.Name, entry.ARN, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateVerified), nil
}

// adoptKey handles finding a real, alias-named key this CR's own ledger
// has no record of at all — either a foreign key that happens to sit at
// this CR's deterministic alias, or one this CR owned in a previous
// lifetime (e.g. the CR was deleted and recreated, with the key retained).
func adoptKey(
	ctx context.Context,
	client kmsAPI,
	namespace, crName, crUID, alias, resourceName string,
	opts keyOptions,
	meta types.KeyMetadata,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	arn := *meta.Arn
	tags, tErr := listAllResourceTags(ctx, client, arn)
	if tErr != nil {
		return ledger, wrapAWSError(tErr, fmt.Sprintf("reading tags on existing KMS key alias %q", alias))
	}
	tagMap := tagsToMap(tags)

	if cloudctlaws.IsOwnedBy(tagMap, namespace, crName, crUID) {
		return recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateVerified), nil
	}
	if existingOwner, ok := tagMap[cloudctlaws.OwnerTagKey]; ok && existingOwner != cloudctlaws.OwnerTagValue(namespace, crName) {
		return ledger, fmt.Errorf("KMS alias %q is already owned by a different AppDependencies CR (%s) — this looks like a naming collision, not adopting", alias, existingOwner)
	}
	if !opts.adopt {
		return ledger, fmt.Errorf("KMS alias %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", alias)
	}

	merged := cloudctlaws.MergeTags(tagMap, ownerTags(namespace, crName, crUID))
	if _, err := client.TagResource(ctx, &kms.TagResourceInput{
		KeyId: &arn,
		Tags:  mapToTags(merged),
	}); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("adopting KMS key %q (tagging)", alias))
	}

	if meta.KeyState == types.KeyStatePendingDeletion {
		if _, err := client.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{
			KeyId: &arn,
		}); err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("canceling scheduled deletion of adopted KMS key %q", alias))
		}
	}

	return recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateVerified), nil
}

// ensureAlias makes alias point at keyID/arn, tolerating the alias already
// existing as long as it already points at this exact key — the expected,
// idempotent outcome on a retried reconcile — and failing loudly if it
// points at a different key entirely (a genuine naming collision). AWS
// itself always errors on a pre-existing alias name regardless of target,
// so this tolerance has to be built here, not assumed from the API.
func ensureAlias(ctx context.Context, client kmsAPI, alias, arn, keyID string) error {
	_, err := client.CreateAlias(ctx, &kms.CreateAliasInput{
		AliasName:   &alias,
		TargetKeyId: &keyID,
	})
	if err == nil {
		return nil
	}
	var alreadyExists *types.AlreadyExistsException
	if !errors.As(err, &alreadyExists) {
		return wrapAWSError(err, fmt.Sprintf("creating KMS key alias %q", alias))
	}

	describeOut, dErr := client.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: &alias,
	})
	if dErr != nil {
		return wrapAWSError(dErr, fmt.Sprintf("verifying existing KMS key alias %q", alias))
	}
	if *describeOut.KeyMetadata.Arn != arn {
		return fmt.Errorf("KMS alias %q already points at a different key (%s) than this CR's own (%s) — naming collision, not correcting", alias, *describeOut.KeyMetadata.Arn, arn)
	}
	return nil
}

func recordKey(
	ledger []depsv1alpha1.ManagedResource,
	resourceName, arn string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	state depsv1alpha1.ManagedResourceState,
) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	if existing := status.FindManagedResource(ledger, resourceType, resourceName); existing != nil {
		createdAt = existing.CreatedAt
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           resourceName,
		ARN:            arn,
		DeletionPolicy: deletionPolicy,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
		State:          state,
	})
	return ledger
}

func ownerTags(namespace, crName, crUID string) map[string]string {
	return map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}
}

// mapToTags/tagsToMap use KMS's own Tag shape (TagKey/TagValue) — unlike
// IAM/DynamoDB/S3, which all use Key/Value.
func mapToTags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{TagKey: &k, TagValue: &v})
	}
	return tags
}

func tagsToMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.TagKey != nil && t.TagValue != nil {
			m[*t.TagKey] = *t.TagValue
		}
	}
	return m
}

func listAllResourceTags(ctx context.Context, client kmsAPI, keyID string) ([]types.Tag, error) {
	var all []types.Tag
	var marker *string
	for {
		out, err := client.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: &keyID, Marker: marker})
		if err != nil {
			return nil, err
		}
		all = append(all, out.Tags...)
		if !out.Truncated {
			return all, nil
		}
		marker = out.NextMarker
	}
}

func wrapAWSError(err error, context string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", context, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
