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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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
	// aliasParts, when non-empty, marks this key as one derived from
	// another resource package's entry (see EnsureDedicatedKey) — the
	// alias is built from these parts via DerivedResourceName instead of
	// treating resourceName (which for a derived key is the '#'-joined
	// ledger key, illegal in a real AWS alias) as the AWS-facing name.
	aliasParts []string
}

// aliasPrefix is the mandatory literal every KMS alias name starts with.
const aliasPrefix = "alias/"

// aliasName is this CR's deterministic KMS alias for one key entry — the
// only name-based handle a KMS key has. Unlike every other resource type,
// CreateKey itself accepts no name at all; only CreateAlias, a required
// second call, does.
func aliasName(namespace, crName, resourceName string, opts keyOptions) string {
	if len(opts.aliasParts) > 0 {
		return aliasPrefix + cloudctlaws.DerivedResourceNameFor(cloudctlaws.NameStyleWordChars, namespace, crName, resourceType, aliasMaxLen-len(aliasPrefix), opts.aliasParts...)
	}
	return aliasPrefix + cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleWordChars, namespace, crName, resourceType, resourceName, aliasMaxLen-len(aliasPrefix))
}

// Ensure reconciles every declared KMS key against AWS, updating the
// ownership ledger as it goes.
func Ensure(
	ctx context.Context,
	kmsClient kmsAPI,
	namespace, crName, crUID string,
	spec *depsv1alpha1.KMSSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
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
		ledger, err = ensureKey(ctx, kmsClient, namespace, crName, crUID, k.Name, opts, ledger, checkpoint, recordEvent)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("key %q: %w", k.Name, err)
		}
	}
	return ledger, firstErr
}

// DedicatedKeyLedgerName re-exports cloudctlaws.DedicatedKeyLedgerName for
// this package's own callers.
func DedicatedKeyLedgerName(ownerType, resourceName string) string {
	return cloudctlaws.DedicatedKeyLedgerName(ownerType, resourceName)
}

// EnsureDedicatedKey ensures a dedicated, operator-owned KMS key exists
// for a single resource belonging to another resource package (sqs, sns,
// s3, dynamodb) — for encryption.enabled:true, the common case that never
// requires touching a kms.resources section at all. Returns the key's
// ARN, for the caller to pass into its own create/attribute call.
// deletionPolicy should mirror the owning resource's own current policy
// (Retain if the resource itself is retained — its data still needs to
// stay decryptable — Delete otherwise), not be fixed at creation, so it's
// re-supplied and re-recorded on every call rather than only set once.
func EnsureDedicatedKey(
	ctx context.Context,
	kmsClient kmsAPI,
	namespace, crName, crUID, ownerType, resourceName string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) (arn string, updatedLedger []depsv1alpha1.ManagedResource, err error) {
	ledgerName := DedicatedKeyLedgerName(ownerType, resourceName)
	opts := keyOptions{
		deletionPolicy: deletionPolicy,
		aliasParts:     []string{ownerType, resourceName, cloudctlaws.DedicatedKeyRole},
	}
	updatedLedger, err = ensureKey(ctx, kmsClient, namespace, crName, crUID, ledgerName, opts, ledger, checkpoint, recordEvent)
	if err != nil {
		return "", updatedLedger, err
	}
	entry, found := status.FindManagedResource(updatedLedger, resourceType, ledgerName)
	if !found {
		return "", updatedLedger, fmt.Errorf("KMS key %q is missing from the ledger right after ensuring it", ledgerName)
	}
	return entry.ARN, updatedLedger, nil
}

// SharedKeyARN returns the ARN of the key declared as name under this CR's
// own kms.resources, once it exists in the ledger. A key that isn't there
// yet is a normal, self-resolving state: kms is reconciled before the
// resources that reference it, so this only misses on a first pass or after
// a failed create.
func SharedKeyARN(ledger []depsv1alpha1.ManagedResource, name string) (arn string, ok bool) {
	entry, found := status.FindManagedResource(ledger, resourceType, name)
	if !found || entry.ARN == "" {
		return "", false
	}
	return entry.ARN, true
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
	kmsClient kmsAPI,
	namespace, crName, crUID, resourceName string,
	opts keyOptions,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	alias := aliasName(namespace, crName, resourceName, opts)

	if entry, entryFound := status.FindManagedResource(ledger, resourceType, resourceName); entryFound {
		return resumeKey(ctx, kmsClient, namespace, crName, crUID, alias, opts, entry, ledger, recordEvent)
	}

	descirbeOut, err := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: &alias,
	})
	var notFound *types.NotFoundException
	if errors.As(err, &notFound) {
		return createKey(ctx, kmsClient, namespace, crName, crUID, alias, resourceName, opts, ledger, checkpoint, recordEvent)
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up KMS key alias")
	}

	return adoptKey(ctx, kmsClient, namespace, crName, crUID, alias, resourceName, opts, *descirbeOut.KeyMetadata, ledger, recordEvent)
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
	kmsClient kmsAPI,
	namespace, crName, crUID, alias, resourceName string,
	opts keyOptions,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	createOut, err := kmsClient.CreateKey(ctx, &kms.CreateKeyInput{
		Tags: mapToTags(ownerTags(namespace, crName, crUID)),
	})
	if err != nil {
		return ledger, wrapAWSError(err, "creating KMS key")
	}
	arn := *createOut.KeyMetadata.Arn
	keyID := *createOut.KeyMetadata.KeyId

	ledger = recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateTagPending)

	// Persist now, before EnableKeyRotation/CreateAlias: the key already
	// exists and is durably tagged as ours, but until it has an alias it's
	// only findable by ARN — a crash here with no record of this ARN
	// anywhere would leave AWS holding a real, billed key that the next
	// reconcile has no way to find and would create a second one alongside.
	if checkpoint != nil {
		if err := checkpoint(ctx, ledger); err != nil {
			return ledger, wrapAWSError(err, "checkpointing ledger before creating KMS alias")
		}
	}
	// Reported at the same point the checkpoint above persists.
	if recordEvent != nil {
		recordEvent("Normal", "KeyCreated", fmt.Sprintf("Created KMS key %s", arn))
	}

	if err := ensureKeyPolicy(ctx, kmsClient, arn); err != nil {
		return ledger, wrapAWSError(err, "setting KMS key policy")
	}

	// Automatic rotation is always on and never exposed as spec config; it is
	// re-asserted at revalidation in case someone turned it off.
	if _, err := kmsClient.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{
		KeyId: &keyID,
	}); err != nil {
		return ledger, wrapAWSError(err, "enabling automatic key rotation")
	}

	if err := ensureAlias(ctx, kmsClient, alias, arn, keyID); err != nil {
		return ledger, err
	}
	if recordEvent != nil {
		recordEvent("Normal", "KeyAliasCreated", fmt.Sprintf("Created alias %s for KMS key %s", alias, arn))
	}

	return recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateVerified), nil
}

// resumeKey handles a resourceName this CR's own ledger already has an
// entry for, regardless of that entry's state — re-verifying ownership,
// finishing alias creation if that's what didn't complete last time, and
// reviving the key out of a scheduled deletion if Cleanup's own quiet
// window scheduled one and the resource has since reappeared in spec.
// Once the entry is already Verified and within its trust window, the
// ownership-tag re-check is skipped - the same cadence every other resource
// type applies - while existence and key state are still read every pass.
func resumeKey(
	ctx context.Context,
	kmsClient kmsAPI,
	namespace, crName, crUID, alias string,
	opts keyOptions,
	entry depsv1alpha1.ManagedResource,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	// Existence and key state are read on every pass: the trust window only
	// exempts the ownership-tag re-check. A key deleted out-of-band is an
	// error either way, never a recreate, so reading it late would only delay
	// the report; a deletion someone else scheduled is reversed by the full
	// path below, so it is better caught now than at the next revalidation.
	describeOut, err := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{
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
		if recordEvent != nil {
			recordEvent("Warning", "KeyMissing", fmt.Sprintf("KMS key %s no longer exists in AWS and will not be recreated", entry.ARN))
		}
		return ledger, fmt.Errorf(
			"KMS key %q (%s) no longer exists in AWS — it was deleted out-of-band, and everything encrypted under it is unrecoverable; remove and re-add this entry to provision a new key once you've confirmed that's acceptable",
			entry.Name, entry.ARN,
		)
	}
	if err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("looking up KMS key %q", entry.Name))
	}
	if err := keyStateError(entry.ARN, describeOut.KeyMetadata.KeyState, recordEvent); err != nil {
		return ledger, err
	}

	if !status.NeedsRevalidation(entry) && describeOut.KeyMetadata.KeyState != types.KeyStatePendingDeletion {
		updated := entry
		updated.DeletionPolicy = opts.deletionPolicy
		status.UpsertManagedResource(&ledger, updated)
		return ledger, nil
	}

	keyID := *describeOut.KeyMetadata.KeyId

	tags, tErr := listAllResourceTags(ctx, kmsClient, entry.ARN)
	if tErr != nil {
		return ledger, wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of KMS key %q", entry.Name))
	}
	if tagMap := tagsToMap(tags); !cloudctlaws.IsOwnedBy(tagMap, namespace, crName, crUID) {
		staleUID, stale := cloudctlaws.IsStaleUID(tagMap, namespace, crName, crUID)
		if !stale || !cloudctlaws.MayTransferFrom(ctx, staleUID) {
			return ledger, fmt.Errorf("KMS key %q is no longer tagged as owned by this CR - refusing to manage it further", entry.Name)
		}
		if _, err := kmsClient.TagResource(ctx, &kms.TagResourceInput{
			KeyId: &entry.ARN,
			Tags:  mapToTags(cloudctlaws.MergeTags(tagMap, ownerTags(namespace, crName, crUID))),
		}); err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("taking over KMS key %q (tagging)", entry.Name))
		}
		if recordEvent != nil {
			recordEvent("Normal", cloudctlaws.EventOwnershipTransferred, fmt.Sprintf("Took over KMS key %s from a previous incarnation of this CR", entry.ARN))
		}
	}

	if describeOut.KeyMetadata.KeyState == types.KeyStatePendingDeletion {
		if err := reviveKey(ctx, kmsClient, entry.ARN); err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("canceling scheduled deletion of KMS key %q now that it's declared again", entry.Name))
		}
		if recordEvent != nil {
			recordEvent("Normal", "KeyDeletionCancelled", fmt.Sprintf("Canceled scheduled deletion of KMS key %s (alias %s); resource reappeared in spec", entry.ARN, alias))
		}
	}

	if err := ensureKeyPolicy(ctx, kmsClient, entry.ARN); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("re-asserting the key policy of KMS key %q", entry.Name))
	}
	reenabled, err := ensureRotation(ctx, kmsClient, keyID)
	if err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("re-asserting automatic rotation of KMS key %q", entry.Name))
	}
	if reenabled && recordEvent != nil {
		recordEvent("Warning", "KeyRotationReenabled", fmt.Sprintf("Automatic rotation of KMS key %s was off and has been turned back on", entry.ARN))
	}

	if err := ensureAlias(ctx, kmsClient, alias, entry.ARN, keyID); err != nil {
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
	kmsClient kmsAPI,
	namespace, crName, crUID, alias, resourceName string,
	opts keyOptions,
	meta types.KeyMetadata,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	arn := *meta.Arn
	tags, tErr := listAllResourceTags(ctx, kmsClient, arn)
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
	transferred := false
	if staleUID, stale := cloudctlaws.IsStaleUID(tagMap, namespace, crName, crUID); stale {
		if !cloudctlaws.MayTransferFrom(ctx, staleUID) {
			return ledger, fmt.Errorf("KMS alias %q is tagged with this CR's name but a different UID (%s) — likely a stale resource from a deleted-and-recreated CR, refusing to adopt automatically; to take it over, annotate this CR with %s=%s", alias, staleUID, cloudctlaws.AdoptFromUIDAnnotation, staleUID)
		}
		transferred = true
	}
	if !transferred && !opts.adopt {
		return ledger, fmt.Errorf("KMS alias %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", alias)
	}

	merged := cloudctlaws.MergeTags(tagMap, ownerTags(namespace, crName, crUID))
	if _, err := kmsClient.TagResource(ctx, &kms.TagResourceInput{
		KeyId: &arn,
		Tags:  mapToTags(merged),
	}); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("adopting KMS key %q (tagging)", alias))
	}
	if recordEvent != nil {
		if transferred {
			recordEvent("Normal", cloudctlaws.EventOwnershipTransferred, fmt.Sprintf("Took over KMS key %s (alias %s) from a previous incarnation of this CR", arn, alias))
		} else {
			recordEvent("Normal", "KeyAdopted", fmt.Sprintf("Adopted existing KMS key %s (alias %s) under management", arn, alias))
		}
	}

	if meta.KeyState == types.KeyStatePendingDeletion {
		if err := reviveKey(ctx, kmsClient, arn); err != nil {
			return ledger, wrapAWSError(err, fmt.Sprintf("canceling scheduled deletion of adopted KMS key %q", alias))
		}
		if recordEvent != nil {
			recordEvent("Normal", "KeyDeletionCancelled", fmt.Sprintf("Canceled scheduled deletion of adopted KMS key %s (alias %s)", arn, alias))
		}
		meta.KeyState = types.KeyStateEnabled
	}

	ledger = recordKey(ledger, resourceName, arn, opts.deletionPolicy, depsv1alpha1.ManagedResourceStateVerified)
	if err := keyStateError(arn, meta.KeyState, recordEvent); err != nil {
		return ledger, err
	}
	if err := ensureKeyPolicy(ctx, kmsClient, arn); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("setting the key policy of adopted KMS key %q", alias))
	}
	if _, err := ensureRotation(ctx, kmsClient, *meta.KeyId); err != nil {
		return ledger, wrapAWSError(err, fmt.Sprintf("enabling automatic rotation of adopted KMS key %q", alias))
	}
	return ledger, nil
}

const (
	keyPolicyName       = "default"
	sidSNSDelivery      = "cloudctl-sns-delivery"
	sidCloudWatchAlarms = "cloudctl-cloudwatch-alarms"
)

// serviceKeyStatements are the key policy statements the operator owns,
// keyed by Sid. AWS services aren't IAM identities in the account, so only
// the key policy can let SNS deliver into an encrypted queue or CloudWatch
// publish to an encrypted topic.
func serviceKeyStatements(account string) map[string]any {
	statement := func(sid, service string) any {
		return map[string]any{
			"Sid":       sid,
			"Effect":    "Allow",
			"Principal": map[string]any{"Service": service},
			"Action":    []any{"kms:GenerateDataKey*", "kms:Decrypt"},
			"Resource":  "*",
			"Condition": map[string]any{"StringEquals": map[string]any{"aws:SourceAccount": account}},
		}
	}
	return map[string]any{
		sidSNSDelivery:      statement(sidSNSDelivery, "sns.amazonaws.com"),
		sidCloudWatchAlarms: statement(sidCloudWatchAlarms, "cloudwatch.amazonaws.com"),
	}
}

// mergeKeyPolicy adds or replaces the operator's statements in policy, by
// Sid, and keeps every other statement: PutKeyPolicy replaces the whole
// document. An empty policy gets the account root statement AWS's default
// policy has, without which IAM policies grant nothing and PutKeyPolicy is
// refused.
func mergeKeyPolicy(policy, account string) (merged string, changed bool, err error) {
	doc := map[string]any{}
	if policy != "" {
		if err := json.Unmarshal([]byte(policy), &doc); err != nil {
			return "", false, fmt.Errorf("parsing key policy: %w", err)
		}
	}
	var statements []any
	switch st := doc["Statement"].(type) {
	case []any:
		statements = st
	case map[string]any:
		statements = []any{st}
	}
	if policy == "" {
		doc["Version"] = "2012-10-17"
		statements = append(statements, map[string]any{
			"Sid":       "Enable IAM User Permissions",
			"Effect":    "Allow",
			"Principal": map[string]any{"AWS": "arn:aws:iam::" + account + ":root"},
			"Action":    "kms:*",
			"Resource":  "*",
		})
		changed = true
	}

	owned := serviceKeyStatements(account)
	for i, st := range statements {
		m, _ := st.(map[string]any)
		sid, _ := m["Sid"].(string)
		want, ours := owned[sid]
		if !ours {
			continue
		}
		if !reflect.DeepEqual(st, want) {
			statements[i] = want
			changed = true
		}
		delete(owned, sid)
	}
	for _, sid := range slices.Sorted(maps.Keys(owned)) {
		statements = append(statements, owned[sid])
		changed = true
	}
	if !changed {
		return policy, false, nil
	}
	doc["Statement"] = statements
	encoded, err := json.Marshal(doc)
	if err != nil {
		return "", false, fmt.Errorf("encoding key policy: %w", err)
	}
	return string(encoded), true, nil
}

// ensureKeyPolicy brings a key's policy up to date, writing it only when
// something changed. The account comes from the key's own ARN.
func ensureKeyPolicy(ctx context.Context, kmsClient kmsAPI, arn string) error {
	parsed, err := awsarn.Parse(arn)
	if err != nil {
		return fmt.Errorf("parsing key ARN %q: %w", arn, err)
	}
	out, err := kmsClient.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: &arn, PolicyName: aws.String(keyPolicyName)})
	if err != nil {
		return err
	}
	merged, changed, err := mergeKeyPolicy(aws.ToString(out.Policy), parsed.AccountID)
	if err != nil || !changed {
		return err
	}
	_, err = kmsClient.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{
		KeyId: &arn, PolicyName: aws.String(keyPolicyName), Policy: &merged,
	})
	return err
}

// reasonAdoptionConflict is the reason RDS gives for the same kind of refusal.
const reasonAdoptionConflict = "AdoptionConflict"

// CheckAdoptedKey decides whether a resource found encrypted with currentKey
// (a key ID, ARN or alias; empty for none or a key AWS owns) can be adopted
// by a spec that wants wantKey (an ARN, empty for none). A key AWS manages
// for the service is usable by any role in the account, so it is fine. Any
// other key must be the one the spec names: the operator only grants,
// tracks and protects keys it knows, and data already written stays
// encrypted with the old key even if the resource is switched to a new one.
func CheckAdoptedKey(ctx context.Context, kmsClient kmsAPI, currentKey, wantKey string) error {
	if currentKey == "" {
		return nil
	}
	if kmsClient == nil {
		return fmt.Errorf("no KMS client to check key %s of the resource being adopted", currentKey)
	}
	out, err := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: &currentKey})
	if err != nil {
		return wrapAWSError(err, fmt.Sprintf("looking up key %s of the resource being adopted", currentKey))
	}
	arn := aws.ToString(out.KeyMetadata.Arn)
	if out.KeyMetadata.KeyManager == types.KeyManagerTypeAws || (wantKey != "" && arn == wantKey) {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("it is encrypted with KMS key %s, which this CR doesn't manage; declare that key under "+
			"kms.resources with adopt: true and point encryption.kmsKeyRef at it", arn),
		Reason: reasonAdoptionConflict,
	}
}

// AdoptedKeyCheck returns CheckAdoptedKey bound to the key a spec wants, for
// packages that check a resource's key only once they know it exists.
func AdoptedKeyCheck(kmsClient kmsAPI, wantKey *string) func(ctx context.Context, currentKey string) error {
	return func(ctx context.Context, currentKey string) error {
		return CheckAdoptedKey(ctx, kmsClient, currentKey, aws.ToString(wantKey))
	}
}

// Reasons for a key the operator won't use, and won't change on its own:
// someone may have disabled it deliberately, during an incident for example.
const (
	reasonKeyDisabled    = "KeyDisabled"
	reasonKeyUnavailable = "KeyUnavailable"
)

// keyStateError reports a key that can't encrypt or decrypt, since every
// resource encrypted under it then fails for the app. PendingDeletion is
// handled by its callers.
func keyStateError(arn string, state types.KeyState, recordEvent status.EventRecorder) error {
	reason := reasonKeyUnavailable
	switch state {
	case types.KeyStateEnabled, types.KeyStatePendingDeletion:
		return nil
	case types.KeyStateCreating, types.KeyStateUpdating:
		return &cloudctlaws.ReconcileError{Err: fmt.Errorf("KMS key %s is %s", arn, state), Retryable: true}
	case types.KeyStateDisabled:
		reason = reasonKeyDisabled
	}
	msg := fmt.Sprintf("KMS key %s is %s, so nothing encrypted with it can be read or written; the operator does not re-enable it", arn, state)
	if recordEvent != nil {
		recordEvent("Warning", reason, msg)
	}
	return &cloudctlaws.ReconcileError{Err: errors.New(msg), Reason: reason}
}

// reviveKey cancels a scheduled deletion. AWS leaves a key Disabled after
// that, so it is enabled again too.
func reviveKey(ctx context.Context, kmsClient kmsAPI, arn string) error {
	if _, err := kmsClient.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{KeyId: &arn}); err != nil {
		return err
	}
	_, err := kmsClient.EnableKey(ctx, &kms.EnableKeyInput{KeyId: &arn})
	return err
}

// ensureRotation turns automatic rotation back on if it is off, and reports
// whether it had to.
func ensureRotation(ctx context.Context, kmsClient kmsAPI, keyID string) (reenabled bool, err error) {
	out, err := kmsClient.GetKeyRotationStatus(ctx, &kms.GetKeyRotationStatusInput{KeyId: &keyID})
	if err != nil {
		return false, err
	}
	if out.KeyRotationEnabled {
		return false, nil
	}
	if _, err := kmsClient.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{KeyId: &keyID}); err != nil {
		return false, err
	}
	return true, nil
}

// ensureAlias makes alias point at keyID/arn, tolerating the alias already
// existing as long as it already points at this exact key — the expected,
// idempotent outcome on a retried reconcile — and failing loudly if it
// points at a different key entirely (a genuine naming collision). AWS
// itself always errors on a pre-existing alias name regardless of target,
// so this tolerance has to be built here, not assumed from the API.
func ensureAlias(ctx context.Context, kmsClient kmsAPI, alias, arn, keyID string) error {
	_, err := kmsClient.CreateAlias(ctx, &kms.CreateAliasInput{
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

	describeOut, dErr := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{
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
	if existing, existingFound := status.FindManagedResource(ledger, resourceType, resourceName); existingFound {
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

func listAllResourceTags(ctx context.Context, kmsClient kmsAPI, keyID string) ([]types.Tag, error) {
	var all []types.Tag
	var marker *string
	for {
		out, err := kmsClient.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: &keyID, Marker: marker})
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

func wrapAWSError(err error, errContext string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", errContext, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
