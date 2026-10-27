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
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func TestEnsure_CreatesKeyWithOwnerTagsAliasAndRotation(t *testing.T) {
	client := newFakeKMS()
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry, entryFound := status.FindManagedResource(ledger, resourceType, "primary")
	if !entryFound {
		t.Fatal("expected a ledger entry for the created key")
	}
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected State Verified, got %v", entry.State)
	}

	wantAlias := aliasName("default", "checkout-service", "primary", keyOptions{})
	arn, ok := client.aliases[wantAlias]
	if !ok {
		t.Fatalf("expected alias %q to be created", wantAlias)
	}
	if arn != entry.ARN {
		t.Errorf("alias points at %q, want %q", arn, entry.ARN)
	}

	key := client.keys[entry.ARN]
	if !cloudctlaws.IsOwnedBy(key.tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the key to be tagged as owned by this CR")
	}
	if !key.rotationEnabled {
		t.Error("expected automatic key rotation to be enabled on a newly created key")
	}
}

func TestEnsure_ResumesFromLedgerWithoutRecreatingKeyWhenAliasIsMissing(t *testing.T) {
	client := newFakeKMS()
	// Simulate a previous reconcile that succeeded at CreateKey (tags
	// already atomic) but crashed before CreateAlias ever ran.
	arn := "arn:aws:kms:us-east-1:123456789012:key/existing-id"
	client.keys[arn] = &fakeKey{
		arn:      arn,
		keyID:    "existing-id",
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
		keyState: types.KeyStateEnabled,
	}
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, State: depsv1alpha1.ManagedResourceStateTagPending, CreatedAt: metav1.Now()},
	}

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	updated, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if len(client.keys) != 1 {
		t.Fatalf("expected no second key to be created, got %d keys", len(client.keys))
	}
	entry, _ := status.FindManagedResource(updated, resourceType, "primary")
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected State Verified after finishing alias creation, got %v", entry.State)
	}
	if client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] != arn {
		t.Error("expected the alias to now point at the existing key")
	}
}

// TestEnsure_SkipsReVerificationWithinTrustWindow proves resumeKey skips the
// ownership re-check (ListResourceTags) once an entry is already Verified
// and recently checked, the same trust-window cadence the other resource
// types apply, while still reading the key itself on every pass.
func TestEnsure_SkipsReVerificationWithinTrustWindow(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	recentlyVerified := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt: metav1.Now(), LastVerifiedAt: &recentlyVerified, State: depsv1alpha1.ManagedResourceStateVerified},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	updated, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.describeKeyCalls[arn] != 1 {
		t.Errorf("expected exactly one DescribeKey call (existence is read every pass), got %d", client.describeKeyCalls[arn])
	}
	if client.listResourceTagsCalls[arn] != 0 {
		t.Errorf("expected no ListResourceTags call within the trust window, got %d", client.listResourceTagsCalls[arn])
	}
	entry, entryFound := status.FindManagedResource(updated, resourceType, "primary")
	if !entryFound || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the entry to remain Verified, got %+v", entry)
	}
}

// TestEnsure_ReVerifiesOnceTrustWindowExpires is the other half: once the
// window has elapsed, the real check must still run.
func TestEnsure_ReVerifiesOnceTrustWindowExpires(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt: metav1.Now(), LastVerifiedAt: &stale, State: depsv1alpha1.ManagedResourceStateVerified},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	updated, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.describeKeyCalls[arn] != 1 {
		t.Errorf("expected exactly 1 DescribeKey call once the trust window expired, got %d", client.describeKeyCalls[arn])
	}
	if client.listResourceTagsCalls[arn] != 1 {
		t.Errorf("expected exactly 1 ListResourceTags call once the trust window expired, got %d", client.listResourceTagsCalls[arn])
	}
	entry, entryFound := status.FindManagedResource(updated, resourceType, "primary")
	if !entryFound || entry.LastVerifiedAt == nil || !entry.LastVerifiedAt.After(stale.Time) {
		t.Error("expected LastVerifiedAt to be refreshed after a real re-verification")
	}
}

func TestEnsure_RefusesForeignAliasedKeyWithoutAdopt(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error for an untagged pre-existing key alias without adopt:true")
	}
}

func TestEnsure_AdoptsUntaggedKeyWhenRequested(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled, tags: map[string]string{"team": "someone-else"}}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if !cloudctlaws.IsOwnedBy(client.keys[arn].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the key to be tagged as owned by this CR after adoption")
	}
	if client.keys[arn].tags["team"] != "someone-else" {
		t.Error("expected pre-existing tags to be preserved during adoption")
	}
	entry, entryFound := status.FindManagedResource(ledger, resourceType, "primary")
	if !entryFound || entry.ARN != arn {
		t.Fatal("expected a ledger entry pointing at the adopted key")
	}
}

func TestEnsure_RejectsKeyOwnedByDifferentCREvenWithAdopt(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{
		arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled,
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "other-service"), cloudctlaws.OwnerUIDTagKey: "uid-2"},
	}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a key owned by a different AppDependencies CR")
	}
}

// TestEnsure_RejectsKeyWithStaleUIDEvenWithAdopt guards against the
// deleted-and-recreated-CR case: a key tagged with this exact CR's own
// namespace/name, but a different UID, must never be silently re-adopted
// just because adopt:true is set - a name match alone is never ownership.
func TestEnsure_RejectsKeyWithStaleUIDEvenWithAdopt(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/stale-id"
	client.keys[arn] = &fakeKey{
		arn: arn, keyID: "stale-id", keyState: types.KeyStateEnabled,
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "old-uid"},
	}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "new-uid", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected adopt:true to never override a key tagged with this CR's name but a stale (different) UID")
	}
}

func TestEnsure_DetectsAliasCollisionWithADifferentKey(t *testing.T) {
	client := newFakeKMS()
	// A ledger entry exists (this CR believes it owns a key), but the
	// deterministic alias name has since been claimed by an entirely
	// different key - e.g. someone manually created one out-of-band.
	ourARN := "arn:aws:kms:us-east-1:123456789012:key/our-id"
	otherARN := "arn:aws:kms:us-east-1:123456789012:key/other-id"
	ownerTags := map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"}
	client.keys[ourARN] = &fakeKey{arn: ourARN, keyID: "our-id", tags: ownerTags, keyState: types.KeyStateEnabled}
	client.keys[otherARN] = &fakeKey{arn: otherARN, keyID: "other-id", keyState: types.KeyStateEnabled}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = otherARN

	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: ourARN, State: depsv1alpha1.ManagedResourceStateTagPending, CreatedAt: metav1.Now()},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err == nil {
		t.Fatal("expected an error when the deterministic alias points at a different key than this CR's own ledger entry")
	}
}

func TestEnsure_RefusesToRecreateAfterOutOfBandDeletion(t *testing.T) {
	client := newFakeKMS()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: "arn:aws:kms:us-east-1:123456789012:key/gone", State: depsv1alpha1.ManagedResourceStateVerified, CreatedAt: metav1.Now()},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err == nil {
		t.Fatal("expected an error when the ledger's key no longer exists in AWS at all")
	}
	if len(client.keys) != 0 {
		t.Error("expected no replacement key to be silently created")
	}
}

func TestEnsure_CancelsScheduledDeletionWhenResourceReappearsInSpec(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/existing-id"
	client.keys[arn] = &fakeKey{
		arn: arn, keyID: "existing-id", keyState: types.KeyStatePendingDeletion,
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
	}
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, State: depsv1alpha1.ManagedResourceStateVerified, CreatedAt: metav1.Now()},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Errorf("expected scheduled deletion to be canceled, got KeyState %v", client.keys[arn].keyState)
	}
}

func TestEnsure_TruncatesAliasExceedingKMSLimit(t *testing.T) {
	client := newFakeKMS()
	longKey := strings.Repeat("a", 250)
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: longKey}}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if len(client.keys) != 1 {
		t.Errorf("expected a key to have been created despite the over-length name, got %d keys", len(client.keys))
	}
	if !status.HasManagedResource(ledger, resourceType, longKey) {
		t.Error("expected a ledger entry for the key")
	}
}

func TestEnsure_ClassifiesPermissionErrorsAsNotRetryable(t *testing.T) {
	client := newFakeKMS()
	client.createKeyErr = &fakeAWSError{code: "AccessDeniedException", fault: smithy.FaultClient}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected a permission-denied error to be classified as not retryable")
	}
}

func TestEnsureDedicatedKey_CreatesKeyUnderDerivedLedgerName(t *testing.T) {
	client := newFakeKMS()

	arn, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyDelete, nil, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() error = %v", err)
	}
	if arn == "" {
		t.Fatal("expected a non-empty ARN")
	}

	ledgerName := DedicatedKeyLedgerName("sqs", "orders")
	entry, entryFound := status.FindManagedResource(ledger, resourceType, ledgerName)
	if !entryFound {
		t.Fatalf("expected a ledger entry named %q, not \"orders\" - must never collide with a user's own kms.resources entry of the same base name", ledgerName)
	}
	if entry.ARN != arn {
		t.Errorf("ledger ARN = %q, want %q", entry.ARN, arn)
	}

	wantAlias := aliasName("default", "checkout-service", ledgerName, keyOptions{aliasParts: []string{"sqs", "orders", "key"}})
	if client.aliases[wantAlias] != arn {
		t.Errorf("expected alias %q to point at %q", wantAlias, arn)
	}
	if strings.Contains(wantAlias, "#") {
		t.Errorf("alias %q must never contain '#' — AWS rejects it in a real alias name", wantAlias)
	}
}

func TestEnsureDedicatedKey_AliasNeverCollidesWithAPlainKeyOfTheSameVisibleName(t *testing.T) {
	// A dedicated key for sqs/orders and a plain kms.resources entry named
	// "sqs-orders-key" have the same human-readable prefix once derived,
	// but must hash to different aliases — DerivedResourceName hashes
	// "sqs", "orders", "key" as three separate tuple elements, never
	// pre-joined into the single string a plain ResourceName call would see.
	dedicated := aliasName("default", "checkout-service", DedicatedKeyLedgerName("sqs", "orders"), keyOptions{aliasParts: []string{"sqs", "orders", "key"}})
	plain := aliasName("default", "checkout-service", "sqs-orders-key", keyOptions{})
	if dedicated == plain {
		t.Errorf("dedicated key alias %q must not collide with a plain resource's alias of the same visible name", dedicated)
	}
}

func TestEnsureDedicatedKey_ReusesExistingKeyOnSecondCall(t *testing.T) {
	client := newFakeKMS()

	arn1, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyDelete, nil, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() first call error = %v", err)
	}
	arn2, _, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyDelete, ledger, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() second call error = %v", err)
	}

	if arn1 != arn2 {
		t.Errorf("expected the same key ARN across calls, got %q then %q", arn1, arn2)
	}
	if len(client.keys) != 1 {
		t.Errorf("expected exactly one key to exist, got %d", len(client.keys))
	}
}

// Unlike the same resource reconciling twice (above, which correctly
// reuses one key), two different resources that happen to share a name
// across sections — an SQS queue and an S3 bucket both named "data" —
// must never receive the same dedicated key.
func TestEnsureDedicatedKey_DifferentOwningResourcesNeverShareAKey(t *testing.T) {
	client := newFakeKMS()

	sqsKeyARN, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "data", depsv1alpha1.DeletionPolicyDelete, nil, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() for the sqs queue named \"data\" error = %v", err)
	}
	s3KeyARN, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "s3", "data", depsv1alpha1.DeletionPolicyDelete, ledger, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() for the s3 bucket named \"data\" error = %v", err)
	}

	if sqsKeyARN == s3KeyARN {
		t.Errorf("the sqs queue's dedicated key and the s3 bucket's dedicated key are the same key (%s) — each resource was promised a key dedicated to it alone", sqsKeyARN)
	}
	if len(client.keys) != 2 {
		t.Errorf("expected 2 distinct KMS keys, got %d", len(client.keys))
	}
	if len(ledger) != 2 {
		t.Errorf("expected 2 ledger entries, got %d: %+v", len(ledger), ledger)
	}
}

func TestEnsureDedicatedKey_UpdatesDeletionPolicyOnEachCall(t *testing.T) {
	client := newFakeKMS()

	_, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyRetain, nil, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() error = %v", err)
	}
	_, ledger, err = EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyDelete, ledger, nil, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() error = %v", err)
	}

	entry, _ := status.FindManagedResource(ledger, resourceType, DedicatedKeyLedgerName("sqs", "orders"))
	if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
		t.Errorf("expected DeletionPolicy to track the owning resource's current policy, got %v", entry.DeletionPolicy)
	}
}

// A crash between CreateKey and CreateAlias must never leave AWS holding a
// second, orphaned key once a later reconcile retries from a lost ledger -
// as long as the checkpoint fired before the crash actually got persisted.
func TestEnsureDedicatedKey_CrashBetweenCreateAndAliasDoesNotOrphanAKey(t *testing.T) {
	client := newFakeKMS()
	client.createAliasErr = errors.New("simulated crash: process died before CreateAlias ran")

	// The checkpoint stands in for the controller's real Status().Update()
	// call - captures whatever ledger state existed at the moment it fired,
	// independent of what EnsureDedicatedKey itself eventually returns.
	var persisted []depsv1alpha1.ManagedResource
	checkpoint := func(_ context.Context, ledger []depsv1alpha1.ManagedResource) error {
		persisted = ledger
		return nil
	}

	// First reconcile: CreateKey succeeds, then the process dies before
	// CreateAlias - indistinguishable, from the ledger's point of view,
	// from CreateAlias itself failing, since neither one is ever recorded
	// without the checkpoint below.
	_, _, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyDelete, nil, checkpoint, nil)
	if err == nil {
		t.Fatal("expected the simulated crash to surface as an error")
	}
	if len(client.keys) != 1 {
		t.Fatalf("expected exactly one key in AWS after the failed first attempt, got %d", len(client.keys))
	}
	if len(persisted) == 0 {
		t.Fatal("expected the checkpoint to have been called with the key recorded before CreateAlias ran")
	}

	// Second reconcile: simulates the crash by starting from exactly (and
	// only) what the checkpoint captured, not from whatever the first call
	// returned in memory - the in-memory return value is what a real crash
	// would actually lose.
	client.createAliasErr = nil
	_, _, err = EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "sqs", "orders", depsv1alpha1.DeletionPolicyDelete, persisted, checkpoint, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() second attempt error = %v", err)
	}

	if len(client.keys) != 1 {
		t.Errorf("expected the first key to be found and reused, got %d keys total in AWS", len(client.keys))
	}
}

func TestEnsure_ContinuesToOtherKeysAfterOneFails(t *testing.T) {
	client := newFakeKMS()
	foreignARN := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[foreignARN] = &fakeKey{arn: foreignARN, keyID: "foreign-id", keyState: types.KeyStateEnabled}
	client.aliases[aliasName("default", "checkout-service", "bad-key", keyOptions{})] = foreignARN

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: "bad-key"},
		{Name: "good-key"},
	}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error reported for the untagged pre-existing key")
	}
	if len(client.keys) != 2 {
		t.Errorf("expected the second, valid key to still be created, got %d keys", len(client.keys))
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

func TestEnsure_CreatingKey_EmitsCreatedAndAliasedEvents(t *testing.T) {
	client := newFakeKMS()
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if len(*events) != 2 {
		t.Fatalf("expected exactly two events, got %+v", *events)
	}
	if (*events)[0].reason != "KeyCreated" {
		t.Errorf("expected the first event to be KeyCreated, got %+v", (*events)[0])
	}
	if (*events)[1].reason != "KeyAliasCreated" {
		t.Errorf("expected the second event to be KeyAliasCreated, got %+v", (*events)[1])
	}
}

// A crash (or any failure) between CreateKey and CreateAlias must still
// leave a KeyCreated event on the record with no matching KeyAliasCreated -
// that gap is exactly the diagnostic trail explaining a key that exists in
// AWS but isn't fully usable yet.
func TestEnsure_FailingBeforeAlias_EmitsOnlyCreatedEvent(t *testing.T) {
	client := newFakeKMS()
	client.createAliasErr = errors.New("simulated failure before CreateAlias ran")
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, recordEvent); err == nil {
		t.Fatal("expected the simulated failure to surface as an error")
	}

	if len(*events) != 1 || (*events)[0].reason != "KeyCreated" {
		t.Errorf("expected exactly one KeyCreated event and no KeyAliasCreated, got %+v", *events)
	}
}

func TestEnsure_AdoptingKey_EmitsAdoptedEvent(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled, tags: map[string]string{"team": "someone-else"}}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "KeyAdopted" {
		t.Errorf("expected exactly one KeyAdopted event, got %+v", *events)
	}
}

func TestEnsure_CancelingScheduledDeletion_EmitsEvent(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/existing-id"
	client.keys[arn] = &fakeKey{
		arn: arn, keyID: "existing-id", keyState: types.KeyStatePendingDeletion, rotationEnabled: true,
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
	}
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, State: depsv1alpha1.ManagedResourceStateVerified, CreatedAt: metav1.Now()},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "KeyDeletionCancelled" {
		t.Errorf("expected exactly one KeyDeletionCancelled event, got %+v", *events)
	}
}

func TestEnsure_KeyDeletedOutOfBandWithinTrustWindow_IsALoudErrorNotARecreate(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/gone"
	recentlyVerified := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt: metav1.Now(), LastVerifiedAt: &recentlyVerified, State: depsv1alpha1.ManagedResourceStateVerified},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	var events []string
	record := func(_, reason, _ string) { events = append(events, reason) }
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, record)
	if err == nil || !strings.Contains(err.Error(), "no longer exists in AWS") {
		t.Fatalf("expected a loud error for a key missing inside the trust window, got %v", err)
	}
	if len(client.keys) != 0 {
		t.Error("expected no replacement key to be created")
	}
	if !slices.Contains(events, "KeyMissing") {
		t.Errorf("expected a KeyMissing warning, got %v", events)
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.KeyMissing {
		t.Errorf("the condition reason should be KeyMissing, got %v", err)
	}
}

func TestEnsureAlias_AnAliasOnAnotherKeyIsReportedAsNameTaken(t *testing.T) {
	client := newFakeKMS()
	other := "arn:aws:kms:us-east-1:123456789012:key/other-id"
	client.keys[other] = &fakeKey{arn: other, keyID: "other-id", keyState: types.KeyStateEnabled}
	alias := aliasName("default", "checkout-service", "primary", keyOptions{})
	client.aliases[alias] = other

	err := ensureAlias(context.Background(), client, alias, "arn:aws:kms:us-east-1:123456789012:key/mine", "mine")

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.NameTaken {
		t.Fatalf("an alias that points at another key should be NameTaken, got %v", err)
	}
	if reconcileErr.Retryable {
		t.Error("a name collision does not fix itself, so it must not be retried")
	}
	if client.aliases[alias] != other {
		t.Error("the other key's alias must be left alone")
	}
}

func TestEnsure_DeletionScheduledOutOfBandWithinTrustWindow_IsCancelled(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	client.keys[arn].keyState = types.KeyStatePendingDeletion
	recentlyVerified := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt: metav1.Now(), LastVerifiedAt: &recentlyVerified, State: depsv1alpha1.ManagedResourceStateVerified},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Errorf("expected a deletion scheduled by someone else to be cancelled while the key is still declared, got %v", client.keys[arn].keyState)
	}
}

func trustedKeyLedger(arn string) []depsv1alpha1.ManagedResource {
	recentlyVerified := metav1.Now()
	return []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt: metav1.Now(), LastVerifiedAt: &recentlyVerified, State: depsv1alpha1.ManagedResourceStateVerified},
	}
}

// Every resource encrypted under a key that can't be used fails for the app,
// so the key's own section must not stay Ready. Someone may have disabled it
// on purpose, so it is reported, never re-enabled.
func TestEnsure_KeyThatCannotBeUsedIsReportedNotReEnabled(t *testing.T) {
	cases := []struct {
		state      types.KeyState
		wantReason string
	}{
		{types.KeyStateDisabled, "KeyDisabled"},
		{types.KeyStateUnavailable, "KeyUnavailable"},
		{types.KeyStatePendingImport, "KeyUnavailable"},
		{types.KeyStatePendingReplicaDeletion, "KeyUnavailable"},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			client := newFakeKMS()
			arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
			client.keys[arn] = ownedFakeKey(arn, "id-1")
			client.keys[arn].keyState = tc.state
			spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
			recordEvent, events := newEventCollector()

			_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, trustedKeyLedger(arn), nil, recordEvent)
			var reconcileErr *cloudctlaws.ReconcileError
			if !errors.As(err, &reconcileErr) || reconcileErr.Reason != tc.wantReason || reconcileErr.Retryable {
				t.Fatalf("expected a non-retryable %s error, got %v", tc.wantReason, err)
			}
			if client.keys[arn].keyState != tc.state {
				t.Errorf("key state changed to %v, want it left at %v", client.keys[arn].keyState, tc.state)
			}
			if len(*events) != 1 || (*events)[0].eventType != "Warning" || (*events)[0].reason != tc.wantReason {
				t.Errorf("expected one %s warning, got %+v", tc.wantReason, *events)
			}
		})
	}
}

func TestEnsure_RotationTurnedOffIsReEnabledAtRevalidation(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	client.keys[arn].rotationEnabled = false
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	ledger := trustedKeyLedger(arn)
	ledger[0].LastVerifiedAt = &stale
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	recordEvent, events := newEventCollector()

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if !client.keys[arn].rotationEnabled {
		t.Error("expected rotation to be re-enabled")
	}
	if len(*events) != 1 || (*events)[0].reason != "KeyRotationReenabled" {
		t.Errorf("expected one KeyRotationReenabled event, got %+v", *events)
	}
}

func TestEnsure_AdoptedKeyGetsRotation(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled, tags: map[string]string{}}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if !client.keys[arn].rotationEnabled {
		t.Error("expected an adopted key to get the same rotation as a created one")
	}
}

type policyStatementView struct {
	Sid       string
	Principal map[string]any
	Condition map[string]map[string]any
}

func keyPolicyStatements(t *testing.T, policy string) map[string]policyStatementView {
	t.Helper()
	var doc struct{ Statement []policyStatementView }
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		t.Fatalf("key policy is not valid JSON: %v\n%s", err, policy)
	}
	bySid := map[string]policyStatementView{}
	for _, st := range doc.Statement {
		bySid[st.Sid] = st
	}
	return bySid
}

// A key is created with the account root statement and no service statements:
// which services need it depends on what is encrypted with it, so
// SyncServiceGrants decides, and a key nothing uses grants no service anything.
func TestEnsure_CreatedKeyPolicyHasTheRootStatementAndNoServiceGrants(t *testing.T) {
	client := newFakeKMS()
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry, _ := status.FindManagedResource(ledger, resourceType, "primary")
	statements := keyPolicyStatements(t, client.keys[entry.ARN].policy)

	if _, ok := statements[sidSNSDelivery]; ok {
		t.Errorf("a key nothing delivers into got the SNS statement: %v", statements)
	}
	if _, ok := statements[sidCloudWatchAlarms]; ok {
		t.Errorf("a key no topic uses got the CloudWatch statement: %v", statements)
	}
	rootKept := false
	for _, st := range statements {
		if st.Principal["AWS"] == "arn:aws:iam::123456789012:root" {
			rootKept = true
		}
	}
	if !rootKept {
		t.Error("expected the account root statement, without which IAM policies grant nothing and PutKeyPolicy is refused")
	}
}

const teamAdmins = `{"Version":"2012-10-17","Statement":{"Sid":"team-admins","Effect":"Allow",` +
	`"Principal":{"AWS":"arn:aws:iam::123456789012:role/admins"},"Action":"kms:*","Resource":"*"}}`

const keyForGrants = "arn:aws:kms:us-east-1:123456789012:key/id-1"

func keyWithPolicy(policy string) *fakeKMS {
	client := newFakeKMS()
	client.keys[keyForGrants] = ownedFakeKey(keyForGrants, "id-1")
	client.keys[keyForGrants].policy = policy
	return client
}

func TestSyncServiceGrants_SNSMayUseTheKeyOnlyForTheTopicsThatDeliverIntoIt(t *testing.T) {
	client := keyWithPolicy(teamAdmins)
	topicA := "arn:aws:sns:us-east-1:123456789012:orders"
	topicB := "arn:aws:sns:us-east-1:123456789012:billing"

	if err := SyncServiceGrants(context.Background(), client, keyForGrants, ServiceGrants{SNSTopicARNs: []string{topicB, topicA, topicA}}, nil); err != nil {
		t.Fatalf("SyncServiceGrants() error = %v", err)
	}

	statements := keyPolicyStatements(t, client.keys[keyForGrants].policy)
	if _, ok := statements["team-admins"]; !ok {
		t.Error("another statement was dropped")
	}
	if _, ok := statements[sidCloudWatchAlarms]; ok {
		t.Error("CloudWatch got a statement although no alarm publishes to a topic encrypted with this key")
	}
	st, ok := statements[sidSNSDelivery]
	if !ok {
		t.Fatalf("expected the SNS statement, got %v", statements)
	}
	if st.Principal["Service"] != "sns.amazonaws.com" || st.Condition["StringEquals"]["aws:SourceAccount"] != "123456789012" {
		t.Errorf("the statement should name SNS and this account, got %+v", st)
	}
	var doc struct {
		Statement []struct {
			Sid       string
			Condition map[string]map[string]any
		}
	}
	if err := json.Unmarshal([]byte(client.keys[keyForGrants].policy), &doc); err != nil {
		t.Fatal(err)
	}
	for _, s := range doc.Statement {
		if s.Sid == sidSNSDelivery {
			got := s.Condition["ArnEquals"]["aws:SourceArn"]
			want := []any{topicB, topicA} // sorted: billing before orders
			if !reflect.DeepEqual(got, want) {
				t.Errorf("aws:SourceArn = %v, want exactly the two topics, sorted and without repeats: %v", got, want)
			}
		}
	}
}

func TestSyncServiceGrants_CloudWatchMayUseTheKeyOnlyForTheAlarmsThatPublishToIt(t *testing.T) {
	client := keyWithPolicy(teamAdmins)
	alarm := "arn:aws:cloudwatch:us-east-1:123456789012:alarm:shop-checkout-age"

	if err := SyncServiceGrants(context.Background(), client, keyForGrants, ServiceGrants{AlarmARNs: []string{alarm}}, nil); err != nil {
		t.Fatalf("SyncServiceGrants() error = %v", err)
	}

	statements := keyPolicyStatements(t, client.keys[keyForGrants].policy)
	if _, ok := statements[sidSNSDelivery]; ok {
		t.Error("SNS got a statement although no queue encrypted with this key subscribes to a topic")
	}
	if st := statements[sidCloudWatchAlarms]; st.Principal["Service"] != "cloudwatch.amazonaws.com" {
		t.Errorf("expected the CloudWatch statement, got %v", statements)
	}
}

// Earlier versions put both statements, open to every topic and alarm in the
// account, on every key. They are narrowed or removed, and nothing else is.
func TestSyncServiceGrants_RetiresTheAccountWideStatementsEarlierVersionsWrote(t *testing.T) {
	legacy := `{"Version":"2012-10-17","Statement":[` +
		`{"Sid":"team-admins","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/admins"},"Action":"kms:*","Resource":"*"},` +
		`{"Sid":"cloudctl-sns-delivery","Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":["kms:GenerateDataKey*","kms:Decrypt"],"Resource":"*","Condition":{"StringEquals":{"aws:SourceAccount":"123456789012"}}},` +
		`{"Sid":"cloudctl-cloudwatch-alarms","Effect":"Allow","Principal":{"Service":"cloudwatch.amazonaws.com"},"Action":["kms:GenerateDataKey*","kms:Decrypt"],"Resource":"*","Condition":{"StringEquals":{"aws:SourceAccount":"123456789012"}}}]}`
	client := keyWithPolicy(legacy)
	recordEvent, events := collectEvents()

	if err := SyncServiceGrants(context.Background(), client, keyForGrants, ServiceGrants{}, recordEvent); err != nil {
		t.Fatalf("SyncServiceGrants() error = %v", err)
	}

	statements := keyPolicyStatements(t, client.keys[keyForGrants].policy)
	if len(statements) != 1 || statements["team-admins"].Sid == "" {
		t.Errorf("only the team's own statement should remain, got %v", statements)
	}
	if !slices.Contains(*events, "KeyPolicyUpdated") {
		t.Errorf("changing a key policy should be said in an event, got %v", *events)
	}
}

func TestSyncServiceGrants_WritesOnlyWhenSomethingDiffers(t *testing.T) {
	client := keyWithPolicy(teamAdmins)
	grants := ServiceGrants{SNSTopicARNs: []string{"arn:aws:sns:us-east-1:123456789012:orders"}}
	for i := 0; i < 3; i++ {
		if err := SyncServiceGrants(context.Background(), client, keyForGrants, grants, nil); err != nil {
			t.Fatalf("SyncServiceGrants() error = %v", err)
		}
	}
	if client.putKeyPolicyCalls != 1 {
		t.Errorf("PutKeyPolicy calls = %d over three identical syncs, want 1", client.putKeyPolicyCalls)
	}

	grants.SNSTopicARNs = append(grants.SNSTopicARNs, "arn:aws:sns:us-east-1:123456789012:billing")
	if err := SyncServiceGrants(context.Background(), client, keyForGrants, grants, nil); err != nil {
		t.Fatalf("SyncServiceGrants() error = %v", err)
	}
	if client.putKeyPolicyCalls != 2 {
		t.Errorf("PutKeyPolicy calls = %d after a topic was added, want 2", client.putKeyPolicyCalls)
	}
}

// A key scheduled for deletion cannot have its policy changed, and the KMS
// section already reports it.
func TestSyncServiceGrants_LeavesAKeyThatCannotBeChanged(t *testing.T) {
	client := keyWithPolicy(teamAdmins)
	client.putKeyPolicyErr = &types.KMSInvalidStateException{}

	err := SyncServiceGrants(context.Background(), client, keyForGrants,
		ServiceGrants{SNSTopicARNs: []string{"arn:aws:sns:us-east-1:123456789012:orders"}}, nil)

	if err != nil {
		t.Errorf("SyncServiceGrants() error = %v, want none for a key in a state that rejects policy changes", err)
	}
	if err := SyncServiceGrants(context.Background(), client, "arn:aws:kms:us-east-1:123456789012:key/gone", ServiceGrants{}, nil); err != nil {
		t.Errorf("a key that no longer exists is not this function's to report, got %v", err)
	}
}

func collectEvents() (func(eventType, reason, message string), *[]string) {
	var reasonsSeen []string
	return func(_, reason, _ string) { reasonsSeen = append(reasonsSeen, reason) }, &reasonsSeen
}

func TestCheckAdoptedKey(t *testing.T) {
	client := newFakeKMS()
	const customer = "arn:aws:kms:us-east-1:123456789012:key/customer"
	const awsManaged = "arn:aws:kms:us-east-1:123456789012:key/aws-sqs"
	client.keys[customer] = &fakeKey{arn: customer, keyID: "customer", keyState: types.KeyStateEnabled}
	client.keys[awsManaged] = &fakeKey{arn: awsManaged, keyID: "aws-sqs", keyState: types.KeyStateEnabled, keyManager: types.KeyManagerTypeAws}
	client.aliases["alias/team-key"] = customer

	cases := []struct {
		name          string
		current, want string
		wantRefused   bool
	}{
		{"not encrypted, or with a key AWS owns", "", "", false},
		{"a key AWS manages for the service", awsManaged, "", false},
		{"the key the spec names", customer, customer, false},
		{"the key the spec names, found by alias", "alias/team-key", customer, false},
		{"a customer key the spec doesn't name", customer, "", true},
		{"a customer key other than the one the spec names", customer, "arn:aws:kms:us-east-1:123456789012:key/other", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckAdoptedKey(context.Background(), client, tc.current, tc.want)
			if !tc.wantRefused {
				if err != nil {
					t.Errorf("CheckAdoptedKey() = %v, want nil", err)
				}
				return
			}
			var reconcileErr *cloudctlaws.ReconcileError
			if !errors.As(err, &reconcileErr) || reconcileErr.Reason != "AdoptionConflict" || reconcileErr.Retryable {
				t.Fatalf("expected a non-retryable AdoptionConflict, got %v", err)
			}
			if !strings.Contains(err.Error(), customer) || !strings.Contains(err.Error(), "kmsKeyRef") {
				t.Errorf("the error should name the key and say how to adopt it: %v", err)
			}
		})
	}
}

const previousUID = "old-uid"

func staleOwnerTags() map[string]string {
	return map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
		cloudctlaws.OwnerUIDTagKey: previousUID,
		"team":                     "payments",
	}
}

// A key found under this CR's alias but tagged with an earlier UID is taken
// over only when the CR was told that exact UID.
func TestEnsure_TakesOverAKeyFromTheUIDItWasTold(t *testing.T) {
	for name, tc := range map[string]struct {
		from        string
		transferred bool
	}{"the previous UID": {previousUID, true}, "a different UID": {"another-uid", false}, "no UID": {"", false}} {
		t.Run(name, func(t *testing.T) {
			client := newFakeKMS()
			arn := "arn:aws:kms:us-east-1:123456789012:key/old"
			client.keys[arn] = &fakeKey{arn: arn, keyID: "old", keyState: types.KeyStateEnabled, rotationEnabled: true, tags: staleOwnerTags()}
			client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn
			var events []string
			record := func(_, reason, _ string) { events = append(events, reason) }

			spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
			ctx := cloudctlaws.WithAdoptFromUID(context.Background(), tc.from)
			_, err := Ensure(ctx, client, "default", "checkout-service", "new-uid", spec, nil, nil, record)

			tags := client.keys[arn].tags
			if !tc.transferred {
				if err == nil || tags[cloudctlaws.OwnerUIDTagKey] != previousUID {
					t.Fatalf("expected a refusal that leaves the tags alone, got err=%v tags=%v", err, tags)
				}
				return
			}
			if err != nil {
				t.Fatalf("Ensure() error = %v", err)
			}
			if tags[cloudctlaws.OwnerUIDTagKey] != "new-uid" || tags["team"] != "payments" {
				t.Errorf("expected the new UID with other tags kept, got %v", tags)
			}
			if !slices.Contains(events, cloudctlaws.EventOwnershipTransferred) || slices.Contains(events, "KeyAdopted") {
				t.Errorf("expected OwnershipTransferred and no adoption event, got %v", events)
			}
		})
	}
}

// With a restored ledger the key is already known, and the next ownership
// re-check meets the old UID instead.
func TestEnsure_TakesOverAKnownKeyWhoseTagsCarryThePreviousUID(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/old"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "old", keyState: types.KeyStateEnabled, rotationEnabled: true, tags: staleOwnerTags()}
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	ledger := []depsv1alpha1.ManagedResource{{
		Type: resourceType, Name: "primary", ARN: arn, State: depsv1alpha1.ManagedResourceStateVerified,
		CreatedAt: stale, LastVerifiedAt: &stale,
	}}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "new-uid", spec, ledger, nil, nil); err == nil {
		t.Fatal("without the annotation the key must be refused")
	}
	ctx := cloudctlaws.WithAdoptFromUID(context.Background(), previousUID)
	if _, err := Ensure(ctx, client, "default", "checkout-service", "new-uid", spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() with the annotation error = %v", err)
	}
	if client.keys[arn].tags[cloudctlaws.OwnerUIDTagKey] != "new-uid" {
		t.Errorf("expected the key re-tagged, got %v", client.keys[arn].tags)
	}
}

func TestPinsAKey(t *testing.T) {
	dedicated := depsv1alpha1.ManagedResource{Type: resourceType, Name: DedicatedKeyLedgerName("sqs", "orders")}
	tests := []struct {
		name   string
		ledger []depsv1alpha1.ManagedResource
		entry  depsv1alpha1.ManagedResource
		want   bool
	}{
		{"a key stamped on the entry", nil, depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", KMSKeyARN: "arn:aws:kms:us-east-1:123456789012:key/k"}, true},
		{"an older entry whose dedicated key is in the ledger", []depsv1alpha1.ManagedResource{dedicated}, depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders"}, true},
		{"a derived queue, through its parent's dedicated key", []depsv1alpha1.ManagedResource{dedicated}, depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders#dlq"}, true},
		{"an unencrypted resource", nil, depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders"}, false},
		{"another type's dedicated key does not count", []depsv1alpha1.ManagedResource{dedicated}, depsv1alpha1.ManagedResource{Type: "s3", Name: "orders"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := PinsAKey(tc.ledger, tc.entry.Type, tc.entry); got != tc.want {
				t.Errorf("PinsAKey() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnsure_AdoptingAKeyPendingDeletionCancelsTheDeletionBeforeTagging(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/existing-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "existing-id", keyState: types.KeyStatePendingDeletion, tags: map[string]string{}}
	client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] = arn
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v; AWS refuses to tag a key that is pending deletion", err)
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Errorf("expected the scheduled deletion to be canceled, got %v", client.keys[arn].keyState)
	}
	if !cloudctlaws.IsOwnedBy(client.keys[arn].tags, "default", "checkout-service", "uid-1") {
		t.Errorf("expected the key to be tagged as owned, got %v", client.keys[arn].tags)
	}
}
