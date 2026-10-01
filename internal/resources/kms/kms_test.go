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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func TestEnsure_CreatesKeyWithOwnerTagsAliasAndRotation(t *testing.T) {
	client := newFakeKMS()
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, resourceType, "primary")
	if entry == nil {
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
	entry := status.FindManagedResource(updated, resourceType, "primary")
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected State Verified after finishing alias creation, got %v", entry.State)
	}
	if client.aliases[aliasName("default", "checkout-service", "primary", keyOptions{})] != arn {
		t.Error("expected the alias to now point at the existing key")
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
	entry := status.FindManagedResource(ledger, resourceType, "primary")
	if entry == nil || entry.ARN != arn {
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
	if status.FindManagedResource(ledger, resourceType, longKey) == nil {
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
	entry := status.FindManagedResource(ledger, resourceType, ledgerName)
	if entry == nil {
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

	entry := status.FindManagedResource(ledger, resourceType, DedicatedKeyLedgerName("sqs", "orders"))
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
		arn: arn, keyID: "existing-id", keyState: types.KeyStatePendingDeletion,
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
