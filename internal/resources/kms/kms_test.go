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

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
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

	wantAlias := "alias/default-checkout-service-primary"
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
	updated, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
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
	if client.aliases["alias/default-checkout-service-primary"] != arn {
		t.Error("expected the alias to now point at the existing key")
	}
}

func TestEnsure_RefusesForeignAliasedKeyWithoutAdopt(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled}
	client.aliases["alias/default-checkout-service-primary"] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error for an untagged pre-existing key alias without adopt:true")
	}
}

func TestEnsure_AdoptsUntaggedKeyWhenRequested(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-id"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-id", keyState: types.KeyStateEnabled, tags: map[string]string{"team": "someone-else"}}
	client.aliases["alias/default-checkout-service-primary"] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
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
	client.aliases["alias/default-checkout-service-primary"] = arn

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary", Adopt: true}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a key owned by a different AppDependencies CR")
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
	client.aliases["alias/default-checkout-service-primary"] = otherARN

	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: ourARN, State: depsv1alpha1.ManagedResourceStateTagPending, CreatedAt: metav1.Now()},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
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
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
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

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Errorf("expected scheduled deletion to be canceled, got KeyState %v", client.keys[arn].keyState)
	}
}

func TestEnsure_RejectsAliasExceedingKMSLimit(t *testing.T) {
	client := newFakeKMS()
	longKey := strings.Repeat("a", 250)
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: longKey}}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error for a computed alias exceeding the length limit")
	}
	if len(client.keys) != 0 {
		t.Error("expected no CreateKey call to have been made for an over-length alias")
	}
}

func TestEnsure_ClassifiesPermissionErrorsAsNotRetryable(t *testing.T) {
	client := newFakeKMS()
	client.createKeyErr = &fakeAWSError{code: "AccessDeniedException", fault: smithy.FaultClient}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected a permission-denied error to be classified as not retryable")
	}
}

func TestEnsureDedicatedKey_CreatesKeyUnderDerivedLedgerName(t *testing.T) {
	client := newFakeKMS()

	arn, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "orders", depsv1alpha1.DeletionPolicyDelete, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() error = %v", err)
	}
	if arn == "" {
		t.Fatal("expected a non-empty ARN")
	}

	entry := status.FindManagedResource(ledger, resourceType, "orders-key")
	if entry == nil {
		t.Fatal("expected a ledger entry named \"orders-key\", not \"orders\" - must never collide with a user's own kms.resources entry of the same base name")
	}
	if entry.ARN != arn {
		t.Errorf("ledger ARN = %q, want %q", entry.ARN, arn)
	}

	wantAlias := "alias/default-checkout-service-orders-key"
	if client.aliases[wantAlias] != arn {
		t.Errorf("expected alias %q to point at %q", wantAlias, arn)
	}
}

func TestEnsureDedicatedKey_ReusesExistingKeyOnSecondCall(t *testing.T) {
	client := newFakeKMS()

	arn1, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "orders", depsv1alpha1.DeletionPolicyDelete, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() first call error = %v", err)
	}
	arn2, _, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "orders", depsv1alpha1.DeletionPolicyDelete, ledger)
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

func TestEnsureDedicatedKey_UpdatesDeletionPolicyOnEachCall(t *testing.T) {
	client := newFakeKMS()

	_, ledger, err := EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "orders", depsv1alpha1.DeletionPolicyRetain, nil)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() error = %v", err)
	}
	_, ledger, err = EnsureDedicatedKey(context.Background(), client, "default", "checkout-service", "uid-1", "orders", depsv1alpha1.DeletionPolicyDelete, ledger)
	if err != nil {
		t.Fatalf("EnsureDedicatedKey() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, resourceType, "orders-key")
	if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
		t.Errorf("expected DeletionPolicy to track the owning resource's current policy, got %v", entry.DeletionPolicy)
	}
}

func TestEnsure_ContinuesToOtherKeysAfterOneFails(t *testing.T) {
	client := newFakeKMS()
	// The first entry fails at name-length validation (a local check, no
	// AWS call at all); the second is a normal key. Proves Ensure doesn't
	// abort the whole loop on the first failure.
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: strings.Repeat("a", 250)},
		{Name: "good-key"},
	}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error reported for the over-length key")
	}
	if len(client.keys) != 1 {
		t.Errorf("expected the second, valid key to still be created, got %d keys", len(client.keys))
	}
}
