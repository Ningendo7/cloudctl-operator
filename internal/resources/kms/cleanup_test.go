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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func ownedFakeKey(arn, keyID string) *fakeKey {
	return &fakeKey{
		arn: arn, keyID: keyID, keyState: types.KeyStateEnabled,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
}

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, CreatedAt: metav1.Now()},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 1 || results[0].Reason != CleanupReasonRetained {
		t.Fatalf("expected a Retained result, got %+v", results)
	}
	if status.FindManagedResource(updated, resourceType, "primary") == nil {
		t.Error("expected the ledger entry to remain for a retained resource")
	}
	if cloudctlaws.IsOwnedBy(client.keys[arn].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the ownership tag to be relinquished from a retained-but-no-longer-declared key")
	}
}

func TestCleanup_HoldsNewlyEligibleKeyForQuietWindowBeforeScheduling(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now()},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Error("expected no ScheduleKeyDeletion call on the very first pass a key becomes eligible")
	}
	if len(results) != 1 || results[0].Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected a PendingDeletion result, got %+v", results)
	}
	entry := status.FindManagedResource(updated, resourceType, "primary")
	if entry == nil || entry.PendingDeletionSince == nil {
		t.Fatal("expected PendingDeletionSince to be set")
	}
}

func TestCleanup_SchedulesDeletionAfterQuietWindowElapses(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	since := metav1.NewTime(time.Now().Add(-(deletionQuietWindow + time.Minute)))
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if client.keys[arn].keyState != types.KeyStatePendingDeletion {
		t.Errorf("expected ScheduleKeyDeletion to have been called, KeyState = %v", client.keys[arn].keyState)
	}
	if status.FindManagedResource(updated, resourceType, "primary") != nil {
		t.Error("expected the ledger entry to be removed once deletion is scheduled - AWS's own 30-day window takes over from here")
	}
}

func TestCleanup_SkipsAlreadyScheduledKey(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	k := ownedFakeKey(arn, "id-1")
	k.keyState = types.KeyStatePendingDeletion
	client.keys[arn] = k
	since := metav1.NewTime(time.Now().Add(-(deletionQuietWindow + time.Minute)))
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	// Still present - Cleanup only drops the ledger entry at the moment it
	// itself calls ScheduleKeyDeletion, not on every subsequent pass while
	// waiting for AWS's own window to elapse.
	if status.FindManagedResource(updated, resourceType, "primary") == nil {
		t.Error("expected the ledger entry to remain untouched for an already-scheduled key")
	}
}

func TestCleanup_TreatsAlreadyDeletedKeyAsSuccess(t *testing.T) {
	client := newFakeKMS()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: "arn:aws:kms:us-east-1:123456789012:key/gone", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now()},
	}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if status.FindManagedResource(updated, resourceType, "primary") != nil {
		t.Error("expected the ledger entry to be dropped for a key already gone in AWS")
	}
}

func TestCleanup_RefusesDeletingUnverifiedOwnership(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = &fakeKey{arn: arn, keyID: "id-1", keyState: types.KeyStateEnabled} // no ownership tags
	since := metav1.NewTime(time.Now().Add(-(deletionQuietWindow + time.Minute)))
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err == nil {
		t.Fatal("expected an error refusing to delete a key no longer verified as owned by this CR")
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Error("expected no ScheduleKeyDeletion call")
	}
	if status.FindManagedResource(updated, resourceType, "primary") == nil {
		t.Error("expected the ledger entry to be preserved, not silently dropped, on a real failure")
	}
}

func TestCleanup_ClearsPendingDeletionMarkerWhenRedeclared(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	since := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "primary", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	entry := status.FindManagedResource(updated, resourceType, "primary")
	if entry == nil {
		t.Fatal("expected the ledger entry to remain for a still-declared key")
	}
	if entry.PendingDeletionSince != nil {
		t.Error("expected PendingDeletionSince to be cleared once the key is declared again")
	}
}

func TestCleanup_ContinuesToOtherResourcesAfterOneFails(t *testing.T) {
	client := newFakeKMS()
	badARN := "arn:aws:kms:us-east-1:123456789012:key/bad"
	goodARN := "arn:aws:kms:us-east-1:123456789012:key/good"
	client.keys[badARN] = &fakeKey{arn: badARN, keyID: "bad", keyState: types.KeyStateEnabled} // unowned - will fail
	client.keys[goodARN] = ownedFakeKey(goodARN, "good")
	since := metav1.NewTime(time.Now().Add(-(deletionQuietWindow + time.Minute)))
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "bad-key", ARN: badARN, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
		{Type: resourceType, Name: "good-key", ARN: goodARN, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err == nil {
		t.Fatal("expected an error reported for the unowned key")
	}
	if client.keys[goodARN].keyState != types.KeyStatePendingDeletion {
		t.Error("expected the good key's deletion to still be scheduled despite the other one failing")
	}
	if status.FindManagedResource(updated, resourceType, "good-key") != nil {
		t.Error("expected the good key's ledger entry to be removed")
	}
	if status.FindManagedResource(updated, resourceType, "bad-key") == nil {
		t.Error("expected the bad key's ledger entry to be preserved after a real failure")
	}
}

func TestCleanup_TreatsADedicatedKeyStillNeededAsDeclared(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	since := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		// "orders-key" is never in spec.KMS.Resources - it's a dedicated
		// key another section (sqs) provisioned via EnsureDedicatedKey.
		{Type: resourceType, Name: "orders-key", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, []string{"orders-key"}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no cleanup results for a still-needed dedicated key, got %+v", results)
	}
	entry := status.FindManagedResource(updated, resourceType, "orders-key")
	if entry == nil {
		t.Fatal("expected the dedicated key's ledger entry to remain")
	}
	if entry.PendingDeletionSince != nil {
		t.Error("expected any in-progress pending-deletion marker to be cleared now that the dedicated key is needed again")
	}
	if client.keys[arn].keyState != types.KeyStateEnabled {
		t.Error("expected no ScheduleKeyDeletion call for a still-needed dedicated key")
	}
}

func TestCleanup_SchedulesADedicatedKeyNoLongerNeeded(t *testing.T) {
	client := newFakeKMS()
	arn := "arn:aws:kms:us-east-1:123456789012:key/id-1"
	client.keys[arn] = ownedFakeKey(arn, "id-1")
	since := metav1.NewTime(time.Now().Add(-(deletionQuietWindow + time.Minute)))
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "orders-key", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, CreatedAt: metav1.Now(), PendingDeletionSince: &since},
	}

	// dedicatedKeysStillNeeded no longer lists "orders-key" - the owning
	// SQS resource's encryption was turned off, or the resource itself
	// was removed from spec.
	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", nil, nil, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if client.keys[arn].keyState != types.KeyStatePendingDeletion {
		t.Error("expected ScheduleKeyDeletion to have been called for a dedicated key no longer needed")
	}
	if status.FindManagedResource(updated, resourceType, "orders-key") != nil {
		t.Error("expected the dedicated key's ledger entry to be removed once deletion is scheduled")
	}
}
