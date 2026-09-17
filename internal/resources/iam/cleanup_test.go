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

package iam

import (
	"context"
	"testing"

	"github.com/aws/smithy-go"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func TestCleanup_NoOpWhenNoRoleWasEverCreated(t *testing.T) {
	client := newFakeIAM()
	client.deleteRoleErr = &fakeAWSError{code: "ShouldNotBeCalled"}
	cr := ownedCR("checkout-service")

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, nil, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected a no-op when no role was ever created", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected an unchanged empty ledger, got %+v", ledger)
	}
}

func roleLedger(arn string) []depsv1alpha1.ManagedResource {
	return []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: roleLedgerName, ARN: arn, State: depsv1alpha1.ManagedResourceStateVerified},
	}
}

func TestCleanup_KeepsRoleWhenStillNeeded(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:  arn,
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
	}
	client.deleteRoleErr = &fakeAWSError{code: "ShouldNotBeCalled"}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) == nil {
		t.Error("expected the role's ledger entry to remain - it's still needed")
	}
	if _, ok := client.roles[name]; !ok {
		t.Error("expected the role to still exist")
	}
}

func TestCleanup_DeletesRoleWhenNoLongerNeeded(t *testing.T) {
	client := newFakeIAM()
	// CR owns nothing anymore - grants will come back empty.
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:      arn,
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: string(cr.UID)},
		policies: map[string]string{roleInlinePolicyName: "{}"},
	}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) != nil {
		t.Error("expected the role's ledger entry to be removed")
	}
	if _, ok := client.roles[name]; ok {
		t.Error("expected the role to have been deleted")
	}
}

func TestCleanup_AlwaysDeletesOnCRDeletionRegardlessOfGrants(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service") // still "owns" a resource with a ledger entry
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:      arn,
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
		policies: map[string]string{roleInlinePolicyName: "{}"},
	}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) != nil {
		t.Error("expected the role's ledger entry to be removed on CR deletion, regardless of still-active grants")
	}
	if _, ok := client.roles[name]; ok {
		t.Error("expected the role to have been deleted on CR deletion")
	}
}

func TestCleanup_TreatsAlreadyGoneRoleAsSuccess(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	arn := "arn:aws:iam::123456789012:role/" + roleName("default", "checkout-service")
	// Role never registered in the fake at all - simulates it already
	// having been deleted out from under us.

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected an already-gone role to be treated as success", err)
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) != nil {
		t.Error("expected the ledger entry to be removed for an already-gone role")
	}
}

func TestCleanup_RefusesDeletingUnverifiedOwnership(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{arn: arn, tags: map[string]string{"team": "someone-else"}}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err == nil {
		t.Fatal("expected Cleanup to refuse deleting a role whose ownership tags no longer verify")
	}
	if _, ok := client.roles[name]; !ok {
		t.Error("expected the role to NOT be deleted when ownership can't be re-verified")
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) == nil {
		t.Error("expected the ledger entry to remain for retry")
	}
}

func TestCleanup_DeletesInlinePolicyAndRole(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:      arn,
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: string(cr.UID)},
		policies: map[string]string{roleInlinePolicyName: "{}"},
	}

	if _, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, ok := client.roles[name]; ok {
		t.Error("expected the role itself to be deleted")
	}
}

func TestCleanup_ReturnsMalformedARNErrorWithoutTouchingLedger(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	badLedger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: roleLedgerName, ARN: "not-a-real-arn", State: depsv1alpha1.ManagedResourceStateVerified},
	}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, badLedger, true)
	if err == nil {
		t.Fatal("expected an error for a malformed role ARN")
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) == nil {
		t.Error("expected the ledger entry to survive a parse failure, not be silently dropped")
	}
}

func TestCleanup_PropagatesGenericListRoleTagsFailureWithoutTouchingLedger(t *testing.T) {
	// A real failure re-verifying ownership (throttling, a permission
	// gap) must surface as an error and leave the ledger entry in place
	// for retry - not be confused with "the role is already gone" (which
	// specifically requires a NoSuchEntityException) and not silently
	// remove the entry, which would leak the still-existing role.
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{arn: arn, tags: map[string]string{}}
	client.listRoleTagsErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err == nil {
		t.Fatal("expected a transient ListRoleTags failure to be reported as an error")
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) == nil {
		t.Error("expected the ledger entry to survive a transient failure, not be silently dropped")
	}
	if _, ok := client.roles[name]; !ok {
		t.Error("expected the role to NOT be deleted when ownership couldn't even be checked")
	}
}

func TestCleanup_PropagatesGenericDeleteRolePolicyFailureWithoutTouchingLedger(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:      arn,
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: string(cr.UID)},
		policies: map[string]string{roleInlinePolicyName: "{}"},
	}
	client.deleteRolePolicyErr = &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err == nil {
		t.Fatal("expected a real DeleteRolePolicy failure to be reported as an error")
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) == nil {
		t.Error("expected the ledger entry to survive the failure, not be silently dropped")
	}
	if _, ok := client.roles[name]; !ok {
		t.Error("expected the role to NOT be deleted when its inline policy couldn't be removed")
	}
}

func TestCleanup_PropagatesGenericDeleteRoleFailureWithoutTouchingLedger(t *testing.T) {
	// The most important of these: if DeleteRole fails for a real reason
	// (e.g. the role still has other policies attached blocking deletion)
	// and this wrongly removed the ledger entry anyway, we'd permanently
	// lose track of a role still sitting in AWS - a real resource leak,
	// not just a retry delay.
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:      arn,
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: string(cr.UID)},
		policies: map[string]string{},
	}
	client.deleteRoleErr = &fakeAWSError{code: "DeleteConflict", fault: smithy.FaultClient}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err == nil {
		t.Fatal("expected a real DeleteRole failure to be reported as an error")
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) == nil {
		t.Error("expected the ledger entry to survive the failure - removing it here would leak the still-existing role")
	}
	if _, ok := client.roles[name]; !ok {
		t.Error("expected the fake role to still exist, matching the simulated real-world failure")
	}
}

func TestCleanup_TreatsAlreadyGoneInlinePolicyAsSuccess(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{}
	cr.Namespace, cr.Name = "default", "checkout-service"
	name := roleName("default", "checkout-service")
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn:      arn,
		tags:     map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: string(cr.UID)},
		policies: map[string]string{}, // inline policy already gone
	}

	ledger, err := Cleanup(context.Background(), client, newFakeK8sClient(), cr, roleLedger(arn), true)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected an already-gone inline policy to not block role deletion", err)
	}
	if _, ok := client.roles[name]; ok {
		t.Error("expected the role to still be deleted")
	}
	if status.FindManagedResource(ledger, resourceType, roleLedgerName) != nil {
		t.Error("expected the ledger entry to be removed")
	}
}
