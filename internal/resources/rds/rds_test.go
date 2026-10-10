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

package rds

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/smithy-go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kmstest"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

// newAuthorizedSubnetGroupGrant builds a grant authorizing namespace to
// use dbSubnetGroupName - the fixture every test exercising a real
// Ensure() call (not specifically testing the authorization check itself)
// starts from, since Ensure() refuses to proceed at all otherwise.
func newAuthorizedSubnetGroupGrant(dbSubnetGroupName, namespace string) *depsv1alpha1.RDSSubnetGroupGrant {
	return &depsv1alpha1.RDSSubnetGroupGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "test-grant"},
		Spec: depsv1alpha1.RDSSubnetGroupGrantSpec{
			DBSubnetGroupName: dbSubnetGroupName,
			AllowedNamespaces: []string{namespace},
		},
	}
}

func baseInstanceSpec(name string) depsv1alpha1.RDSInstanceSpec {
	return depsv1alpha1.RDSInstanceSpec{
		Name:              name,
		DBSubnetGroupName: "prod-private-data-tier",
		Engine:            "postgres",
		EngineVersion:     "16.3",
		InstanceClass:     "db.t4g.micro",
		AllocatedStorage:  20,
	}
}

func TestEnsure_NilSpec_ReturnsLedgerUnchanged(t *testing.T) {
	client := newFakeRDS()
	ledger, err := Ensure(context.Background(), client, nil, nil, nil, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if ledger != nil {
		t.Errorf("expected a nil ledger unchanged, got %+v", ledger)
	}
}

func TestEnsure_CreatesNewInstance(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, resourceType, "orders-db")
	if entry == nil {
		t.Fatal("expected a ledger entry for the created instance")
	}
	if entry.State != depsv1alpha1.ManagedResourceStateCreating {
		t.Errorf("expected State Creating right after CreateDBInstance, got %v", entry.State)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	instance, ok := client.instances[instanceID]
	if !ok {
		t.Fatal("expected the instance to have been created")
	}
	if !cloudctlaws.IsOwnedBy(instance.tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the instance to be tagged as owned by this CR at creation")
	}
}

// TestEnsure_NeverSetsStaticMasterPassword guards the design invariant
// that AWS Secrets Manager - via RDS's own ManageMasterUserPassword
// feature - is the sole, permanent source of truth for this instance's
// credentials. This operator must never supply a literal
// MasterUserPassword of its own; doing so would make it a second,
// competing authority for credentials AWS is supposed to own and rotate
// entirely by itself.
func TestEnsure_NeverSetsStaticMasterPassword(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	in := client.lastCreateDBInstanceInput
	if in == nil {
		t.Fatal("expected CreateDBInstance to have been called")
	}
	if in.MasterUserPassword != nil {
		t.Errorf("expected MasterUserPassword to never be set, got %q - Secrets Manager must be the only source of truth for credentials", *in.MasterUserPassword)
	}
	if in.ManageMasterUserPassword == nil || !*in.ManageMasterUserPassword {
		t.Error("expected ManageMasterUserPassword=true so AWS itself owns provisioning and rotating the credentials")
	}
}

func TestEnsure_ReplicationRequested_ReturnsHardUnsupportedError(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}
	spec.Resources[0].Replication = &depsv1alpha1.RDSReplicationSpec{Enabled: true, Region: "us-west-2"}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error for a replication-enabled instance")
	}
	if !errors.Is(err, ErrReplicationNotSupported) {
		t.Errorf("expected errors.Is(err, ErrReplicationNotSupported), got %v", err)
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if errors.As(err, &reconcileErr) {
		t.Errorf("expected a hard, non-retryable error, got a ReconcileError with Retryable=%v", reconcileErr.Retryable)
	}
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	if _, created := client.instances[instanceID]; created {
		t.Error("expected no CreateDBInstance call for a replication-enabled instance - it should fail before touching AWS at all")
	}
}

func TestEnsure_ReplicationRequested_DoesNotBlockOtherInstances(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	replicated := baseInstanceSpec("orders-db")
	replicated.Replication = &depsv1alpha1.RDSReplicationSpec{Enabled: true, Region: "us-west-2"}
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{replicated, baseInstanceSpec("invoices-db")}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error from the replication-enabled instance")
	}
	if status.FindManagedResource(ledger, resourceType, "invoices-db") == nil {
		t.Error("expected the unaffected instance to still be created despite the other one's unsupported replication request")
	}
}

func TestEnsure_ReplicationDisabled_ProceedsNormally(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	spec.Replication = &depsv1alpha1.RDSReplicationSpec{Enabled: false}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012",
		&depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if status.FindManagedResource(ledger, resourceType, "orders-db") == nil {
		t.Error("expected the instance to be created normally when replication is declared but disabled")
	}
}

func TestEnsure_StillCreating_IsRetryable(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID].status = "creating"

	_, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil)
	if err == nil {
		t.Fatal("expected an error while the instance is still creating")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError, got %v", err)
	}
}

// TestEnsure_UnrecognizedStatus_DefaultsToRetryable is the one behavior
// deliberately the opposite of DynamoDB's own status handling: RDS's
// DBInstanceStatus is a free-form string with a much larger, less
// predictable set of non-terminal values than DynamoDB's four states, so
// an unrecognized one must never be silently treated as usable.
func TestEnsure_UnrecognizedStatus_DefaultsToRetryable(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID].status = "storage-optimization"

	_, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil)
	if err == nil {
		t.Fatal("expected an unrecognized status to be treated as not-yet-usable, not silently accepted")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for an unrecognized status, got %v", err)
	}
}

func TestEnsure_FailedStatus_IsHardError(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID].status = "failed"

	_, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil)
	if err == nil {
		t.Fatal("expected an error when the instance status is failed")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if errors.As(err, &reconcileErr) && reconcileErr.Retryable {
		t.Error("expected a failed instance to be a hard error, not retryable - it needs manual investigation")
	}
}

func TestEnsure_BecomingAvailable_EmitsAvailableEvent(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	var events []string
	recordEvent := func(eventType, reason, message string) { events = append(events, reason) }

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if !contains(events, "InstanceCreating") {
		t.Errorf("expected an InstanceCreating event, got %v", events)
	}

	events = nil
	_, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, recordEvent)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if !contains(events, "InstanceAvailable") {
		t.Errorf("expected an InstanceAvailable event once the instance transitions out of Creating, got %v", events)
	}
}

func TestEnsure_IsIdempotentAndPreservesCreatedAt(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	firstCreatedAt := status.FindManagedResource(ledger, resourceType, "orders-db").CreatedAt

	ledger, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(client.instances) != 1 {
		t.Fatalf("expected exactly one instance after re-reconciling, got %d", len(client.instances))
	}
	got := status.FindManagedResource(ledger, resourceType, "orders-db").CreatedAt
	if !got.Equal(&firstCreatedAt) {
		t.Errorf("expected CreatedAt to be preserved across reconciles, got %v want %v", got, firstCreatedAt)
	}
}

func TestEnsure_RefusesUnownedExistingInstance(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
		tags: map[string]string{"team": "someone-else"},
	}

	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}
	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error when a same-named instance exists without our ownership tag")
	}
}

// TestEnsure_RefusesUnownedInstanceStillProvisioning proves ownership is
// checked before the status gate, not after: a foreign instance that
// hasn't even finished creating yet must still be refused immediately,
// not reported as a retryable "still creating" - tags are set atomically
// at CreateDBInstance, so there's no reason to wait out the full
// provisioning window just to learn a problem that was never going away.
func TestEnsure_RefusesUnownedInstanceStillProvisioning(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "creating",
		tags: map[string]string{"team": "someone-else"},
	}

	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}
	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error for an unowned instance even while still provisioning")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if errors.As(err, &reconcileErr) && reconcileErr.Retryable {
		t.Errorf("expected a non-retryable ownership error, not the retryable still-provisioning one, got %v", err)
	}
}

func TestEnsure_AdoptsUntaggedInstance(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
	}

	spec := baseInstanceSpec("orders-db")
	spec.Adopt = true
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if entry := status.FindManagedResource(ledger, resourceType, "orders-db"); entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected a Verified ledger entry after adoption, got %+v", entry)
	}
	if !cloudctlaws.IsOwnedBy(client.instances[instanceID].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the instance to be tagged as owned by this CR after adoption")
	}
}

func TestEnsure_RefusesAdoptingInstanceOwnedByDifferentCR(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "some-other-cr"),
			cloudctlaws.OwnerUIDTagKey: "different-uid",
		},
	}

	spec := baseInstanceSpec("orders-db")
	spec.Adopt = true
	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err == nil {
		t.Fatal("expected adopt:true to never override an instance already owned by a different AppDependencies CR")
	}
}

// TestEnsure_RefusesAdoptingInstanceWithStaleUIDEvenWithAdoptTrue guards
// against the deleted-and-recreated-CR case: an instance tagged with this
// exact CR's own namespace/name, but a different UID, must never be
// silently re-adopted just because adopt:true is set.
func TestEnsure_RefusesAdoptingInstanceWithStaleUIDEvenWithAdoptTrue(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "old-uid",
		},
	}

	spec := baseInstanceSpec("orders-db")
	spec.Adopt = true
	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "new-uid", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err == nil {
		t.Fatal("expected adopt:true to never override an instance tagged with this CR's name but a stale (different) UID")
	}
}

func TestEnsure_SubnetGroupNotAuthorized_IsRetryable(t *testing.T) {
	client := newFakeRDS()
	// No RDSSubnetGroupGrant exists at all.
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error - no grant authorizes this namespace for this subnet group")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError (a grant may be created later), got %v", err)
	}
	if len(client.instances) != 0 {
		t.Error("expected no instance to be created when the subnet group isn't authorized")
	}
}

func TestEnsure_SubnetGroupGrantCoversDifferentNamespace_IsNotAuthorized(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "fulfillment")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error - the grant covers a different namespace")
	}
}

// TestEnsure_ListsSubnetGroupGrantsOnceAcrossMultipleResources guards
// against re-listing every RDSSubnetGroupGrant once per declared
// instance - the same grant set authorizes every instance in one pass,
// so it only needs fetching once.
func TestEnsure_ListsSubnetGroupGrantsOnceAcrossMultipleResources(t *testing.T) {
	rdsClient := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")

	var listCalls int
	k8sClient := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build(),
		interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*depsv1alpha1.RDSSubnetGroupGrantList); ok {
					listCalls++
				}
				return c.List(ctx, list, opts...)
			},
		},
	)
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
		baseInstanceSpec("orders-db"),
		baseInstanceSpec("invoices-db"),
	}}

	if _, err := Ensure(context.Background(), rdsClient, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if listCalls != 1 {
		t.Errorf("expected exactly 1 RDSSubnetGroupGrant list across 2 resources, got %d", listCalls)
	}
}

func TestEnsure_ProvisionsDedicatedKeyWhenEncryptionEnabled(t *testing.T) {
	client := newFakeRDS()
	kmsClient := kmstest.NewFakeKMSClient()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()

	spec := baseInstanceSpec("orders-db")
	spec.Encryption = &depsv1alpha1.EncryptionSpec{Enabled: true}

	ledger, err := Ensure(context.Background(), client, kmsClient, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", kms.DedicatedKeyLedgerName(resourceType, "orders-db"))
	if keyEntry == nil {
		t.Fatal("expected a dedicated KMS key ledger entry")
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	instance, ok := client.instances[instanceID]
	if !ok {
		t.Fatal("expected the instance to have been created")
	}
	if instance.kmsKeyARN != keyEntry.ARN {
		t.Errorf("instance kmsKeyARN = %q, want %q", instance.kmsKeyARN, keyEntry.ARN)
	}
}

func TestEnsure_KMSKeyRefRetriesWhenNotYetAuthorized(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()

	spec := baseInstanceSpec("orders-db")
	spec.Encryption = &depsv1alpha1.EncryptionSpec{
		KMSKeyRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"},
	}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error - the producer CR doesn't exist yet")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError (self-resolving forward reference), got %v", err)
	}
}

func TestEnsure_KMSKeyRefResolvesWhenAuthorized(t *testing.T) {
	client := newFakeRDS()
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
				{Name: "shared-key", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "default", Name: "checkout-service"},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "kms", Name: "shared-key", ARN: "arn:aws:kms:us-east-1:123456789012:key/shared-id"},
			},
		},
	}
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer, grant).Build()
	ec2Client := newFakeEC2()

	spec := baseInstanceSpec("orders-db")
	spec.Encryption = &depsv1alpha1.EncryptionSpec{
		KMSKeyRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"},
	}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	if client.instances[instanceID].kmsKeyARN != "arn:aws:kms:us-east-1:123456789012:key/shared-id" {
		t.Errorf("expected the instance to be encrypted with the shared key ARN, got %q", client.instances[instanceID].kmsKeyARN)
	}
	if status.FindManagedResource(ledger, resourceType, "orders-db") == nil {
		t.Fatal("expected a ledger entry for the instance")
	}
}

func TestEnsure_ContinuesToOtherInstancesAfterOneFails(t *testing.T) {
	client := newFakeRDS()
	// Only "prod-private-data-tier" is authorized - "orders-db" references
	// an unauthorized subnet group and should fail without blocking
	// "sessions-db".
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()

	failing := baseInstanceSpec("orders-db")
	failing.DBSubnetGroupName = "some-unauthorized-subnet-group"
	succeeding := baseInstanceSpec("sessions-db")

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012",
		&depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{failing, succeeding}}, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error from the failing instance")
	}
	if status.FindManagedResource(ledger, resourceType, "sessions-db") == nil {
		t.Error("expected the second instance to still be created despite the first one failing")
	}
	if status.FindManagedResource(ledger, resourceType, "orders-db") != nil {
		t.Error("expected no ledger entry for the unauthorized instance")
	}
}

func TestEnsure_HighAvailabilityAndBackupFlowThroughToCreateCall(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()

	spec := baseInstanceSpec("orders-db")
	spec.HighAvailability = &depsv1alpha1.RDSHighAvailabilitySpec{Enabled: true}
	spec.Backup = &depsv1alpha1.RDSBackupSpec{Enabled: true}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	instance := client.instances[instanceID]
	if !instance.multiAZ {
		t.Error("expected multiAZ to be true when highAvailability.enabled is true")
	}
	if instance.backupRetentionPeriod != defaultBackupRetentionDays {
		t.Errorf("backupRetentionPeriod = %d, want %d", instance.backupRetentionPeriod, defaultBackupRetentionDays)
	}
}

func TestEnsure_ClassifiesTransientAWSErrorsAsRetryable(t *testing.T) {
	client := newFakeRDS()
	client.describeDBInstancesErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error when the instance lookup fails")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for a throttling error, got %v", err)
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func TestEnsure_CreatesNewInstance_PassesThroughAllFields(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	instance := client.instances[instanceID]
	if instance.engine != "postgres" {
		t.Errorf("engine = %q, want postgres", instance.engine)
	}
	if instance.engineVersion != "16.3" {
		t.Errorf("engineVersion = %q, want 16.3", instance.engineVersion)
	}
	if instance.instanceClass != "db.t4g.micro" {
		t.Errorf("instanceClass = %q, want db.t4g.micro", instance.instanceClass)
	}
	if instance.dbSubnetGroupName != "prod-private-data-tier" {
		t.Errorf("dbSubnetGroupName = %q, want prod-private-data-tier", instance.dbSubnetGroupName)
	}
	if instance.masterUsername != masterUsername {
		t.Errorf("masterUsername = %q, want %q", instance.masterUsername, masterUsername)
	}
	if !instance.manageMasterPassword {
		t.Error("expected ManageMasterUserPassword to be true - the operator must never generate its own password")
	}
}

func TestEnsure_BackupDisabled_SetsRetentionPeriodToZero(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	spec.Backup = &depsv1alpha1.RDSBackupSpec{Enabled: false}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	if got := client.instances[instanceID].backupRetentionPeriod; got != 0 {
		t.Errorf("backupRetentionPeriod = %d, want 0 when backup is disabled", got)
	}
}

func TestEnsure_NoEncryption_LeavesInstanceUnencrypted(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	if got := client.instances[instanceID].kmsKeyARN; got != "" {
		t.Errorf("expected no KMS key when encryption isn't declared, got %q", got)
	}
}

// TestEnsure_WithinTrustWindow_SkipsReVerification proves a reconcile
// that finds an already-Verified, not-yet-stale ledger entry never calls
// ListTagsForResource again - the same property every other resource
// package's own trust-window test already proves, ported here since
// Ensure's own trust-window branch is identical in shape to DynamoDB's.
func TestEnsure_WithinTrustWindow_SkipsReVerification(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	// First pass only creates the instance (status Creating in our
	// ledger) - no tag check happens yet, same as real AWS, since
	// there's nothing to verify ownership of until it's usable.
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	// Second pass finds the (fake, immediately-available) instance and
	// the ledger entry still marked Creating, so NeedsRevalidation is
	// true - this is the pass that actually calls ListTagsForResource
	// and marks the entry Verified.
	ledger, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	callsAfterVerify := client.listTagsForResourceCalls
	if callsAfterVerify == 0 {
		t.Fatal("expected ListTagsForResource to be called once the instance needed its first real verification")
	}

	// Third pass is now within the trust window - should skip entirely.
	ledger, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("third Ensure() error = %v", err)
	}
	if client.listTagsForResourceCalls != callsAfterVerify {
		t.Errorf("expected ListTagsForResource not to be called again within the trust window, got %d more calls", client.listTagsForResourceCalls-callsAfterVerify)
	}

	// Backdating LastVerifiedAt past the trust window should make the
	// next reconcile re-verify for real.
	entry := status.FindManagedResource(ledger, resourceType, "orders-db")
	stale := metav1.NewTime(entry.LastVerifiedAt.Add(-2 * status.TrustWindow))
	entry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, *entry)

	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil); err != nil {
		t.Fatalf("fourth Ensure() error = %v", err)
	}
	if client.listTagsForResourceCalls <= callsAfterVerify {
		t.Error("expected ListTagsForResource to be called again once the trust window has expired")
	}
}

func TestEnsure_CreateDBInstanceFails_IsClassifiedCorrectly(t *testing.T) {
	client := newFakeRDS()
	client.createDBInstanceErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error when CreateDBInstance fails")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for a throttling error, got %v", err)
	}
}

func TestEnsure_AddTagsToResourceFails_DuringAdoption(t *testing.T) {
	client := newFakeRDS()
	client.addTagsToResourceErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
	}

	spec := baseInstanceSpec("orders-db")
	spec.Adopt = true
	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error when AddTagsToResource fails during adoption")
	}
}

func TestEnsure_ListTagsForResourceFails(t *testing.T) {
	client := newFakeRDS()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
	client.listTagsForResourceErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err == nil {
		t.Fatal("expected an error when ListTagsForResource fails - ownership can't be re-verified without it")
	}
}

func TestEnsure_AdoptingInstance_EmitsAdoptedEvent(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	client.instances[instanceID] = &fakeInstance{
		arn: "arn:aws:rds:us-east-1:123456789012:db:" + instanceID, status: "available",
	}

	var events []string
	recordEvent := func(eventType, reason, message string) { events = append(events, reason) }

	spec := baseInstanceSpec("orders-db")
	spec.Adopt = true
	_, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if !contains(events, "InstanceAdopted") {
		t.Errorf("expected an InstanceAdopted event, got %v", events)
	}
}

// --- Attribute drift-correction (ModifyDBInstance) ---

func TestEnsure_UpdatesInstanceClassWhenChanged(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}

	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	// User resizes the instance in spec.
	spec.InstanceClass = "db.t4g.small"
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if client.modifyDBInstanceCalls != 1 {
		t.Fatalf("expected exactly one ModifyDBInstance call, got %d", client.modifyDBInstanceCalls)
	}
	in := client.lastModifyDBInstanceInput
	if in.DBInstanceClass == nil || *in.DBInstanceClass != "db.t4g.small" {
		t.Errorf("expected DBInstanceClass=db.t4g.small in the Modify call, got %v", in.DBInstanceClass)
	}
	instanceID := cloudctlaws.ResourceName("default", "checkout-service", resourceType, "orders-db", 63)
	if client.instances[instanceID].instanceClass != "db.t4g.small" {
		t.Errorf("expected the fake instance's class to actually update, got %q", client.instances[instanceID].instanceClass)
	}
	entry := status.FindManagedResource(ledger, resourceType, "orders-db")
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the instance to still verify successfully after a modify, got state %s", entry.State)
	}
}

func TestEnsure_EnablesMultiAZWhenHighAvailabilityToggledOn(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.HighAvailability = &depsv1alpha1.RDSHighAvailabilitySpec{Enabled: true}
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	in := client.lastModifyDBInstanceInput
	if in == nil || in.MultiAZ == nil || !*in.MultiAZ {
		t.Errorf("expected MultiAZ=true in the Modify call, got %v", in)
	}
}

func TestEnsure_DisablesMultiAZWhenHighAvailabilityToggledOff(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	spec.HighAvailability = &depsv1alpha1.RDSHighAvailabilitySpec{Enabled: true}
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.HighAvailability = &depsv1alpha1.RDSHighAvailabilitySpec{Enabled: false}
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	in := client.lastModifyDBInstanceInput
	if in == nil || in.MultiAZ == nil || *in.MultiAZ {
		t.Errorf("expected MultiAZ=false in the Modify call, got %v", in)
	}
}

func TestEnsure_UpdatesBackupRetentionWhenBackupToggled(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.Backup = &depsv1alpha1.RDSBackupSpec{Enabled: true}
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	in := client.lastModifyDBInstanceInput
	if in == nil || in.BackupRetentionPeriod == nil || *in.BackupRetentionPeriod != defaultBackupRetentionDays {
		t.Errorf("expected BackupRetentionPeriod=%d in the Modify call, got %v", defaultBackupRetentionDays, in)
	}
}

func TestEnsure_NoModifyCallWhenNothingChanged(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if client.modifyDBInstanceCalls != 0 {
		t.Errorf("expected no ModifyDBInstance call when spec matches AWS already, got %d", client.modifyDBInstanceCalls)
	}
}

func TestEnsure_DoesNotCallModifyOnFreshCreate(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}

	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.modifyDBInstanceCalls != 0 {
		t.Errorf("expected no ModifyDBInstance call on the very first, create-only pass, got %d", client.modifyDBInstanceCalls)
	}
}

func TestEnsure_PropagatesModifyDBInstanceFailure(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.InstanceClass = "db.t4g.small"
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	client.modifyDBInstanceErr = &fakeAWSError{code: "ThrottlingException"}
	_, err = Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil)
	if err == nil {
		t.Fatal("expected ModifyDBInstance's failure to propagate")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for a throttling error, got %v", err)
	}
}

func TestEnsure_ModifiesMultipleAttributesInOneCall(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.InstanceClass = "db.t4g.small"
	spec.HighAvailability = &depsv1alpha1.RDSHighAvailabilitySpec{Enabled: true}
	spec.Backup = &depsv1alpha1.RDSBackupSpec{Enabled: true}
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if client.modifyDBInstanceCalls != 1 {
		t.Fatalf("expected all three changes to be sent in exactly one ModifyDBInstance call, got %d calls", client.modifyDBInstanceCalls)
	}
	in := client.lastModifyDBInstanceInput
	if in.DBInstanceClass == nil || *in.DBInstanceClass != "db.t4g.small" {
		t.Error("expected DBInstanceClass in the combined Modify call")
	}
	if in.MultiAZ == nil || !*in.MultiAZ {
		t.Error("expected MultiAZ in the combined Modify call")
	}
	if in.BackupRetentionPeriod == nil || *in.BackupRetentionPeriod != defaultBackupRetentionDays {
		t.Error("expected BackupRetentionPeriod in the combined Modify call")
	}
}

func TestEnsure_ModifyAppliesImmediately(t *testing.T) {
	client := newFakeRDS()
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	ec2Client := newFakeEC2()
	spec := baseInstanceSpec("orders-db")
	rdsSpec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	ledger, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.InstanceClass = "db.t4g.small"
	rdsSpec = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{spec}}
	if _, err := Ensure(context.Background(), client, nil, ec2Client, k8sClient, "default", "checkout-service", "uid-1", "us-east-1", "123456789012", rdsSpec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	in := client.lastModifyDBInstanceInput
	if in.ApplyImmediately == nil || !*in.ApplyImmediately {
		t.Error("expected ApplyImmediately=true so the operator's desired state takes effect promptly, not on AWS's own maintenance window")
	}
}
