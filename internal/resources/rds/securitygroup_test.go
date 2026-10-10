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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

// newConsumerWithPodIdentity builds a consuming CR that has already
// published its own dedicated pod security group - the fixture every
// ingress-granting test starts from, since EnsureSecurityGroup only ever
// reads this, never creates it (that's the consumer's own job, a later
// piece).
func newConsumerWithPodIdentity(namespace, crName, podSGARN string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName, ARN: podSGARN},
			},
		},
	}
}

func TestEnsureSecurityGroup_CreatesNewGroupInResolvedVPC(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}

	groupID := SecurityGroupIDFromARN(arn)
	group, ok := ec2Client.groups[groupID]
	if !ok {
		t.Fatal("expected a security group to have been created")
	}
	if group.vpcID != "vpc-prod-private-data-tier" {
		t.Errorf("security group created in VPC %q, want the one resolved from the subnet group", group.vpcID)
	}
	if !cloudctlaws.IsOwnedBy(group.tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the security group to be tagged as owned by this CR at creation")
	}
	wantName := cloudctlaws.ResourceName("default", "checkout-service", "rds-sg", "orders-db", 255)
	if group.name != wantName {
		t.Errorf("security group name = %q, want %q", group.name, wantName)
	}
}

func TestEnsureSecurityGroup_IsIdempotent_ReusesExistingGroup(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	firstARN, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	secondARN, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}

	if firstARN != secondARN {
		t.Errorf("expected the same security group ARN across reconciles, got %q then %q", firstARN, secondARN)
	}
	if len(ec2Client.groups) != 1 {
		t.Errorf("expected exactly one security group after two reconciles, got %d", len(ec2Client.groups))
	}
}

func TestEnsureSecurityGroup_GrantsIngressForAuthorizedConsumer(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}

	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 1 {
		t.Fatalf("expected exactly one ingress rule, got %d", len(group.ingress))
	}
	rule := group.ingress[0]
	if rule.sourceGroupID != "sg-consumer1" {
		t.Errorf("ingress rule source = %q, want sg-consumer1", rule.sourceGroupID)
	}
	if rule.fromPort != 5432 || rule.toPort != 5432 {
		t.Errorf("ingress rule port = %d-%d, want 5432-5432 for postgres", rule.fromPort, rule.toPort)
	}
}

func TestEnsureSecurityGroup_MySQLUsesPort3306(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "mysql", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 1 || group.ingress[0].fromPort != 3306 {
		t.Errorf("expected a port-3306 ingress rule for mysql, got %+v", group.ingress)
	}
}

// TestEnsureSecurityGroup_SkipsIngressForConsumerWithoutPublishedPodIdentity
// covers the forward-reference tolerance: the consumer CR exists, but
// hasn't reconciled its own pod identity yet (no ledger entry for it) -
// this must be skipped silently, not treated as an error, since it
// self-resolves on a later pass once the consumer's own reconcile runs.
func TestEnsureSecurityGroup_SkipsIngressForConsumerWithoutPublishedPodIdentity(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	// Consumer exists but has never published a pod security group.
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("expected no error for an unresolved forward reference, got %v", err)
	}
	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 0 {
		t.Errorf("expected no ingress rule yet, got %+v", group.ingress)
	}
}

func TestEnsureSecurityGroup_SkipsIngressForNonexistentConsumerCR(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build() // consumer CR doesn't exist at all
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("expected no error when the consumer CR doesn't exist yet, got %v", err)
	}
	if len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress) != 0 {
		t.Error("expected no ingress rule when the consumer CR doesn't exist")
	}
}

// TestEnsureSecurityGroup_ReassertingIngressIsIdempotent proves a second
// reconcile against an already-granted consumer doesn't surface
// InvalidPermission.Duplicate as an error - AuthorizeSecurityGroupIngress
// is expected to be called every pass, not tracked separately.
func TestEnsureSecurityGroup_ReassertingIngressIsIdempotent(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith); err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}
	if ec2Client.authorizeSecurityGroupIngressCalls != 1 {
		t.Fatalf("expected exactly one real Authorize call after the first pass, got %d", ec2Client.authorizeSecurityGroupIngressCalls)
	}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v (duplicate-permission errors must be swallowed, not surfaced)", err)
	}
	if ec2Client.authorizeSecurityGroupIngressCalls != 2 {
		t.Errorf("expected Authorize to be called again (and hit the duplicate case) on the second pass, got %d total calls", ec2Client.authorizeSecurityGroupIngressCalls)
	}
	if len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress) != 1 {
		t.Error("expected still exactly one ingress rule, not a duplicate entry")
	}
}

func TestEnsureSecurityGroup_MultipleConsumers_GrantsOnlyTheResolvedOne(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	resolvedConsumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	// "analytics" exists but hasn't published its pod identity yet.
	unresolvedConsumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "analytics", Name: "analytics-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(resolvedConsumer, unresolvedConsumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{
		{Namespace: "fulfillment", Name: "fulfillment-service"},
		{Namespace: "analytics", Name: "analytics-service"},
	}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 1 {
		t.Fatalf("expected exactly one ingress rule (only the resolved consumer), got %d", len(group.ingress))
	}
	if group.ingress[0].sourceGroupID != "sg-consumer1" {
		t.Errorf("expected the resolved consumer's security group, got %q", group.ingress[0].sourceGroupID)
	}
}

func TestEnsureSecurityGroup_PropagatesVPCResolutionFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	rdsClient.describeDBSubnetGroupsErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	_, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err == nil {
		t.Fatal("expected an error when VPC resolution fails")
	}
	if len(ec2Client.groups) != 0 {
		t.Error("expected no security group to be created when VPC resolution fails")
	}
}

func TestEnsureSecurityGroup_PropagatesCreateSecurityGroupFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	ec2Client.createSecurityGroupErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	_, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err == nil {
		t.Fatal("expected an error when CreateSecurityGroup fails")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for a throttling error, got %v", err)
	}
}

func TestEnsureSecurityGroup_PropagatesAuthorizeIngressFailure_WhenNotDuplicate(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	ec2Client.authorizeSecurityGroupIngressErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	_, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err == nil {
		t.Fatal("expected a real (non-duplicate) Authorize failure to surface as an error")
	}
}

// --- Ingress revocation ---

func TestEnsureSecurityGroup_RevokesIngressForRemovedConsumer(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer1 := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	consumer2 := newConsumerWithPodIdentity("analytics", "analytics-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer2")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer1, consumer2).Build()

	bothShared := []depsv1alpha1.SharedWithEntry{
		{Namespace: "fulfillment", Name: "fulfillment-service"},
		{Namespace: "analytics", Name: "analytics-service"},
	}
	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, bothShared)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}
	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 2 {
		t.Fatalf("expected both consumers granted, got %+v", group.ingress)
	}

	// fulfillment-service revoked from sharedWith - only analytics remains.
	onlyAnalytics := []depsv1alpha1.SharedWithEntry{{Namespace: "analytics", Name: "analytics-service"}}
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, onlyAnalytics)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}

	if len(group.ingress) != 1 || group.ingress[0].sourceGroupID != "sg-consumer2" {
		t.Fatalf("expected only sg-consumer2's ingress rule to remain, got %+v", group.ingress)
	}
	if len(ec2Client.revokedGroupIDs) != 1 || ec2Client.revokedGroupIDs[0] != "sg-consumer1" {
		t.Errorf("expected sg-consumer1 to have been revoked, got %v", ec2Client.revokedGroupIDs)
	}
}

func TestEnsureSecurityGroup_RevokesAllIngressWhenSharedWithEmptied(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	// sharedWith emptied entirely - this consumer (and everyone else) is revoked.
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}

	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 0 {
		t.Errorf("expected no ingress rules left, got %+v", group.ingress)
	}
}

// TestEnsureSecurityGroup_IgnoresRulesOnADifferentPort proves revocation
// stays scoped to the engine's own port - a rule on some other port
// (never one this package would create, but defensively left alone
// rather than assumed to be ours) must survive even if its source group
// isn't in the current desired set.
func TestEnsureSecurityGroup_IgnoresRulesOnADifferentPort(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	groupID := SecurityGroupIDFromARN(arn)
	ec2Client.groups[groupID].ingress = append(ec2Client.groups[groupID].ingress, fakeIngressRule{sourceGroupID: "sg-unrelated", fromPort: 9999, toPort: 9999})

	// Reconcile again with still-empty sharedWith - must not touch the
	// foreign, different-port rule.
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}

	group := ec2Client.groups[groupID]
	if len(group.ingress) != 1 || group.ingress[0].sourceGroupID != "sg-unrelated" {
		t.Errorf("expected the foreign, different-port rule to survive untouched, got %+v", group.ingress)
	}
	if len(ec2Client.revokedGroupIDs) != 0 {
		t.Errorf("expected no revocation of a rule on a different port, got %v", ec2Client.revokedGroupIDs)
	}
}

func TestEnsureSecurityGroup_NoRevokeCallsWhenNothingStale(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith); err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}
	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, sharedWith); err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}

	if ec2Client.revokeSecurityGroupIngressCalls != 0 {
		t.Errorf("expected zero Revoke calls when the desired set never shrank, got %d", ec2Client.revokeSecurityGroupIngressCalls)
	}
}

func TestEnsureSecurityGroup_SwapsConsumer_RevokesOldGrantsNewInOnePass(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	oldConsumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-old")
	newConsumer := newConsumerWithPodIdentity("analytics", "analytics-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-new")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(oldConsumer, newConsumer).Build()

	oldShared := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}
	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, oldShared)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	newShared := []depsv1alpha1.SharedWithEntry{{Namespace: "analytics", Name: "analytics-service"}}
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, newShared)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}

	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	if len(group.ingress) != 1 || group.ingress[0].sourceGroupID != "sg-new" {
		t.Fatalf("expected only sg-new to have access after the swap, got %+v", group.ingress)
	}
	if len(ec2Client.revokedGroupIDs) != 1 || ec2Client.revokedGroupIDs[0] != "sg-old" {
		t.Errorf("expected sg-old to have been revoked, got %v", ec2Client.revokedGroupIDs)
	}
}

func TestEnsureSecurityGroup_RevokeNotFound_TreatedAsSuccess(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	groupID := SecurityGroupIDFromARN(arn)
	// Seed a stale rule directly into the fake's described state without
	// it being removable via the normal path, simulating a rule that was
	// already revoked out-of-band (e.g. by a human) between reconciles -
	// DescribeSecurityGroups still reports it this one last time, but the
	// actual Revoke call races against its own removal.
	ec2Client.groups[groupID].ingress = append(ec2Client.groups[groupID].ingress, fakeIngressRule{sourceGroupID: "sg-ghost", fromPort: 5432, toPort: 5432})
	ec2Client.revokeSecurityGroupIngressErr = &fakeAWSError{code: "InvalidPermission.NotFound", fault: smithy.FaultClient}

	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("expected InvalidPermission.NotFound on revoke to be treated as success, got %v", err)
	}
}

func TestEnsureSecurityGroup_PropagatesRevokeFailure_WhenNotNotFound(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	groupID := SecurityGroupIDFromARN(arn)
	ec2Client.groups[groupID].ingress = append(ec2Client.groups[groupID].ingress, fakeIngressRule{sourceGroupID: "sg-stale", fromPort: 5432, toPort: 5432})
	ec2Client.revokeSecurityGroupIngressErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}

	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err == nil {
		t.Fatal("expected a real (non-NotFound) Revoke failure to surface as an error")
	}
}

func TestEnsureSecurityGroup_PropagatesDescribeFailureDuringRevokeCheck(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	_, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	ec2Client.describeSecurityGroupsByIDErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, nil)
	if err == nil {
		t.Fatal("expected the Describe failure (needed to check current rules before revoking) to propagate")
	}
}

func TestSecurityGroupIDFromARN(t *testing.T) {
	tests := []struct {
		arn  string
		want string
	}{
		{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-0123abcd", "sg-0123abcd"},
		{"sg-already-bare", "sg-already-bare"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := SecurityGroupIDFromARN(tt.arn); got != tt.want {
			t.Errorf("SecurityGroupIDFromARN(%q) = %q, want %q", tt.arn, got, tt.want)
		}
	}
}

func TestEnginePort(t *testing.T) {
	tests := []struct {
		engine string
		want   int32
	}{
		{"postgres", 5432},
		{"mysql", 3306},
		{"mariadb", 3306},
	}
	for _, tt := range tests {
		if got := enginePort(tt.engine); got != tt.want {
			t.Errorf("enginePort(%q) = %d, want %d", tt.engine, got, tt.want)
		}
	}
}
