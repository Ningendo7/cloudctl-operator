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
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
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
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{
			Consumes: []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}},
		}},
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	secondARN, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "mysql", testRegion, testAccountID, false, sharedWith)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
	if err != nil {
		t.Fatalf("expected no error when the consumer CR doesn't exist yet, got %v", err)
	}
	if len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress) != 0 {
		t.Error("expected no ingress rule when the consumer CR doesn't exist")
	}
}

// A second pass against an already-granted consumer must not write: the
// duplicate grant would fail every pass and spend EC2's mutating-call quota.
func TestEnsureSecurityGroup_ReassertingIngressMakesNoWrite(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}

	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith); err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}
	if ec2Client.authorizeSecurityGroupIngressCalls != 1 {
		t.Fatalf("expected exactly one real Authorize call after the first pass, got %d", ec2Client.authorizeSecurityGroupIngressCalls)
	}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
	if err != nil {
		t.Fatalf("second EnsureSecurityGroup() error = %v", err)
	}
	if ec2Client.authorizeSecurityGroupIngressCalls != 1 || ec2Client.revokeSecurityGroupIngressCalls != 0 {
		t.Errorf("a pass with every rule in place made %d Authorize and %d Revoke calls in total, want 1 and 0",
			ec2Client.authorizeSecurityGroupIngressCalls, ec2Client.revokeSecurityGroupIngressCalls)
	}
	if len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress) != 1 {
		t.Error("expected still exactly one ingress rule, not a duplicate entry")
	}
}

func TestEnsureSecurityGroup_GrantsEveryMissingConsumerInOneCall(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	first := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	second := newConsumerWithPodIdentity("billing", "billing-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer2")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(first, second).Build()
	sharedWith := []depsv1alpha1.SharedWithEntry{
		{Namespace: "fulfillment", Name: "fulfillment-service"},
		{Namespace: "billing", Name: "billing-service"},
	}
	ensure := func() string {
		t.Helper()
		arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
			"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
		if err != nil {
			t.Fatalf("EnsureSecurityGroup() error = %v", err)
		}
		return arn
	}

	arn := ensure()
	if ec2Client.authorizeSecurityGroupIngressCalls != 1 || len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress) != 2 {
		t.Fatalf("first pass: %d Authorize calls and %d rules, want one call granting both",
			ec2Client.authorizeSecurityGroupIngressCalls, len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress))
	}

	// One rule removed by hand: only that one is granted again.
	group := ec2Client.groups[SecurityGroupIDFromARN(arn)]
	group.ingress = group.ingress[:1]
	missing := "sg-consumer2"
	if group.ingress[0].sourceGroupID == "sg-consumer2" {
		missing = "sg-consumer1"
	}
	ensure()
	if ec2Client.authorizeSecurityGroupIngressCalls != 2 || len(group.ingress) != 2 {
		t.Errorf("after a rule was removed by hand: %d Authorize calls and %d rules, want 2 and 2", ec2Client.authorizeSecurityGroupIngressCalls, len(group.ingress))
	}
	if !slices.ContainsFunc(group.ingress, func(r fakeIngressRule) bool { return r.sourceGroupID == missing }) {
		t.Errorf("expected %s granted again, got %+v", missing, group.ingress)
	}
}

func TestEnsureSecurityGroup_ADuplicateGrantFromARaceIsNotAnError(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	ec2Client.authorizeSecurityGroupIngressErr = &fakeAWSError{code: errCodeInvalidPermissionDuplicate, fault: smithy.FaultClient}
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()

	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false,
		[]depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}); err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
	if err == nil {
		t.Fatal("expected an error when CreateSecurityGroup fails")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for a throttling error, got %v", err)
	}
}

// EC2 reads lag its writes: a group created by an earlier pass can be missed
// by the lookup, and creating it again is refused as a duplicate.
func TestEnsureSecurityGroup_ADuplicateGroupIsRetriedNotRefused(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	ec2Client.createSecurityGroupErr = &fakeAWSError{code: errCodeInvalidGroupDuplicate, fault: smithy.FaultClient}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	_, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError, got %v", err)
	}

	ec2Client.createSecurityGroupErr = nil
	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil); err != nil {
		t.Fatalf("the next pass should succeed once the group is visible: %v", err)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, bothShared)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, onlyAnalytics)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	// sharedWith emptied entirely - this consumer (and everyone else) is revoked.
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	groupID := SecurityGroupIDFromARN(arn)
	ec2Client.groups[groupID].ingress = append(ec2Client.groups[groupID].ingress, fakeIngressRule{sourceGroupID: "sg-unrelated", fromPort: 9999, toPort: 9999})

	// Reconcile again with still-empty sharedWith - must not touch the
	// foreign, different-port rule.
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith); err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}
	if _, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith); err != nil {
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, oldShared)
	if err != nil {
		t.Fatalf("first EnsureSecurityGroup() error = %v", err)
	}

	newShared := []depsv1alpha1.SharedWithEntry{{Namespace: "analytics", Name: "analytics-service"}}
	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, newShared)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
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
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
	if err != nil {
		t.Fatalf("expected InvalidPermission.NotFound on revoke to be treated as success, got %v", err)
	}
}

func TestEnsureSecurityGroup_PropagatesRevokeFailure_WhenNotNotFound(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	groupID := SecurityGroupIDFromARN(arn)
	ec2Client.groups[groupID].ingress = append(ec2Client.groups[groupID].ingress, fakeIngressRule{sourceGroupID: "sg-stale", fromPort: 5432, toPort: 5432})
	ec2Client.revokeSecurityGroupIngressErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}

	_, err = EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, nil)
	if err == nil {
		t.Fatal("expected a real (non-NotFound) Revoke failure to surface as an error")
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

func existingGroupWithTags(t *testing.T, ec2Client *fakeEC2, tags map[string]string) string {
	t.Helper()
	name := cloudctlaws.ResourceName("default", "checkout-service", "rds-sg", "orders-db", 255)
	id := "sg-pre-existing"
	ec2Client.groups[id] = &fakeSecurityGroup{id: id, name: name, vpcID: "vpc-prod-private-data-tier", tags: tags}
	return id
}

func ensureGroup(t *testing.T, rdsClient *fakeRDS, ec2Client *fakeEC2, adopt bool) (string, error) {
	t.Helper()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	return EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, adopt, nil)
}

func TestEnsureSecurityGroup_RefusesExistingGroupItDoesNotOwn(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]string
	}{
		{name: "untagged", tags: map[string]string{}},
		{name: "owned by a different CR", tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "someone-else"),
			cloudctlaws.OwnerUIDTagKey: "uid-9",
		}},
		{name: "left by an earlier incarnation of this CR", tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-old",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdsClient, ec2Client := newFakeRDS(), newFakeEC2()
			id := existingGroupWithTags(t, ec2Client, tc.tags)

			if _, err := ensureGroup(t, rdsClient, ec2Client, false); err == nil {
				t.Fatal("expected a group this CR doesn't own to be refused")
			}
			if ec2Client.authorizeSecurityGroupIngressCalls+ec2Client.revokeSecurityGroupIngressCalls != 0 {
				t.Error("a refused group's rules must not be touched")
			}
			if len(ec2Client.groups[id].ingress) != 0 {
				t.Error("a refused group must be left exactly as found")
			}
		})
	}
}

func TestEnsureSecurityGroup_AdoptTakesUntaggedOrStaleGroupButNeverAnotherCRs(t *testing.T) {
	t.Run("untagged", func(t *testing.T) {
		rdsClient, ec2Client := newFakeRDS(), newFakeEC2()
		id := existingGroupWithTags(t, ec2Client, map[string]string{})
		if _, err := ensureGroup(t, rdsClient, ec2Client, true); err != nil {
			t.Fatalf("EnsureSecurityGroup() error = %v", err)
		}
		if !cloudctlaws.IsOwnedBy(ec2Client.groups[id].tags, "default", "checkout-service", "uid-1") {
			t.Errorf("expected the adopted group to be tagged as owned, got %v", ec2Client.groups[id].tags)
		}
	})

	t.Run("stale UID of the same CR", func(t *testing.T) {
		rdsClient, ec2Client := newFakeRDS(), newFakeEC2()
		id := existingGroupWithTags(t, ec2Client, map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-old",
		})
		if _, err := ensureGroup(t, rdsClient, ec2Client, true); err != nil {
			t.Fatalf("EnsureSecurityGroup() error = %v", err)
		}
		if !cloudctlaws.IsOwnedBy(ec2Client.groups[id].tags, "default", "checkout-service", "uid-1") {
			t.Errorf("expected the UID tag to be taken over, got %v", ec2Client.groups[id].tags)
		}
	})

	t.Run("another CR's group is never adopted", func(t *testing.T) {
		rdsClient, ec2Client := newFakeRDS(), newFakeEC2()
		id := existingGroupWithTags(t, ec2Client, map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "someone-else"),
			cloudctlaws.OwnerUIDTagKey: "uid-9",
		})
		if _, err := ensureGroup(t, rdsClient, ec2Client, true); err == nil {
			t.Fatal("expected adopt:true not to override a different CR's ownership")
		}
		if ec2Client.groups[id].tags[cloudctlaws.OwnerUIDTagKey] != "uid-9" {
			t.Error("the other CR's tags must be left alone")
		}
	})
}

// grantedGroups returns the consumer group IDs the instance's group admits.
func grantedGroups(ec2Client *fakeEC2, instanceGroupARN string) []string {
	rules := ec2Client.groups[SecurityGroupIDFromARN(instanceGroupARN)].ingress
	ids := make([]string, 0, len(rules))
	for _, rule := range rules {
		ids = append(ids, rule.sourceGroupID)
	}
	return ids
}

func ensureInstanceGroup(t *testing.T, ec2Client *fakeEC2, k8sClient client.Client, sharedWith ...depsv1alpha1.SharedWithEntry) string {
	t.Helper()
	arn, err := EnsureSecurityGroup(context.Background(), newFakeRDS(), ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false, sharedWith)
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	return arn
}

var fulfillmentShare = depsv1alpha1.SharedWithEntry{Namespace: "fulfillment", Name: "fulfillment-service"}

// A consumer that is being deleted, or has stopped listing the instance in
// rds.consumes, no longer needs the database. Without this its group stays
// referenced by a rule here, which is what stops the consumer from ever
// deleting that group.
func TestEnsureSecurityGroup_NoIngressForAConsumerBeingDeleted(t *testing.T) {
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	now := metav1.Now()
	consumer.DeletionTimestamp = &now
	consumer.Finalizers = []string{"deps.cloudctl.io/finalizer"}
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()

	arn := ensureInstanceGroup(t, ec2Client, k8sClient, fulfillmentShare)
	if got := grantedGroups(ec2Client, arn); len(got) != 0 {
		t.Errorf("granted %v to a consumer that is being deleted, want none", got)
	}
}

func TestEnsureSecurityGroup_NoIngressForAConsumerThatDoesNotConsumeThisInstance(t *testing.T) {
	cases := map[string][]depsv1alpha1.ConsumeRef{
		"no rds.consumes at all":            nil,
		"a different instance of the owner": {{Namespace: "default", Name: "checkout-service", ResourceName: "other-db"}},
		"a different owner":                 {{Namespace: "default", Name: "billing-service", ResourceName: "orders-db"}},
	}
	for name, consumes := range cases {
		t.Run(name, func(t *testing.T) {
			consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
			consumer.Spec.RDS.Consumes = consumes
			ec2Client := newFakeEC2()
			k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()

			arn := ensureInstanceGroup(t, ec2Client, k8sClient, fulfillmentShare)
			if got := grantedGroups(ec2Client, arn); len(got) != 0 {
				t.Errorf("granted %v to a consumer that never asked for this instance, want none", got)
			}
		})
	}
}

// The consumer marks its pod group as releasing before it deletes it, and
// the owner has to let go of it as soon as it sees that.
func TestEnsureSecurityGroup_RevokesIngressOnceTheConsumerIsReleasingItsGroup(t *testing.T) {
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()

	arn := ensureInstanceGroup(t, ec2Client, k8sClient, fulfillmentShare)
	if got := grantedGroups(ec2Client, arn); len(got) != 1 {
		t.Fatalf("setup: granted %v, want the consumer's group", got)
	}

	releasing := metav1.Now()
	consumer.Status.ManagedResources[0].PendingDeletionSince = &releasing
	if err := k8sClient.Update(context.Background(), consumer); err != nil {
		t.Fatalf("updating consumer: %v", err)
	}
	ensureInstanceGroup(t, ec2Client, k8sClient, fulfillmentShare)
	if got := grantedGroups(ec2Client, arn); len(got) != 0 {
		t.Errorf("still granting %v to a consumer that is releasing its group", got)
	}
}

func trackedInstanceGroup(t *testing.T, ec2Client *fakeEC2) ([]depsv1alpha1.ManagedResource, string) {
	t.Helper()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	arn := ensureInstanceGroup(t, ec2Client, k8sClient)
	return recordInstanceSecurityGroup(nil, "orders-db", arn), SecurityGroupIDFromARN(arn)
}

// An instance is only ever deleted by hand, and its security group used to
// stay behind: it counted against the VPC quota forever and its rules kept
// the consumers' groups from being deleted.
func TestCleanupInstanceSecurityGroups_DeletesTheGroupOfAnInstanceThatIsGone(t *testing.T) {
	ec2Client := newFakeEC2()
	ledger, groupID := trackedInstanceGroup(t, ec2Client)

	ledger, err := CleanupInstanceSecurityGroups(context.Background(), ec2Client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("CleanupInstanceSecurityGroups() error = %v", err)
	}
	if _, exists := ec2Client.groups[groupID]; exists {
		t.Error("expected the group of the removed instance to be deleted")
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger entry removed, got %+v", ledger)
	}
}

func TestCleanupInstanceSecurityGroups_KeepsTheGroupOfAnInstanceThatStillExists(t *testing.T) {
	declared := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "orders-db"}}}
	tracked := []depsv1alpha1.ManagedResource{{Type: resourceType, Name: "orders-db"}}
	cases := map[string]struct {
		spec     *depsv1alpha1.RDSSpec
		extra    []depsv1alpha1.ManagedResource
		deleting bool
	}{
		"still declared":                   {spec: declared},
		"retained, still in the ledger":    {spec: &depsv1alpha1.RDSSpec{}, extra: tracked},
		"deleting, but the instance lives": {spec: declared, extra: tracked, deleting: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ec2Client := newFakeEC2()
			ledger, groupID := trackedInstanceGroup(t, ec2Client)
			ledger = append(ledger, tc.extra...)

			ledger, err := CleanupInstanceSecurityGroups(context.Background(), ec2Client, "default", "checkout-service", "uid-1", tc.spec, ledger, tc.deleting)
			if err != nil {
				t.Fatalf("CleanupInstanceSecurityGroups() error = %v", err)
			}
			if _, exists := ec2Client.groups[groupID]; !exists {
				t.Error("deleted the security group of an instance that still exists")
			}
			if !status.HasManagedResource(ledger, podNetworkIdentityResourceType, instanceSecurityGroupLedgerName("orders-db")) {
				t.Error("dropped the ledger entry of an instance that still exists")
			}
		})
	}
}

func TestCleanupInstanceSecurityGroups_WaitsWhileAConsumersRuleStillNamesTheGroup(t *testing.T) {
	ec2Client := newFakeEC2()
	ledger, groupID := trackedInstanceGroup(t, ec2Client)
	ec2Client.groups["sg-other"] = &fakeSecurityGroup{id: "sg-other", name: "other", vpcID: "vpc-x",
		ingress: []fakeIngressRule{{sourceGroupID: groupID, fromPort: 5432, toPort: 5432}}}

	ledger, err := CleanupInstanceSecurityGroups(context.Background(), ec2Client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false)
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable error while another group still references it, got %v", err)
	}
	if !status.HasManagedResource(ledger, podNetworkIdentityResourceType, instanceSecurityGroupLedgerName("orders-db")) {
		t.Error("the entry must stay, or the group could never be found again")
	}
}

func TestCleanupInstanceSecurityGroups_NeverDeletesAGroupItNoLongerOwns(t *testing.T) {
	ec2Client := newFakeEC2()
	ledger, groupID := trackedInstanceGroup(t, ec2Client)
	ec2Client.groups[groupID].tags = ownerTags("someone", "else", "uid-9")

	ledger, err := CleanupInstanceSecurityGroups(context.Background(), ec2Client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("CleanupInstanceSecurityGroups() error = %v", err)
	}
	if _, exists := ec2Client.groups[groupID]; !exists {
		t.Error("deleted a security group owned by someone else")
	}
	if len(ledger) != 0 {
		t.Errorf("expected the stale entry dropped, got %+v", ledger)
	}
}

func TestCleanupInstanceSecurityGroups_AGroupThatIsAlreadyGoneJustDropsTheEntry(t *testing.T) {
	ec2Client := newFakeEC2()
	ledger, groupID := trackedInstanceGroup(t, ec2Client)
	delete(ec2Client.groups, groupID)

	ledger, err := CleanupInstanceSecurityGroups(context.Background(), ec2Client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false)
	if err != nil || len(ledger) != 0 {
		t.Errorf("got ledger=%+v err=%v, want an empty ledger and no error", ledger, err)
	}
}

func TestCleanupInstanceSecurityGroups_IgnoresThePodIdentityEntry(t *testing.T) {
	ec2Client := newFakeEC2()
	podEntry := depsv1alpha1.ManagedResource{Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName,
		ARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-pod"}

	ledger, err := CleanupInstanceSecurityGroups(context.Background(), ec2Client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, []depsv1alpha1.ManagedResource{podEntry}, true)
	if err != nil || len(ledger) != 1 || ec2Client.deleteSecurityGroupCalls != 0 {
		t.Errorf("touched the pod identity entry: ledger=%+v err=%v deletes=%d", ledger, err, ec2Client.deleteSecurityGroupCalls)
	}
}

// withFailingCRReadsSG wraps c so that reading any AppDependencies fails, as
// a cache or API-server hiccup would, once *failing is true.
func withFailingCRReadsSG(c client.WithWatch, failing *bool) client.Client {
	return interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isCR := obj.(*depsv1alpha1.AppDependencies); isCR && *failing {
				return errors.New("injected: API server unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
}

func TestEnsureSecurityGroup_KeepsAConsumersIngressWhenItCannotBeRead(t *testing.T) {
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	ec2Client := newFakeEC2()
	failing := false
	k8sClient := withFailingCRReadsSG(fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build(), &failing)
	arn := ensureInstanceGroup(t, ec2Client, k8sClient, fulfillmentShare)

	failing = true
	_, err := EnsureSecurityGroup(context.Background(), newFakeRDS(), ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false,
		[]depsv1alpha1.SharedWithEntry{fulfillmentShare})
	if err == nil {
		t.Fatal("expected an error, not a revoke, when the consumer can't be read")
	}
	if got := grantedGroups(ec2Client, arn); len(got) != 1 || ec2Client.revokeSecurityGroupIngressCalls != 0 {
		t.Errorf("revoked the consumer's database access because it couldn't be read: still granted %v", got)
	}
}

// Both security groups (the instance's and a consumer's pods') are named
// from the CR, so a restored CR meets the previous incarnation's groups.
func TestFindOrCreateSecurityGroup_TakesOverAGroupFromTheUIDItWasTold(t *testing.T) {
	for name, tc := range map[string]struct {
		from        string
		transferred bool
	}{"the previous UID": {"old-uid", true}, "a different UID": {"another-uid", false}, "no UID": {"", false}} {
		t.Run(name, func(t *testing.T) {
			ec2Client := newFakeEC2()
			created, err := ec2Client.CreateSecurityGroup(context.Background(), &ec2.CreateSecurityGroupInput{
				GroupName: aws.String("shared-name"), VpcId: aws.String("vpc-a"),
				TagSpecifications: []ec2types.TagSpecification{{
					ResourceType: ec2types.ResourceTypeSecurityGroup,
					Tags:         mapToEC2Tags(ownerTags("default", "checkout-service", "old-uid")),
				}},
			})
			if err != nil {
				t.Fatalf("setup: %v", err)
			}

			ctx := cloudctlaws.WithAdoptFromUID(context.Background(), tc.from)
			id, _, err := findOrCreateSecurityGroup(ctx, ec2Client, "default", "checkout-service", "new-uid", "shared-name", "vpc-a", false)
			tags := ec2Client.groups[*created.GroupId].tags
			if !tc.transferred {
				if err == nil || tags[cloudctlaws.OwnerUIDTagKey] != "old-uid" {
					t.Fatalf("expected a refusal that leaves the tags alone, got err=%v tags=%v", err, tags)
				}
				return
			}
			if err != nil || id != *created.GroupId {
				t.Fatalf("expected the existing group back, got %q, %v", id, err)
			}
			if tags[cloudctlaws.OwnerUIDTagKey] != "new-uid" {
				t.Errorf("expected the group re-tagged, got %v", tags)
			}
		})
	}
}

// AWS rejects a whole grant if the same pair appears twice in it, so a
// consumer listed twice must still be granted.
func TestEnsureSecurityGroup_AConsumerListedTwiceIsStillGranted(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	consumer := newConsumerWithPodIdentity("fulfillment", "fulfillment-service", "arn:aws:ec2:us-east-1:123456789012:security-group/sg-consumer1")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()
	entry := depsv1alpha1.SharedWithEntry{Namespace: "fulfillment", Name: "fulfillment-service"}

	arn, err := EnsureSecurityGroup(context.Background(), rdsClient, ec2Client, k8sClient,
		"default", "checkout-service", "uid-1", "orders-db", "prod-private-data-tier", "postgres", testRegion, testAccountID, false,
		[]depsv1alpha1.SharedWithEntry{entry, entry})
	if err != nil {
		t.Fatalf("EnsureSecurityGroup() error = %v", err)
	}
	if got := len(ec2Client.groups[SecurityGroupIDFromARN(arn)].ingress); got != 1 {
		t.Errorf("rules = %d, want the consumer granted once", got)
	}
}
