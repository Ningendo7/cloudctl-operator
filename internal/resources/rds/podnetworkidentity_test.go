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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/smithy-go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/serviceaccount"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// newProducerWithInstance builds a producer CR declaring one rds
// resource at the given name/subnet group - the fixture
// firstConsumedSubnetGroupName reads from.
func newProducerWithInstance(namespace, crName, resourceName, dbSubnetGroupName string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName},
		Spec: depsv1alpha1.AppDependenciesSpec{
			RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
				{Name: resourceName, DBSubnetGroupName: dbSubnetGroupName},
			}},
		},
	}
}

func getSecurityGroupPolicy(t *testing.T, k8sClient client.Client, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	policy := &unstructured.Unstructured{}
	policy.SetAPIVersion("vpcresources.k8s.aws/v1beta1")
	policy.SetKind("SecurityGroupPolicy")
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, policy); err != nil {
		t.Fatalf("getting SecurityGroupPolicy %s/%s: %v", namespace, name, err)
	}
	return policy
}

func TestEnsurePodNetworkIdentity_HappyPath(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	producer := newProducerWithInstance("default", "checkout-service", "orders-db", "prod-private-data-tier")
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer, sa).Build()
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err != nil {
		t.Fatalf("EnsurePodNetworkIdentity() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		t.Fatal("expected a published pod-network-identity ledger entry")
	}
	groupID := SecurityGroupIDFromARN(entry.ARN)
	group, ok := ec2Client.groups[groupID]
	if !ok {
		t.Fatal("expected a dedicated security group to have been created")
	}
	if group.vpcID != "vpc-prod-private-data-tier" {
		t.Errorf("security group VPC = %q, want the one resolved from the producer's subnet group", group.vpcID)
	}
	if !cloudctlaws.IsOwnedBy(group.tags, "fulfillment", "fulfillment-service", "uid-1") {
		t.Error("expected the pod security group to be tagged as owned by this CR")
	}

	var gotSA corev1.ServiceAccount
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "fulfillment", Name: "fulfillment-service"}, &gotSA); err != nil {
		t.Fatalf("getting ServiceAccount: %v", err)
	}
	if gotSA.Labels[serviceaccount.NetworkIdentityLabelKey] != "fulfillment-service" {
		t.Errorf("ServiceAccount label %s = %q, want %q", serviceaccount.NetworkIdentityLabelKey, gotSA.Labels[serviceaccount.NetworkIdentityLabelKey], "fulfillment-service")
	}

	policyName := cloudctlaws.ResourceName("fulfillment", "fulfillment-service", "rds-pod-sg-policy", "network-identity", 255)
	policy := getSecurityGroupPolicy(t, k8sClient, "fulfillment", policyName)
	selector, found, err := unstructured.NestedStringMap(policy.Object, "spec", "serviceAccountSelector", "matchLabels")
	if err != nil || !found {
		t.Fatalf("reading serviceAccountSelector.matchLabels: found=%v err=%v", found, err)
	}
	if selector[serviceaccount.NetworkIdentityLabelKey] != "fulfillment-service" {
		t.Errorf("SecurityGroupPolicy selector = %v, want it keyed on the ServiceAccount's own name", selector)
	}
	groupIDs, found, err := unstructured.NestedStringSlice(policy.Object, "spec", "securityGroups", "groupIds")
	if err != nil || !found {
		t.Fatalf("reading securityGroups.groupIds: found=%v err=%v", found, err)
	}
	if len(groupIDs) != 1 || groupIDs[0] != groupID {
		t.Errorf("SecurityGroupPolicy groupIds = %v, want [%s]", groupIDs, groupID)
	}
}

func TestEnsurePodNetworkIdentity_NoResolvedProducer_SkipsSilently(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build() // producer doesn't exist at all
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err != nil {
		t.Fatalf("expected no error for an unresolved forward reference, got %v", err)
	}
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) != nil {
		t.Error("expected no ledger entry to be published yet")
	}
	if len(ec2Client.groups) != 0 {
		t.Error("expected no security group to be created when nothing resolved")
	}
}

func TestEnsurePodNetworkIdentity_EmptyConsumes_SkipsSilently(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	_, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, nil, nil)
	if err != nil {
		t.Fatalf("expected no error for an empty consumes list, got %v", err)
	}
	if len(ec2Client.groups) != 0 {
		t.Error("expected no security group when nothing is being consumed")
	}
}

// TestEnsurePodNetworkIdentity_UsesFirstResolvedProducer proves a
// consumes list where the first entry's producer hasn't declared the
// referenced resource yet still resolves correctly using the second,
// rather than failing outright on the first miss.
func TestEnsurePodNetworkIdentity_UsesFirstResolvedProducer(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	// "checkout-service" exists but has no matching rds resource declared.
	unresolvedProducer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"}}
	resolvedProducer := newProducerWithInstance("default", "billing-service", "invoices-db", "prod-private-data-tier")
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(unresolvedProducer, resolvedProducer, sa).Build()
	consumes := []depsv1alpha1.ConsumeRef{
		{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		{Namespace: "default", Name: "billing-service", ResourceName: "invoices-db"},
	}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err != nil {
		t.Fatalf("EnsurePodNetworkIdentity() error = %v", err)
	}
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		t.Fatal("expected a published ledger entry using the second, resolved producer")
	}
}

func TestEnsurePodNetworkIdentity_IsIdempotent(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	producer := newProducerWithInstance("default", "checkout-service", "orders-db", "prod-private-data-tier")
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer, sa).Build()
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err != nil {
		t.Fatalf("first EnsurePodNetworkIdentity() error = %v", err)
	}
	firstARN := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName).ARN

	ledger, err = EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, ledger)
	if err != nil {
		t.Fatalf("second EnsurePodNetworkIdentity() error = %v", err)
	}
	secondARN := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName).ARN

	if firstARN != secondARN {
		t.Errorf("expected the same security group across reconciles, got %q then %q", firstARN, secondARN)
	}
	if len(ec2Client.groups) != 1 {
		t.Errorf("expected exactly one security group after two reconciles, got %d", len(ec2Client.groups))
	}
}

func TestEnsurePodNetworkIdentity_PropagatesVPCResolutionFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	rdsClient.describeDBSubnetGroupsErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	ec2Client := newFakeEC2()
	producer := newProducerWithInstance("default", "checkout-service", "orders-db", "prod-private-data-tier")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	_, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err == nil {
		t.Fatal("expected an error when VPC resolution fails")
	}
}

func TestEnsurePodNetworkIdentity_PropagatesSecurityGroupCreationFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	ec2Client.createSecurityGroupErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	producer := newProducerWithInstance("default", "checkout-service", "orders-db", "prod-private-data-tier")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	_, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err == nil {
		t.Fatal("expected an error when CreateSecurityGroup fails")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError for a throttling error, got %v", err)
	}
}

func TestEnsurePodNetworkIdentity_DoesNotPublishLedgerEntryOnFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	ec2Client.createSecurityGroupErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	producer := newProducerWithInstance("default", "checkout-service", "orders-db", "prod-private-data-tier")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	ledger, _ := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) != nil {
		t.Error("expected no ledger entry to be published when a dependent step failed")
	}
}

// TestEnsurePodNetworkIdentity_StaysInExistingVPC_WhenStillJustified proves
// resolution order flipping across reconciles (here, producerB simply
// sorts before producerA in consumes) must not move an already-created
// security group to a different VPC as long as some current consumes
// entry still justifies the VPC it's already in.
func TestEnsurePodNetworkIdentity_StaysInExistingVPC_WhenStillJustified(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	producerA := newProducerWithInstance("default", "billing-service", "invoices-db", "vpc-a-subnets")
	producerB := newProducerWithInstance("default", "checkout-service", "orders-db", "vpc-b-subnets")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producerA, producerB).Build()

	groupName := cloudctlaws.ResourceName("fulfillment", "fulfillment-service", "rds-pod-sg", "network-identity", 255)
	existing, err := ec2Client.CreateSecurityGroup(context.Background(), &ec2.CreateSecurityGroupInput{
		GroupName: &groupName, VpcId: aws.String("vpc-vpc-a-subnets"),
	})
	if err != nil {
		t.Fatalf("setup: creating pre-existing security group: %v", err)
	}
	existingLedger := []depsv1alpha1.ManagedResource{{
		Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName,
		ARN: "arn:aws:ec2:" + testRegion + ":" + testAccountID + ":security-group/" + *existing.GroupId,
	}}

	// Lists producerB (resolving to vpc-b) first - a naive from-scratch,
	// first-match resolution would pick vpc-b here, which is the VPC flip
	// the sticky check in EnsurePodNetworkIdentity must prevent.
	consumes := []depsv1alpha1.ConsumeRef{
		{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		{Namespace: "default", Name: "billing-service", ResourceName: "invoices-db"},
	}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, existingLedger)
	if err != nil {
		t.Fatalf("EnsurePodNetworkIdentity() error = %v", err)
	}

	if len(ec2Client.groups) != 1 {
		t.Fatalf("expected no new security group to be created, got %d groups", len(ec2Client.groups))
	}
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil || SecurityGroupIDFromARN(entry.ARN) != *existing.GroupId {
		t.Errorf("expected the ledger to keep pointing at the existing group %q, got %v", *existing.GroupId, entry)
	}
}

// TestEnsurePodNetworkIdentity_MigratesVPC_WhenNoLongerJustified proves the
// accepted other half of the tradeoff: once nothing in consumes resolves
// to the existing security group's VPC any more (a genuine change, not a
// transient resolution flip), a new group gets created in the newly
// resolved VPC rather than getting stuck forever on a stale one.
func TestEnsurePodNetworkIdentity_MigratesVPC_WhenNoLongerJustified(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	producerB := newProducerWithInstance("default", "checkout-service", "orders-db", "vpc-b-subnets")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producerB).Build()

	groupName := cloudctlaws.ResourceName("fulfillment", "fulfillment-service", "rds-pod-sg", "network-identity", 255)
	existing, err := ec2Client.CreateSecurityGroup(context.Background(), &ec2.CreateSecurityGroupInput{
		GroupName: &groupName, VpcId: aws.String("vpc-vpc-a-subnets"),
	})
	if err != nil {
		t.Fatalf("setup: creating pre-existing security group: %v", err)
	}
	existingLedger := []depsv1alpha1.ManagedResource{{
		Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName,
		ARN: "arn:aws:ec2:" + testRegion + ":" + testAccountID + ":security-group/" + *existing.GroupId,
	}}

	// producerA (the one that used to justify vpc-a) is gone from
	// consumes entirely now - nothing left resolves to vpc-a.
	consumes := []depsv1alpha1.ConsumeRef{
		{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
	}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, existingLedger)
	if err != nil {
		t.Fatalf("EnsurePodNetworkIdentity() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		t.Fatal("expected a published ledger entry")
	}
	newGroupID := SecurityGroupIDFromARN(entry.ARN)
	if newGroupID == *existing.GroupId {
		t.Error("expected migration to a new security group once the old VPC is no longer justified")
	}
	newGroup, ok := ec2Client.groups[newGroupID]
	if !ok || newGroup.vpcID != "vpc-vpc-b-subnets" {
		t.Errorf("expected the new group to live in the newly resolved VPC, got %+v", newGroup)
	}
	if _, stillThere := ec2Client.groups[*existing.GroupId]; !stillThere {
		t.Error("expected the old, now-orphaned group to be left alone, not deleted - that's this tradeoff's accepted cost")
	}
}

// TestEnsurePodNetworkIdentity_SkipsVPCRecheckWithinTrustWindow proves the
// sticky-VPC check itself is gated by the same trust window every other
// ownership re-verification already uses - a Verified, recently-checked
// entry must not call DescribeDBSubnetGroups again on every single pass.
func TestEnsurePodNetworkIdentity_SkipsVPCRecheckWithinTrustWindow(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()

	groupName := cloudctlaws.ResourceName("fulfillment", "fulfillment-service", "rds-pod-sg", "network-identity", 255)
	existing, err := ec2Client.CreateSecurityGroup(context.Background(), &ec2.CreateSecurityGroupInput{
		GroupName: &groupName, VpcId: aws.String("vpc-vpc-a-subnets"),
	})
	if err != nil {
		t.Fatalf("setup: creating pre-existing security group: %v", err)
	}
	recentlyVerified := metav1.Now()
	existingLedger := []depsv1alpha1.ManagedResource{{
		Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName,
		ARN: "arn:aws:ec2:" + testRegion + ":" + testAccountID + ":security-group/" + *existing.GroupId,
		State: depsv1alpha1.ManagedResourceStateVerified, LastVerifiedAt: &recentlyVerified,
	}}
	consumes := []depsv1alpha1.ConsumeRef{
		{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
	}
	// No producer CR exists for this consumes entry at all - if the sticky
	// check tried to re-verify, it couldn't resolve anything and the whole
	// call would bail out early (see NoResolvedProducer_SkipsSilently).
	// Succeeding here is itself proof the re-check never ran.
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, existingLedger)
	if err != nil {
		t.Fatalf("EnsurePodNetworkIdentity() error = %v", err)
	}
	if rdsClient.describeDBSubnetGroupsCalls != 0 {
		t.Errorf("expected no DescribeDBSubnetGroups call within the trust window, got %d", rdsClient.describeDBSubnetGroupsCalls)
	}
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil || SecurityGroupIDFromARN(entry.ARN) != *existing.GroupId {
		t.Errorf("expected the ledger to keep pointing at the existing group %q, got %v", *existing.GroupId, entry)
	}
	if entry.LastVerifiedAt == nil || !entry.LastVerifiedAt.Time.Equal(recentlyVerified.Time) {
		t.Error("expected LastVerifiedAt to be preserved, not refreshed, on a skipped pass")
	}
}

// TestEnsurePodNetworkIdentity_RechecksVPCOnceTrustWindowExpires is the
// other half: once the window has elapsed, the real check must still run.
func TestEnsurePodNetworkIdentity_RechecksVPCOnceTrustWindowExpires(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	producerA := newProducerWithInstance("default", "checkout-service", "orders-db", "vpc-a-subnets")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producerA).Build()

	groupName := cloudctlaws.ResourceName("fulfillment", "fulfillment-service", "rds-pod-sg", "network-identity", 255)
	existing, err := ec2Client.CreateSecurityGroup(context.Background(), &ec2.CreateSecurityGroupInput{
		GroupName: &groupName, VpcId: aws.String("vpc-vpc-a-subnets"),
	})
	if err != nil {
		t.Fatalf("setup: creating pre-existing security group: %v", err)
	}
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	existingLedger := []depsv1alpha1.ManagedResource{{
		Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName,
		ARN: "arn:aws:ec2:" + testRegion + ":" + testAccountID + ":security-group/" + *existing.GroupId,
		State: depsv1alpha1.ManagedResourceStateVerified, LastVerifiedAt: &stale,
	}}
	consumes := []depsv1alpha1.ConsumeRef{
		{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
	}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, existingLedger)
	if err != nil {
		t.Fatalf("EnsurePodNetworkIdentity() error = %v", err)
	}
	if rdsClient.describeDBSubnetGroupsCalls != 1 {
		t.Errorf("expected exactly 1 DescribeDBSubnetGroups call once the trust window expired, got %d", rdsClient.describeDBSubnetGroupsCalls)
	}
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil || entry.LastVerifiedAt == nil || !entry.LastVerifiedAt.Time.After(stale.Time) {
		t.Error("expected LastVerifiedAt to be refreshed after a real re-verification")
	}
}

// --- CleanupPodNetworkIdentity ---

func securityGroupPolicyExists(ctx context.Context, k8sClient client.Client, namespace, name string) bool {
	policy := &unstructured.Unstructured{}
	policy.SetAPIVersion("vpcresources.k8s.aws/v1beta1")
	policy.SetKind("SecurityGroupPolicy")
	return k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, policy) == nil
}

// setUpPodNetworkIdentity drives EnsurePodNetworkIdentity to a fully
// established state - the fixture every cleanup test tears down from.
func setUpPodNetworkIdentity(t *testing.T, rdsClient rdsAPI, ec2Client cloudctlaws.EC2Client, k8sClient client.Client) []depsv1alpha1.ManagedResource {
	t.Helper()
	producer := newProducerWithInstance("default", "checkout-service", "orders-db", "prod-private-data-tier")
	if err := k8sClient.Create(context.Background(), producer); err != nil {
		t.Fatalf("setup: creating producer: %v", err)
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	if err := k8sClient.Create(context.Background(), sa); err != nil {
		t.Fatalf("setup: creating ServiceAccount: %v", err)
	}
	consumes := []depsv1alpha1.ConsumeRef{{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"}}

	ledger, err := EnsurePodNetworkIdentity(context.Background(), rdsClient, ec2Client, k8sClient,
		"fulfillment", "fulfillment-service", "uid-1", "fulfillment-service", testRegion, testAccountID, consumes, nil)
	if err != nil {
		t.Fatalf("setup EnsurePodNetworkIdentity() error = %v", err)
	}
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) == nil {
		t.Fatal("setup: expected a published pod-network-identity ledger entry")
	}
	return ledger
}

func TestCleanupPodNetworkIdentity_NoEntryIsNoOp(t *testing.T) {
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	ledger, err := CleanupPodNetworkIdentity(context.Background(), ec2Client, k8sClient, "fulfillment", "fulfillment-service", "fulfillment-service", nil)
	if err != nil {
		t.Fatalf("CleanupPodNetworkIdentity() error = %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected an unchanged empty ledger, got %+v", ledger)
	}
	if ec2Client.deleteSecurityGroupCalls != 0 {
		t.Errorf("expected no DeleteSecurityGroup call when nothing is tracked, got %d", ec2Client.deleteSecurityGroupCalls)
	}
}

func TestCleanupPodNetworkIdentity_TearsDownEverything(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ledger := setUpPodNetworkIdentity(t, rdsClient, ec2Client, k8sClient)
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	groupID := SecurityGroupIDFromARN(entry.ARN)
	policyName := cloudctlaws.ResourceName("fulfillment", "fulfillment-service", "rds-pod-sg-policy", "network-identity", 255)

	if _, stillExists := ec2Client.groups[groupID]; !stillExists {
		t.Fatal("setup: expected the security group to exist before cleanup")
	}
	if !securityGroupPolicyExists(context.Background(), k8sClient, "fulfillment", policyName) {
		t.Fatal("setup: expected the SecurityGroupPolicy to exist before cleanup")
	}

	ledger, err := CleanupPodNetworkIdentity(context.Background(), ec2Client, k8sClient, "fulfillment", "fulfillment-service", "fulfillment-service", ledger)
	if err != nil {
		t.Fatalf("CleanupPodNetworkIdentity() error = %v", err)
	}

	if _, stillExists := ec2Client.groups[groupID]; stillExists {
		t.Error("expected the security group to be deleted")
	}
	if securityGroupPolicyExists(context.Background(), k8sClient, "fulfillment", policyName) {
		t.Error("expected the SecurityGroupPolicy to be deleted")
	}
	var sa corev1.ServiceAccount
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "fulfillment", Name: "fulfillment-service"}, &sa); err != nil {
		t.Fatalf("getting ServiceAccount: %v", err)
	}
	if _, stillLabeled := sa.Labels[serviceaccount.NetworkIdentityLabelKey]; stillLabeled {
		t.Error("expected the network-identity label to be released from the ServiceAccount")
	}
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) != nil {
		t.Error("expected the ledger entry to be removed")
	}
}

func TestCleanupPodNetworkIdentity_SecurityGroupAlreadyGone_TreatedAsSuccess(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ledger := setUpPodNetworkIdentity(t, rdsClient, ec2Client, k8sClient)
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	groupID := SecurityGroupIDFromARN(entry.ARN)
	delete(ec2Client.groups, groupID) // deleted out-of-band already

	ledger, err := CleanupPodNetworkIdentity(context.Background(), ec2Client, k8sClient, "fulfillment", "fulfillment-service", "fulfillment-service", ledger)
	if err != nil {
		t.Fatalf("expected an already-gone security group to be treated as success, got %v", err)
	}
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) != nil {
		t.Error("expected the ledger entry to still be removed")
	}
}

func TestCleanupPodNetworkIdentity_PropagatesDeleteSecurityGroupFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ledger := setUpPodNetworkIdentity(t, rdsClient, ec2Client, k8sClient)

	ec2Client.deleteSecurityGroupErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}
	ledger, err := CleanupPodNetworkIdentity(context.Background(), ec2Client, k8sClient, "fulfillment", "fulfillment-service", "fulfillment-service", ledger)
	if err == nil {
		t.Fatal("expected a real (non-NotFound) DeleteSecurityGroup failure to propagate")
	}
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) == nil {
		t.Error("expected the ledger entry to remain tracked after a failed cleanup, not silently dropped")
	}
}

func TestCleanupPodNetworkIdentity_PropagatesSecurityGroupPolicyDeleteFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	failingClient := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(newScheme(t)).Build(),
		interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*unstructured.Unstructured); ok {
					return errors.New("injected delete failure")
				}
				return c.Delete(ctx, obj, opts...)
			},
		},
	)
	ledger := setUpPodNetworkIdentity(t, rdsClient, ec2Client, failingClient)

	ledger, err := CleanupPodNetworkIdentity(context.Background(), ec2Client, failingClient, "fulfillment", "fulfillment-service", "fulfillment-service", ledger)
	if err == nil {
		t.Fatal("expected the SecurityGroupPolicy delete failure to propagate")
	}
	if status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName) == nil {
		t.Error("expected the ledger entry to remain tracked after a failed cleanup, not silently dropped")
	}
}

func TestCleanupPodNetworkIdentity_IsIdempotent(t *testing.T) {
	rdsClient := newFakeRDS()
	ec2Client := newFakeEC2()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ledger := setUpPodNetworkIdentity(t, rdsClient, ec2Client, k8sClient)

	ledger, err := CleanupPodNetworkIdentity(context.Background(), ec2Client, k8sClient, "fulfillment", "fulfillment-service", "fulfillment-service", ledger)
	if err != nil {
		t.Fatalf("first CleanupPodNetworkIdentity() error = %v", err)
	}
	callsBefore := ec2Client.deleteSecurityGroupCalls

	ledger, err = CleanupPodNetworkIdentity(context.Background(), ec2Client, k8sClient, "fulfillment", "fulfillment-service", "fulfillment-service", ledger)
	if err != nil {
		t.Fatalf("second CleanupPodNetworkIdentity() error = %v", err)
	}
	if ec2Client.deleteSecurityGroupCalls != callsBefore {
		t.Errorf("expected no further DeleteSecurityGroup calls once already cleaned up, got %d more", ec2Client.deleteSecurityGroupCalls-callsBefore)
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger to stay empty, got %+v", ledger)
	}
}

// --- PodNetworkIdentityARN ---

func TestPodNetworkIdentityARN_NotPublished(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{}
	arn, ok := PodNetworkIdentityARN(cr)
	if ok || arn != "" {
		t.Errorf("expected (\"\", false) for a CR with no ledger entries, got (%q, %v)", arn, ok)
	}
}

func TestPodNetworkIdentityARN_Published(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Status: depsv1alpha1.AppDependenciesStatus{
		ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName, ARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-abc"},
		},
	}}
	arn, ok := PodNetworkIdentityARN(cr)
	if !ok || arn != "arn:aws:ec2:us-east-1:123456789012:security-group/sg-abc" {
		t.Errorf("got (%q, %v), want the published ARN and true", arn, ok)
	}
}

func TestPodNetworkIdentityARN_IgnoresOtherLedgerEntries(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Status: depsv1alpha1.AppDependenciesStatus{
		ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: resourceType, Name: "orders-db", ARN: "arn:aws:rds:us-east-1:123456789012:db:orders-db"},
		},
	}}
	if _, ok := PodNetworkIdentityARN(cr); ok {
		t.Error("expected false when only unrelated ledger entries are present")
	}
}
