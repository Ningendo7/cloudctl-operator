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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
