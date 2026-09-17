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
	"net/url"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const (
	testOIDCArn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
	testOIDCURL = "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
)

func ownedCR(name string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:default-" + name + "-orders"},
			},
		},
	}
}

func TestEnsure_ReturnsNoRoleWhenNothingNeedsIAM(t *testing.T) {
	client := newFakeIAM()
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "empty-cr", UID: "uid-1"}}

	ledger, arn, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if arn != "" {
		t.Errorf("expected no role ARN for a CR needing no IAM, got %q", arn)
	}
	if len(ledger) != 0 {
		t.Errorf("expected an unchanged empty ledger, got %+v", ledger)
	}
	if len(client.roles) != 0 {
		t.Error("expected no role to have been created")
	}
}

func TestEnsure_ErrorsWhenOIDCNotConfiguredButNeeded(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), "", "", cr, nil)
	if err == nil {
		t.Fatal("expected an error when OIDC config is missing but a role is actually needed")
	}
	if len(client.roles) != 0 {
		t.Error("expected no role creation attempt without OIDC config")
	}
}

func TestEnsure_CreatesRoleWithTrustPolicyAndPermissions(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")

	ledger, arn, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if arn == "" {
		t.Fatal("expected a role ARN")
	}

	name := roleName("default", "checkout-service")
	role, ok := client.roles[name]
	if !ok {
		t.Fatalf("expected role %q to exist", name)
	}
	if !cloudctlaws.IsOwnedBy(role.tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the role to be tagged as owned by this CR")
	}

	trust, err := url.QueryUnescape(role.trustPolicy)
	if err != nil {
		t.Fatalf("failed to decode stored trust policy: %v", err)
	}
	if !strings.Contains(trust, "system:serviceaccount:default:checkout-service") {
		t.Errorf("expected the trust policy to scope to the default ServiceAccount name (the CR name), got %s", trust)
	}

	policy, ok := role.policies[roleInlinePolicyName]
	if !ok {
		t.Fatal("expected an inline permissions policy to be attached")
	}
	if !strings.Contains(policy, "sqs:SendMessage") {
		t.Errorf("expected the owned queue's full access to be in the policy, got %s", policy)
	}

	entry := status.FindManagedResource(ledger, "iam", "role")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified || entry.ARN != arn {
		t.Errorf("expected a Verified iam/role ledger entry matching the returned ARN, got %+v", entry)
	}
}

func TestEnsure_UsesConfiguredServiceAccountName(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")
	cr.Spec.ServiceAccountName = "custom-sa"

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	role := client.roles[roleName("default", "checkout-service")]
	trust, _ := url.QueryUnescape(role.trustPolicy)
	if !strings.Contains(trust, "system:serviceaccount:default:custom-sa") {
		t.Errorf("expected the trust policy to use the configured ServiceAccount name, got %s", trust)
	}
}

func TestEnsure_IsIdempotentAndPreservesCreatedAt(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")

	ledger, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	firstEntry := status.FindManagedResource(ledger, "iam", "role")

	ledger, _, err = Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	secondEntry := status.FindManagedResource(ledger, "iam", "role")

	if !firstEntry.CreatedAt.Equal(&secondEntry.CreatedAt) {
		t.Errorf("expected CreatedAt to be preserved across reconciles, got %v then %v", firstEntry.CreatedAt, secondEntry.CreatedAt)
	}
	if len(client.roles) != 1 {
		t.Errorf("expected exactly one role to exist, not a duplicate, got %d", len(client.roles))
	}
}

func TestEnsure_CorrectsTrustPolicyDriftOnExistingRole(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")
	name := roleName("default", "checkout-service")
	client.roles[name] = &fakeRole{
		arn: "arn:aws:iam::123456789012:role/" + name,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
		trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Principal":{"Service":"ec2.amazonaws.com"}}]}`,
		policies:    map[string]string{},
	}

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	trust, _ := url.QueryUnescape(client.roles[name].trustPolicy)
	if !strings.Contains(trust, "AssumeRoleWithWebIdentity") {
		t.Errorf("expected the stale EC2 trust policy to be corrected to the IRSA one, got %s", trust)
	}
}

func TestEnsure_DoesNotUpdateTrustPolicyWhenAlreadyCorrect(t *testing.T) {
	// Regression test for the URL-encoding fix: GetRole returns the trust
	// policy URL-encoded (confirmed via AWS's own docs). Comparing that
	// directly against a freshly-built plain-JSON policy would always
	// mismatch and call UpdateAssumeRolePolicy on every single reconcile
	// regardless of whether anything actually changed.
	client := newFakeIAM()
	cr := ownedCR("checkout-service")
	saName := "checkout-service"
	trustPolicy, err := buildTrustPolicy(testOIDCArn, testOIDCURL, "default", saName)
	if err != nil {
		t.Fatalf("buildTrustPolicy() error = %v", err)
	}

	name := roleName("default", "checkout-service")
	client.roles[name] = &fakeRole{
		arn: "arn:aws:iam::123456789012:role/" + name,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
		// Stored URL-encoded, matching what a real GetRole call returns.
		trustPolicy: url.QueryEscape(trustPolicy),
		policies:    map[string]string{},
	}
	client.updateAssumeRolePolicyErr = &fakeAWSError{code: "ShouldNotBeCalled"}

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil); err != nil {
		t.Fatalf("Ensure() error = %v — expected no UpdateAssumeRolePolicy call when the trust policy already matches", err)
	}
}

func TestEnsure_RefusesRoleOwnedByDifferentCR(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR("checkout-service")
	name := roleName("default", "checkout-service")
	client.roles[name] = &fakeRole{
		arn:         "arn:aws:iam::123456789012:role/" + name,
		tags:        map[string]string{"team": "someone-else"},
		trustPolicy: "{}",
		policies:    map[string]string{},
	}

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error for a role name collision with a differently-owned role")
	}
}

func TestEnsure_ClassifiesPermissionErrorsAsNotRetryable(t *testing.T) {
	client := newFakeIAM()
	client.createRoleErr = &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected a permission-denied error to be classified as not retryable")
	}
}

func TestEnsure_ClassifiesTransientErrorsAsRetryable(t *testing.T) {
	client := newFakeIAM()
	client.createRoleErr = &fakeAWSError{code: "ServiceFailure", fault: smithy.FaultServer}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !cloudctlaws.IsRetryable(err) {
		t.Error("expected a server-fault error to be classified as retryable")
	}
}

// TestEnsure_ClassifiesConcurrentModificationAsRetryable exercises
// CreateRole's real documented ConcurrentModification error
// ("multiple requests to change this object were submitted
// simultaneously... wait and retry") end-to-end through Ensure, not just
// in isolation against IsRetryable directly - this is the one AWS error
// this session's error-classification audit added support for and it had
// never been exercised against the actual role-creation path.
func TestEnsure_ClassifiesConcurrentModificationAsRetryable(t *testing.T) {
	client := newFakeIAM()
	client.createRoleErr = &fakeAWSError{code: "ConcurrentModification", fault: smithy.FaultClient}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !cloudctlaws.IsRetryable(err) {
		t.Error("expected ConcurrentModification to be classified as retryable")
	}
}

// The following exercise real, documented CreateRole/PutRolePolicy errors
// that are client-fault by HTTP status and carry no special-cased
// retryable treatment - each should surface as a hard, non-retryable
// failure rather than being silently retried forever.

func TestEnsure_ClassifiesEntityAlreadyExistsAsNotRetryable(t *testing.T) {
	client := newFakeIAM()
	client.createRoleErr = &fakeAWSError{code: "EntityAlreadyExists", fault: smithy.FaultClient}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected EntityAlreadyExists to be classified as not retryable")
	}
}

func TestEnsure_ClassifiesLimitExceededAsNotRetryable(t *testing.T) {
	client := newFakeIAM()
	client.createRoleErr = &fakeAWSError{code: "LimitExceeded", fault: smithy.FaultClient}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected LimitExceeded to be classified as not retryable")
	}
}

func TestEnsure_ClassifiesInvalidInputAsNotRetryable(t *testing.T) {
	client := newFakeIAM()
	client.createRoleErr = &fakeAWSError{code: "InvalidInput", fault: smithy.FaultClient}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected InvalidInput to be classified as not retryable")
	}
}

func TestEnsure_ClassifiesMalformedPolicyDocumentAsNotRetryable(t *testing.T) {
	client := newFakeIAM()
	client.putRolePolicyErr = &fakeAWSError{code: "MalformedPolicyDocument", fault: smithy.FaultClient}
	cr := ownedCR("checkout-service")

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected MalformedPolicyDocument to be classified as not retryable")
	}
}

func TestEnsure_RejectsRoleNameExceedingIAMLimit(t *testing.T) {
	client := newFakeIAM()
	cr := ownedCR(strings.Repeat("a", 70))

	_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if err == nil {
		t.Fatal("expected an error for a computed role name exceeding the length limit")
	}
	if len(client.roles) != 0 {
		t.Error("expected no CreateRole call to have been made for an over-length name")
	}
}

func TestEnsure_GrantsConsumedResourceFromAnotherCR(t *testing.T) {
	client := newFakeIAM()
	producer := producerCR("default", "checkout-service", []depsv1alpha1.SharedWithEntry{
		{Namespace: "default", Name: "fulfillment-service", Access: depsv1alpha1.AccessLevelReadWrite},
	}, "arn:aws:sqs:us-east-1:123456789012:default-checkout-service-orders")

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service", UID: "uid-2"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	_, arn, err := Ensure(context.Background(), client, newFakeK8sClient(producer), testOIDCArn, testOIDCURL, consumer, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if arn == "" {
		t.Fatal("expected a role to be created for the consumer")
	}

	role := client.roles[roleName("default", "fulfillment-service")]
	policy := role.policies[roleInlinePolicyName]
	if !strings.Contains(policy, "arn:aws:sqs:us-east-1:123456789012:default-checkout-service-orders") {
		t.Errorf("expected the consumed queue's ARN in the consumer's policy, got %s", policy)
	}
	if !strings.Contains(policy, "sqs:SendMessage") {
		t.Errorf("expected ReadWrite access (SendMessage) granted, got %s", policy)
	}
}

func TestEnsure_ReportsSkippedConsumesButStillCreatesRoleForResolvedGrants(t *testing.T) {
	cr := ownedCR("checkout-service")
	cr.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{
		{Namespace: "default", Name: "nonexistent-producer", ResourceName: "orders"},
	}
	client := newFakeIAM()

	_, arn, err := Ensure(context.Background(), client, newFakeK8sClient(), testOIDCArn, testOIDCURL, cr, nil)
	if arn == "" {
		t.Fatal("expected the role to still be created for the resolved owned grant")
	}
	if err == nil {
		t.Fatal("expected an error surfacing the unresolvable consume reference")
	}
	if !strings.Contains(err.Error(), "nonexistent-producer") {
		t.Errorf("expected the error to name the unresolved reference, got %v", err)
	}
}
