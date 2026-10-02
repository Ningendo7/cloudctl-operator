//go:build integration

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

// Integration tests run against a real AWS SDK client pointed at a
// LocalStack container instead of this package's own hand-written fakes.
// The fakes only ever encode our own beliefs about how IAM's API behaves;
// these tests catch the case where that belief is simply wrong - in
// particular, trustPolicyEquivalent's assumption that GetRole returns
// AssumeRolePolicyDocument URL-encoded is read from AWS's docs, not
// previously exercised against a real (or real-shaped) API response
// anywhere in this codebase. Excluded from `go test ./...` by the
// "integration" build tag — see docs/testing.md for how to run these
// (LOCALSTACK_ENDPOINT, or `make test-integration`).
package iam

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

const (
	testOIDCProviderARN = "arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/TESTOIDC"
	testOIDCProviderURL = "oidc.eks.us-east-1.amazonaws.com/id/TESTOIDC"
)

// newIntegrationClient builds a real IAM client pointed at LocalStack.
// Deliberately never uses config.LoadDefaultConfig or picks up the
// environment's own AWS credentials/profile — an integration test must be
// structurally incapable of ever reaching real AWS by accident.
func newIntegrationClient(t *testing.T) *iam.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return iam.New(iam.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})
}

// uniqueSuffix keeps each test's CR name (and so its derived role name)
// distinct so parallel/repeated runs against the same long-lived LocalStack
// container never collide.
func uniqueSuffix(t *testing.T) string {
	return "test-" + t.Name()[len("TestIntegration_"):]
}

func deleteRoleIfExists(t *testing.T, client *iam.Client, name string) {
	t.Helper()
	ctx := context.Background()
	policies, err := client.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: &name})
	if err == nil {
		for _, p := range policies.PolicyNames {
			_, _ = client.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{RoleName: &name, PolicyName: &p})
		}
	}
	_, _ = client.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: &name})
}

// crWithOneOwnedQueue builds a minimal CR with one owned SQS resource
// already present in the ledger - enough for collectGrants to produce a
// non-empty grant set (an owned resource needs no AWS call of its own to
// be grantable; it just needs a ledger entry), without needing a real SQS
// queue for a test that's specifically about IAM's own API behavior.
func crWithOneOwnedQueue(namespace, crName, crUID string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName, UID: types.UID(crUID)},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:000000000000:" + namespace + "-" + crName + "-orders"},
			},
		},
	}
}

func TestIntegration_Ensure_CreatesRealRoleWithTrustPolicyAndTags(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	roleARN := ""
	t.Cleanup(func() {
		if roleARN != "" {
			deleteRoleIfExists(t, client, roleName(namespace, crName))
		}
	})

	ledger, arn, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	roleARN = arn
	if arn == "" {
		t.Fatal("expected a non-empty role ARN")
	}

	name := roleName(namespace, crName)
	getOut, err := client.GetRole(context.Background(), &iam.GetRoleInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real GetRole() error = %v", err)
	}
	if *getOut.Role.Arn != arn {
		t.Errorf("real role ARN = %q, want %q", *getOut.Role.Arn, arn)
	}

	tagsOut, err := client.ListRoleTags(context.Background(), &iam.ListRoleTagsInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real ListRoleTags() error = %v", err)
	}
	found := map[string]string{}
	for _, tag := range tagsOut.Tags {
		found[*tag.Key] = *tag.Value
	}
	if found["cloudctl.io/owner"] != namespace+"/"+crName {
		t.Errorf("owner tag = %q, want %q", found["cloudctl.io/owner"], namespace+"/"+crName)
	}

	policiesOut, err := client.ListRolePolicies(context.Background(), &iam.ListRolePoliciesInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real ListRolePolicies() error = %v", err)
	}
	if len(policiesOut.PolicyNames) != 1 {
		t.Fatalf("expected exactly one inline policy, got %v", policiesOut.PolicyNames)
	}

	entry := findEntry(ledger)
	if entry == nil || entry.ARN != arn {
		t.Errorf("expected ledger entry recording the role ARN, got %+v", ledger)
	}
}

func TestIntegration_Ensure_IsIdempotentAgainstRealAWS(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	t.Cleanup(func() { deleteRoleIfExists(t, client, roleName(namespace, crName)) })

	ledger, arn1, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	cr.Status.ManagedResources = ledger

	_, arn2, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if arn1 != arn2 {
		t.Errorf("expected the same role ARN both times, got %q then %q", arn1, arn2)
	}
}

// TestIntegration_Ensure_RefusesForeignRoleWithNoAdoptEscapeHatch is the
// IAM-specific version of every other resource type's naming-collision
// test - except here there is deliberately no adopt:true path at all
// (see ensureRole's own doc comment: the role name is entirely derived,
// never user-supplied, so finding it under different ownership can only
// mean a genuine collision, never a legitimate adoption target).
func TestIntegration_Ensure_RefusesForeignRoleWithNoAdoptEscapeHatch(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	// Create the role directly via the real SDK, with no ownership tags at
	// all - simulating a genuine foreign/pre-existing claim on this exact
	// derived name.
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	if _, err := client.CreateRole(context.Background(), &iam.CreateRoleInput{
		RoleName:                 &name,
		AssumeRolePolicyDocument: &trust,
	}); err != nil {
		t.Fatalf("real CreateRole() (setup) error = %v", err)
	}

	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	_, _, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err == nil {
		t.Fatal("expected Ensure() to refuse a foreign, untagged role with no adopt:true path")
	}
}

// TestIntegration_Ensure_CorrectsTrustPolicyDriftOnRealRole specifically
// exercises trustPolicyEquivalent's URL-encoding assumption against a real
// GetRole response, not a fake that only ever returns what we told it to.
func TestIntegration_Ensure_CorrectsTrustPolicyDriftOnRealRole(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	ledger, _, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	cr.Status.ManagedResources = ledger

	// Directly corrupt the trust policy via the real SDK, simulating
	// out-of-band drift (console edit, a different tool).
	driftedTrust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	if _, err := client.UpdateAssumeRolePolicy(context.Background(), &iam.UpdateAssumeRolePolicyInput{
		RoleName:       &name,
		PolicyDocument: &driftedTrust,
	}); err != nil {
		t.Fatalf("real UpdateAssumeRolePolicy() (setup) error = %v", err)
	}

	if _, _, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, ledger); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	getOut, err := client.GetRole(context.Background(), &iam.GetRoleInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real GetRole() error = %v", err)
	}
	if !trustPolicyEquivalent(*getOut.Role.AssumeRolePolicyDocument, mustBuildExpectedTrustPolicy(t, namespace, crName)) {
		t.Errorf("expected the drifted trust policy to be corrected back, got %s", *getOut.Role.AssumeRolePolicyDocument)
	}
}

func mustBuildExpectedTrustPolicy(t *testing.T, namespace, crName string) string {
	t.Helper()
	policy, err := buildTrustPolicy(testOIDCProviderARN, testOIDCProviderURL, namespace, crName)
	if err != nil {
		t.Fatalf("buildTrustPolicy() error = %v", err)
	}
	return policy
}

func TestIntegration_Cleanup_DeletesRealRoleAndItsInlinePolicy(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	ledger, _, err := Ensure(context.Background(), client, nil, testOIDCProviderARN, testOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	cr.Status.ManagedResources = ledger

	if _, err := Cleanup(context.Background(), client, nil, cr, ledger, true); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if _, err := client.GetRole(context.Background(), &iam.GetRoleInput{RoleName: &name}); err == nil {
		t.Error("expected the real role to be deleted, but GetRole succeeded")
	}
}

func findEntry(ledger []depsv1alpha1.ManagedResource) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Type == resourceType {
			return &ledger[i]
		}
	}
	return nil
}
