//go:build live

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

// Live tests run against a real AWS account - no LocalStack, no
// Kubernetes cluster. The integration tier (iam_integration_test.go)
// catches a wrong belief about IAM's API shape against LocalStack's own
// simulation of it; this tier catches the case where LocalStack's
// simulation itself diverges from the real thing - and, for IAM
// specifically, measures a real-world behavior LocalStack can't simulate
// at all: the propagation lag between CreateRole returning success and
// the role actually becoming assumable. Skipped entirely unless real
// credentials resolve via the standard AWS credential chain, so
// `go test ./...` and CI never need them, and this can never run by
// accident. Every test creates its own uniquely-named real role and
// cleans it up via t.Cleanup, which runs even if the test body fails
// partway through. Run explicitly with whatever already authenticates
// your AWS CLI:
//
//	go test -tags=live ./internal/resources/iam/... -v
package iam

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

const (
	liveOIDCProviderARN = "arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/LIVETEST"
	liveOIDCProviderURL = "oidc.eks.us-east-1.amazonaws.com/id/LIVETEST"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with a
// genuine, harmless STS call (GetCallerIdentity needs no permissions
// beyond being a valid, authenticated principal). Returns the caller's own
// ARN alongside the config since TestLive_Ensure_MeasuresRolePropagationLag
// needs it to build a trust policy that's actually assumable by whoever is
// running this test, not a hardcoded identity from whenever it was written.
func skipUnlessLiveAWSCredentials(t *testing.T) (aws.Config, string) {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}
	return cfg, *identity.Arn
}

func newLiveClient(t *testing.T) (*iam.Client, string) {
	t.Helper()
	cfg, callerARN := skipUnlessLiveAWSCredentials(t)
	return iam.NewFromConfig(cfg), callerARN
}

// liveUniqueSuffix keeps each test's CR name (and so its derived role name)
// distinct so repeated runs against the same real account never collide,
// and never orphan a same-named role from a previous failed run's cleanup.
func liveUniqueSuffix(t *testing.T) string {
	return "live-" + t.Name()[len("TestLive_"):] + "-" + time.Now().UTC().Format("150405")
}

func findEntry(ledger []depsv1alpha1.ManagedResource) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Type == resourceType {
			return &ledger[i]
		}
	}
	return nil
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

func crWithOneOwnedQueue(namespace, crName, crUID string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName, UID: types.UID(crUID)},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:807906459006:" + namespace + "-" + crName + "-orders"},
			},
		},
	}
}

func TestLive_Ensure_CreatesRealRoleWithTrustPolicyAndTags(t *testing.T) {
	client, _ := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	ledger, arn, err := Ensure(ctx, client, nil, liveOIDCProviderARN, liveOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if arn == "" {
		t.Fatal("expected a non-empty role ARN")
	}

	getOut, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real GetRole() error = %v", err)
	}
	if *getOut.Role.Arn != arn {
		t.Errorf("real role ARN = %q, want %q", *getOut.Role.Arn, arn)
	}

	tagsOut, err := client.ListRoleTags(ctx, &iam.ListRoleTagsInput{RoleName: &name})
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

	policiesOut, err := client.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real ListRolePolicies() error = %v", err)
	}
	if len(policiesOut.PolicyNames) != 1 {
		t.Fatalf("expected exactly one real inline policy, got %v", policiesOut.PolicyNames)
	}

	entry := findEntry(ledger)
	if entry == nil || entry.ARN != arn {
		t.Errorf("expected ledger entry recording the role ARN, got %+v", ledger)
	}
}

func TestLive_Ensure_RefusesForeignRoleWithNoAdoptEscapeHatch(t *testing.T) {
	client, _ := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	if _, err := client.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 &name,
		AssumeRolePolicyDocument: &trust,
	}); err != nil {
		t.Fatalf("real CreateRole() (setup) error = %v", err)
	}

	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	_, _, err := Ensure(ctx, client, nil, liveOIDCProviderARN, liveOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err == nil {
		t.Fatal("expected Ensure() to refuse a real foreign, untagged role with no adopt:true path")
	}
}

func TestLive_Ensure_CorrectsTrustPolicyDriftOnRealRole(t *testing.T) {
	client, _ := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	ledger, _, err := Ensure(ctx, client, nil, liveOIDCProviderARN, liveOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	cr.Status.ManagedResources = ledger

	driftedTrust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	if _, err := client.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
		RoleName:       &name,
		PolicyDocument: &driftedTrust,
	}); err != nil {
		t.Fatalf("real UpdateAssumeRolePolicy() (setup) error = %v", err)
	}

	if _, _, err := Ensure(ctx, client, nil, liveOIDCProviderARN, liveOIDCProviderURL, cr, ledger); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	getOut, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: &name})
	if err != nil {
		t.Fatalf("real GetRole() error = %v", err)
	}
	expected, err := buildTrustPolicy(liveOIDCProviderARN, liveOIDCProviderURL, namespace, crName)
	if err != nil {
		t.Fatalf("buildTrustPolicy() error = %v", err)
	}
	if !policyDocumentEquivalent(*getOut.Role.AssumeRolePolicyDocument, expected) {
		t.Errorf("expected the drifted trust policy to be corrected back on real AWS, got %s", *getOut.Role.AssumeRolePolicyDocument)
	}
}

func TestLive_Cleanup_DeletesRealRoleAndItsInlinePolicy(t *testing.T) {
	client, _ := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	cr := crWithOneOwnedQueue(namespace, crName, "uid-1")
	name := roleName(namespace, crName)
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	ledger, _, err := Ensure(ctx, client, nil, liveOIDCProviderARN, liveOIDCProviderURL, cr, cr.Status.ManagedResources)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	cr.Status.ManagedResources = ledger

	if _, err := Cleanup(ctx, client, nil, cr, ledger, true); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if _, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: &name}); err == nil {
		t.Error("expected the real role to be deleted, but GetRole succeeded")
	}
}

// TestLive_Ensure_MeasuresRolePropagationLag empirically characterizes a
// gap that's been documented (docs/aws-assumptions.md) but never actually
// measured: a freshly created IAM role can take a few seconds to become
// universally assumable, invisible to DescribeRole/GetRole (both report
// the role as fully present immediately). Uses plain sts:AssumeRole
// against a self-trusting policy (the real caller identity, resolved at
// runtime - not hardcoded, so this works for whoever actually runs
// `make test-live`) rather than a full IRSA/OIDC round trip, since the
// underlying phenomenon being measured - global IAM replication delay
// after CreateRole - is the same regardless of which STS assumption path
// eventually exercises it, and a real OIDC-based assumption needs a live
// EKS cluster with a running pod, which this test tier deliberately
// doesn't stand up. Does not assert a hard bound - propagation timing is
// inherently variable - but fails if the role is never assumable at all
// within a generous ceiling, which would mean the trust policy itself (or
// this test's own assumptions about it) is wrong, not that propagation is
// merely slow.
func TestLive_Ensure_MeasuresRolePropagationLag(t *testing.T) {
	cfg, callerARN := skipUnlessLiveAWSCredentials(t)
	client := iam.NewFromConfig(cfg)
	ctx := context.Background()
	name := "cloudctl-live-propagation-" + time.Now().UTC().Format("150405")
	t.Cleanup(func() { deleteRoleIfExists(t, client, name) })

	trust := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":%q},"Action":"sts:AssumeRole"}]}`, callerARN)
	createOut, err := client.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 &name,
		AssumeRolePolicyDocument: &trust,
	})
	if err != nil {
		t.Fatalf("real CreateRole() error = %v", err)
	}
	roleARN := *createOut.Role.Arn
	createdAt := time.Now()

	stsClient := sts.NewFromConfig(cfg)
	const ceiling = 60 * time.Second
	deadline := createdAt.Add(ceiling)
	attempts := 0
	var firstSuccessAfter time.Duration
	var lastErr error

	for {
		attempts++
		_, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
			RoleArn:         &roleARN,
			RoleSessionName: aws.String("cloudctl-propagation-probe"),
		})
		if err == nil {
			firstSuccessAfter = time.Since(createdAt)
			break
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("role never became assumable within %s (%d attempts) - last error: %v", ceiling, attempts, lastErr)
		}
		time.Sleep(time.Second)
	}

	t.Logf("IAM role propagation: assumable after %s (%d attempt(s))", firstSuccessAfter, attempts)
	if attempts == 1 {
		t.Logf("note: succeeded on the very first attempt - this run didn't observe the gap, it just confirms one exists somewhere under %s", ceiling)
	}
}
