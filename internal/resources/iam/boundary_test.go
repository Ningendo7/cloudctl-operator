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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

const (
	testBoundary      = "arn:aws:iam::123456789012:policy/cloudctl-boundary"
	testOtherBoundary = "arn:aws:iam::123456789012:policy/someone-elses"
)

var testSettings = RoleSettings{OIDCProviderARN: testOIDCArn, OIDCProviderURL: testOIDCURL, PermissionsBoundaryARN: testBoundary}

// existingRole puts checkout-service's role in the fake, tagged for uid, and
// returns a ledger that trusts it, as a pass inside the trust window sees it.
func existingRole(t *testing.T, client *fakeIAM, uid, boundary string, currentTrust bool) []depsv1alpha1.ManagedResource {
	t.Helper()
	name := roleName("default", "checkout-service")
	trust := `{"Version":"2012-10-17","Statement":[]}`
	if currentTrust {
		built, err := buildTrustPolicy(testOIDCArn, testOIDCURL, "default", "checkout-service")
		if err != nil {
			t.Fatal(err)
		}
		trust = built
	}
	arn := "arn:aws:iam::123456789012:role/" + name
	client.roles[name] = &fakeRole{
		arn: arn,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: uid,
		},
		trustPolicy: url.QueryEscape(trust),
		policies:    map[string]string{},
		boundary:    boundary,
	}
	now := metav1.Now()
	return []depsv1alpha1.ManagedResource{{
		Type: resourceType, Name: roleLedgerName, ARN: arn,
		State: depsv1alpha1.ManagedResourceStateVerified, LastVerifiedAt: &now,
	}}
}

func TestEnsure_CreatesTheRoleWithTheBoundaryAndPath(t *testing.T) {
	client := newFakeIAM()
	settings := testSettings
	settings.Path = "/cloudctl/"

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), settings, ownedCR("checkout-service"), nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	role := client.roles[roleName("default", "checkout-service")]
	if role == nil || role.boundary != testBoundary || role.path != "/cloudctl/" {
		t.Fatalf("created role = %+v, want boundary %s and path /cloudctl/", role, testBoundary)
	}
}

// Inside the trust window too: the boundary is a write, so the tags are read
// first.
func TestEnsure_AddsTheBoundaryToAnExistingRoleAfterCheckingItsTags(t *testing.T) {
	client := newFakeIAM()
	ledger := existingRole(t, client, "uid-1", "", true)

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testSettings, ownedCR("checkout-service"), ledger); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if got := client.roles[roleName("default", "checkout-service")].boundary; got != testBoundary {
		t.Errorf("boundary = %q, want %q", got, testBoundary)
	}
	if client.listRoleTagsCalls == 0 {
		t.Error("the boundary was written without reading the role's tags")
	}
}

func TestEnsure_LeavesABoundaryAlreadyOnTheRole(t *testing.T) {
	client := newFakeIAM()
	ledger := existingRole(t, client, "uid-1", testOtherBoundary, true)

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testSettings, ownedCR("checkout-service"), ledger); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.putBoundaryCalls != 0 {
		t.Errorf("PutRolePermissionsBoundary called %d times, want none", client.putBoundaryCalls)
	}
	if got := client.roles[roleName("default", "checkout-service")].boundary; got != testOtherBoundary {
		t.Errorf("boundary = %q, want it left as %q", got, testOtherBoundary)
	}
}

func TestEnsure_WithoutABoundaryFlagRolesAreLeftWithout(t *testing.T) {
	client := newFakeIAM()
	ledger := existingRole(t, client, "uid-1", "", true)
	settings := testSettings
	settings.PermissionsBoundaryARN = ""

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), settings, ownedCR("checkout-service"), ledger); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if client.putBoundaryCalls != 0 || client.listRoleTagsCalls != 0 {
		t.Errorf("PutRolePermissionsBoundary %d, ListRoleTags %d; want neither inside the trust window with nothing to write",
			client.putBoundaryCalls, client.listRoleTagsCalls)
	}
}

// A role replaced by someone else since it was last verified must not get
// its trust policy rewritten, or this CR's pods could assume it.
func TestEnsure_TrustPolicyDriftInsideTheTrustWindowRechecksOwnership(t *testing.T) {
	for name, tc := range map[string]struct {
		uid         string
		wantErr     bool
		wantUpdates int
	}{
		"still ours": {uid: "uid-1", wantUpdates: 1},
		"replaced":   {uid: "someone-else", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeIAM()
			ledger := existingRole(t, client, tc.uid, testBoundary, false)

			_, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testSettings, ownedCR("checkout-service"), ledger)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Ensure() error = %v, want error %v", err, tc.wantErr)
			}
			if client.updateTrustCalls != tc.wantUpdates {
				t.Errorf("UpdateAssumeRolePolicy called %d times, want %d", client.updateTrustCalls, tc.wantUpdates)
			}
			if client.listRoleTagsCalls == 0 {
				t.Error("the trust policy was handled without reading the role's tags")
			}
		})
	}
}

func TestEnsure_ARoleReplacedBySomeoneElseDoesNotGetTheBoundary(t *testing.T) {
	client := newFakeIAM()
	ledger := existingRole(t, client, "someone-else", "", true)

	if _, _, err := Ensure(context.Background(), client, newFakeK8sClient(), testSettings, ownedCR("checkout-service"), ledger); err == nil {
		t.Fatal("Ensure() succeeded on a role tagged for another CR")
	}
	if client.putBoundaryCalls != 0 {
		t.Errorf("PutRolePermissionsBoundary called %d times on a foreign role", client.putBoundaryCalls)
	}
}
