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

package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func setupTable(t *testing.T, client *fakeDynamoDB, namespace, crName, name string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	t.Helper()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: name, PartitionKey: "id", DeletionPolicy: deletionPolicy, Force: force},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, namespace, crName, "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
	// Move past Creating to Verified, same as a real second reconcile would.
	ledger, err = Ensure(context.Background(), client, nil, nil, namespace, crName, "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("setup second Ensure() error = %v", err)
	}
	return ledger
}

func findResult(results []CleanupResult, name string) *CleanupResult {
	for i := range results {
		if results[i].Name == name {
			return &results[i]
		}
	}
	return nil
}

func advancePastQuietWindow(t *testing.T, ledger []depsv1alpha1.ManagedResource, name string) {
	t.Helper()
	entry := status.FindManagedResource(ledger, "dynamodb", name)
	if entry == nil {
		t.Fatalf("test setup broken: no ledger entry named %q", name)
	}
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)
}

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyRetain, false)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected sessions to be reported Retained, got %+v", results)
	}
	if status.FindManagedResource(updated, "dynamodb", "sessions") == nil {
		t.Error("expected retained entry to stay in the ledger")
	}
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected Retain policy to leave the AWS table in place")
	}
}

func TestCleanup_TreatsUnsetDeletionPolicyAsRetain(t *testing.T) {
	// Defensive-coding verification: Cleanup checks "!= Delete", not
	// "== Retain", specifically so a DeletionPolicy that's somehow truly
	// unset (Go's zero value, bypassing the CRD's own default) still falls
	// to the safe side rather than an undefined one. Every other test
	// exercises an explicitly-set DeletionPolicyRetain, which never
	// actually proves this - this one constructs the ledger entry directly
	// with the field left at its zero value.
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:    "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status: types.TableStatusActive,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type: resourceType,
			Name: "sessions",
			ARN:  client.tables[tableName].arn,
			// DeletionPolicy deliberately left unset (zero value).
		},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected an unset DeletionPolicy to be treated as Retained, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected the table to survive - an unset DeletionPolicy must never be treated as Delete")
	}
	if status.FindManagedResource(updated, "dynamodb", "sessions") == nil {
		t.Error("expected the retained entry to stay in the ledger")
	}
}

func TestCleanup_RelinquishesOwnershipTagForRetainedResource(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyRetain, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")

	if !cloudctlaws.IsOwnedBy(client.tables[tableName].tags, "default", "checkout-service", "uid-1") {
		t.Fatal("test setup broken: expected the table to start out owned by us")
	}

	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if cloudctlaws.IsOwnedBy(client.tables[tableName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the ownership tag to be relinquished once a Retain resource is no longer declared")
	}
}

func TestCleanup_HoldsNewlyEligibleTableForQuietWindowBeforeDeleting(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected sessions to be held PendingDeletion on first encounter, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Fatal("expected the table to survive the first pass even though it's empty right now")
	}

	advancePastQuietWindow(t, updated, "sessions")

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, updated, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r != nil {
		t.Errorf("expected no pending result once actually deleted, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; stillExists {
		t.Error("expected the table to be deleted once the quiet window elapsed and it's confirmed empty")
	}
	if status.FindManagedResource(updated, "dynamodb", "sessions") != nil {
		t.Error("expected the ledger entry to be removed after deletion")
	}
}

func TestCleanup_BlocksDeletingNonEmptyTableWithoutForce(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName].itemCount = 5

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "sessions")

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected sessions to be PendingDeletion, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected the non-empty table to NOT be deleted")
	}
	if entry := status.FindManagedResource(updated, "dynamodb", "sessions"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected PendingDeletionSince to remain recorded")
	}
}

func TestCleanup_ForceDeletesNonEmptyTable(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, true)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName].itemCount = 5

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected force to override the non-empty guard, got %v", results)
	}
	if _, stillExists := client.tables[tableName]; stillExists {
		t.Error("expected force:true to delete the non-empty table immediately, skipping the quiet window entirely")
	}
}

func TestCleanup_RefusesDeletingUnverifiedOwnership(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName].tags = map[string]string{"team": "someone-else"}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected Cleanup to refuse deleting a table whose ownership tags no longer verify")
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected the table to NOT be deleted when ownership can't be re-verified")
	}
}

func TestCleanup_EscalatesToStuckAfterGracePeriod(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName].itemCount = 5

	longAgo := metav1.NewTime(time.Now().Add(-2 * PendingDeletionGracePeriod))
	entry := status.FindManagedResource(ledger, "dynamodb", "sessions")
	entry.PendingDeletionSince = &longAgo
	status.UpsertManagedResource(&ledger, *entry)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Errorf("expected sessions to escalate to StuckPendingDeletion, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected the stuck table to still NOT be force-deleted automatically")
	}
}

func TestCleanup_TreatsAlreadyDeletedTableAsSuccess(t *testing.T) {
	// Regression-shaped test carried over from a real bug found reviewing a
	// sibling project's S3 cleanup: without treating "already gone" as
	// success, retrying cleanup on a table a previous attempt had already
	// deleted (e.g. after a transient failure removing the finalizer
	// itself) would wrongly report ErrTableNotOwned-style failures for a
	// table that was, in fact, correctly cleaned up already.
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")

	advancePastQuietWindow(t, ledger, "sessions")
	// Simulate the table already having been deleted out from under us
	// (e.g. a previous Cleanup pass succeeded but the process crashed
	// before persisting the ledger update).
	delete(client.tables, tableName)

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected an already-gone table to be treated as already cleaned up, not a failure", err)
	}
	if status.FindManagedResource(updated, "dynamodb", "sessions") != nil {
		t.Error("expected the ledger entry to be removed for an already-gone table")
	}
}

func TestCleanup_ContinuesToOtherResourcesAfterOneFails(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Name: "orders", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
	ledger, err = Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("setup second Ensure() error = %v", err)
	}

	sessionsName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[sessionsName].tags = map[string]string{"team": "someone-else"}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected an error reported for the corrupted-ownership sessions table")
	}

	// orders is a fresh delete candidate, so this first pass only enters
	// its mandatory quiet window rather than deleting it outright - what
	// matters here is that it was processed at all despite sessions
	// failing first in the same loop.
	if entry := status.FindManagedResource(updated, "dynamodb", "orders"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected orders to still be processed into its own pending-deletion window despite sessions failing")
	}
	if status.FindManagedResource(updated, "dynamodb", "sessions") == nil {
		t.Error("expected the failed sessions entry to remain in the ledger for retry")
	}
}
