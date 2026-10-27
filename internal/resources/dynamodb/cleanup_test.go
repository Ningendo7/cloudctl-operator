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
	ledger, err := Ensure(context.Background(), client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
	// Move past Creating to Verified, same as a real second reconcile would.
	ledger, err = Ensure(context.Background(), client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
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
	entry, entryFound := status.FindManagedResource(ledger, "dynamodb", name)
	if !entryFound {
		t.Fatalf("test setup broken: no ledger entry named %q", name)
	}
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, entry)
}

// advancePastEmptyCheckBackoffStart pushes PendingDeletionSince back far
// enough that status.NeedsEmptyCheck's backoff window has started.
func advancePastEmptyCheckBackoffStart(t *testing.T, ledger []depsv1alpha1.ManagedResource, name string) {
	t.Helper()
	entry, entryFound := status.FindManagedResource(ledger, "dynamodb", name)
	if !entryFound {
		t.Fatalf("test setup broken: no ledger entry named %q", name)
	}
	past := metav1.NewTime(time.Now().Add(-status.EmptyCheckBackoffStartAfter - time.Minute))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, entry)
}

// pushLastEmptyCheckIntoThePast backdates a ledger entry's LastEmptyCheckAt
// by d, so a subsequent Cleanup() call sees the backoff interval as elapsed.
func pushLastEmptyCheckIntoThePast(t *testing.T, ledger []depsv1alpha1.ManagedResource, name string, d time.Duration) {
	t.Helper()
	entry, entryFound := status.FindManagedResource(ledger, "dynamodb", name)
	if !entryFound {
		t.Fatalf("test setup broken: no ledger entry named %q", name)
	}
	past := metav1.NewTime(time.Now().Add(-d))
	entry.LastEmptyCheckAt = &past
	status.UpsertManagedResource(&ledger, entry)
}

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyRetain, false)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected sessions to be reported Retained, got %+v", results)
	}
	if status.HasManagedResource(updated, "dynamodb", "sessions") {
		t.Error("expected a retained entry that pins no key to leave the ledger")
	}
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
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
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
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

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected an unset DeletionPolicy to be treated as Retained, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected the table to survive - an unset DeletionPolicy must never be treated as Delete")
	}
	if status.HasManagedResource(updated, "dynamodb", "sessions") {
		t.Error("expected a retained entry that pins no key to leave the ledger")
	}
}

func TestCleanup_RelinquishesOwnershipTagForRetainedResource(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyRetain, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)

	if !cloudctlaws.IsOwnedBy(client.tables[tableName].tags, "default", "checkout-service", "uid-1") {
		t.Fatal("test setup broken: expected the table to start out owned by us")
	}

	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if cloudctlaws.IsOwnedBy(client.tables[tableName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the ownership tag to be relinquished once a Retain resource is no longer declared")
	}
}

func TestCleanup_HoldsNewlyEligibleTableForQuietWindowBeforeDeleting(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
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

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, updated, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r != nil {
		t.Errorf("expected no pending result once actually deleted, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; stillExists {
		t.Error("expected the table to be deleted once the quiet window elapsed and it's confirmed empty")
	}
	if status.HasManagedResource(updated, "dynamodb", "sessions") {
		t.Error("expected the ledger entry to be removed after deletion")
	}
}

func TestCleanup_BlocksDeletingNonEmptyTableWithoutForce(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "sessions")

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected sessions to be PendingDeletion, got %+v", results)
	}
	if _, stillExists := client.tables[tableName]; !stillExists {
		t.Error("expected the non-empty table to NOT be deleted")
	}
	if entry, entryFound := status.FindManagedResource(updated, "dynamodb", "sessions"); !entryFound || entry.PendingDeletionSince == nil {
		t.Error("expected PendingDeletionSince to remain recorded")
	}
}

func TestCleanup_PendingDeletion_FirstCheckPastBackoffStartStillRunsAndStamps(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastEmptyCheckBackoffStart(t, ledger, "sessions")

	client.scanCalls = 0
	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if client.scanCalls != 1 {
		t.Errorf("expected a real check on first encounter past the backoff start, got %d calls", client.scanCalls)
	}
	entry, entryFound := status.FindManagedResource(updated, "dynamodb", "sessions")
	if !entryFound || entry.LastEmptyCheckAt == nil {
		t.Error("expected LastEmptyCheckAt to be stamped after a real check")
	}
}

func TestCleanup_PendingDeletion_SkipsRealCheckWhileBackoffIntervalNotElapsed(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastEmptyCheckBackoffStart(t, ledger, "sessions")
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}

	client.scanCalls = 0
	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("third Cleanup() error = %v", err)
	}
	if client.scanCalls != 0 {
		t.Errorf("expected the backoff interval to skip the real check, got %d calls", client.scanCalls)
	}
	if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected sessions to keep reporting PendingDeletion while backed off, got %+v", results)
	}
}

func TestCleanup_PendingDeletion_ChecksAndDeletesOnceBackoffIntervalElapses(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastEmptyCheckBackoffStart(t, ledger, "sessions")
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}

	client.tables[tableName].itemCount = 0
	pushLastEmptyCheckIntoThePast(t, ledger, "sessions", status.EmptyCheckBaseInterval+time.Minute)

	client.scanCalls = 0
	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("third Cleanup() error = %v", err)
	}
	if client.scanCalls != 1 {
		t.Errorf("expected the elapsed backoff interval to allow a real check, got %d calls", client.scanCalls)
	}
	if _, stillExists := client.tables[tableName]; stillExists {
		t.Error("expected the now-empty table to be deleted once backoff allowed a re-check")
	}
	if r := findResult(results, "sessions"); r != nil {
		t.Errorf("expected no pending-deletion result for a table that was actually deleted, got %+v", r)
	}
}

func TestCleanup_ReappearedInSpec_ClearsLastEmptyCheckAt(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastEmptyCheckBackoffStart(t, ledger, "sessions")
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if entry, entryFound := status.FindManagedResource(ledger, "dynamodb", "sessions"); !entryFound || entry.LastEmptyCheckAt == nil {
		t.Fatal("test setup broken: expected LastEmptyCheckAt to be stamped before the reappearance check")
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup (reappeared) error = %v", err)
	}
	entry, entryFound := status.FindManagedResource(updated, "dynamodb", "sessions")
	if !entryFound {
		t.Fatal("expected the ledger entry to survive reappearance")
	}
	if entry.LastEmptyCheckAt != nil {
		t.Error("expected LastEmptyCheckAt cleared alongside PendingDeletionSince on reappearance")
	}
}

func TestCleanup_ForceDeletesNonEmptyTable(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, true)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
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
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].tags = map[string]string{"team": "someone-else"}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
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
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[tableName].itemCount = 5

	longAgo := metav1.NewTime(time.Now().Add(-2 * PendingDeletionGracePeriod))
	entry, _ := status.FindManagedResource(ledger, "dynamodb", "sessions")
	entry.PendingDeletionSince = &longAgo
	status.UpsertManagedResource(&ledger, entry)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
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
	// Without treating "already gone" as success, retrying cleanup on a
	// table a previous attempt had already deleted (e.g. after a transient
	// failure removing the finalizer itself) would wrongly report
	// ErrTableNotOwned-style failures for a table that was, in fact,
	// correctly cleaned up already.
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)

	advancePastQuietWindow(t, ledger, "sessions")
	// Simulate the table already having been deleted out from under us
	// (e.g. a previous Cleanup pass succeeded but the process crashed
	// before persisting the ledger update).
	delete(client.tables, tableName)

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected an already-gone table to be treated as already cleaned up, not a failure", err)
	}
	if status.HasManagedResource(updated, "dynamodb", "sessions") {
		t.Error("expected the ledger entry to be removed for an already-gone table")
	}
}

func TestCleanup_ContinuesToOtherResourcesAfterOneFails(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Name: "orders", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
	ledger, err = Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("setup second Ensure() error = %v", err)
	}

	sessionsName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	client.tables[sessionsName].tags = map[string]string{"team": "someone-else"}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err == nil {
		t.Fatal("expected an error reported for the corrupted-ownership sessions table")
	}

	// orders is a fresh delete candidate, so this first pass only enters
	// its mandatory quiet window rather than deleting it outright - what
	// matters here is that it was processed at all despite sessions
	// failing first in the same loop.
	if entry, entryFound := status.FindManagedResource(updated, "dynamodb", "orders"); !entryFound || entry.PendingDeletionSince == nil {
		t.Error("expected orders to still be processed into its own pending-deletion window despite sessions failing")
	}
	if !status.HasManagedResource(updated, "dynamodb", "sessions") {
		t.Error("expected the failed sessions entry to remain in the ledger for retry")
	}
}

func TestCleanup_RelinquishingTable_EmitsEvent(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyRetain, false)
	recordEvent, events := newEventCollector()

	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, recordEvent); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "TableOwnershipRelinquished" {
		t.Errorf("expected exactly one TableOwnershipRelinquished event, got %+v", *events)
	}
}

func TestCleanup_FirstNoticedForDeletion_EmitsPendingEvent(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	recordEvent, events := newEventCollector()

	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, recordEvent); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "TableDeletionPending" {
		t.Errorf("expected exactly one TableDeletionPending event, got %+v", *events)
	}
}

func TestCleanup_RedeclaredWhilePending_EmitsCancelledEvent(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}

	recordEvent, events := newEventCollector()
	backInSpec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete}}}
	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", backInSpec, ledger, false, false, recordEvent); err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "TableDeletionCancelled" {
		t.Errorf("expected exactly one TableDeletionCancelled event, got %+v", *events)
	}
}

func TestCleanup_DeletingTable_EmitsDeletedEvent(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "sessions")

	recordEvent, events := newEventCollector()
	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, recordEvent); err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}

	if len(*events) != 1 || (*events)[0].reason != "TableDeleted" {
		t.Errorf("expected exactly one TableDeleted event, got %+v", *events)
	}
}

func TestCleanup_ARetainedEncryptedTableIsRelinquishedOnceAndKeptInTheLedger(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyRetain, false)
	// As the controller stamps an encrypted table: its key must outlive it.
	pinnedEntry, _ := status.FindManagedResource(ledger, resourceType, "sessions")
	pinnedEntry.KMSKeyARN = "arn:aws:kms:us-east-1:123456789012:key/sessions-key"
	status.UpsertManagedResource(&ledger, pinnedEntry)
	ctx := context.Background()
	cleanup := func() {
		t.Helper()
		var err error
		if ledger, _, err = Cleanup(ctx, client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
	}

	cleanup()
	entry, found := status.FindManagedResource(ledger, resourceType, "sessions")
	if !found || entry.State != depsv1alpha1.ManagedResourceStateRelinquished {
		t.Fatalf("expected the entry kept as Relinquished, got %+v (found=%v)", entry, found)
	}
	reads := client.listTagsCalls
	cleanup()
	if client.listTagsCalls != reads {
		t.Errorf("tag reads on a relinquished table = %d, want none", client.listTagsCalls-reads)
	}

	// A table deleted by hand after being relinquished is created fresh,
	// not reported as lost data it no longer holds.
	delete(client.tables, sessionsTableName())
	if _, err := Ensure(ctx, client, nil, nil, "default", "checkout-service", "uid-1", sessionsSpec(false), ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() after a relinquished table was deleted by hand: %v", err)
	}
}

// A pending table waiting out the quiet window or the empty-check backoff
// costs no AWS call at all: the ownership re-check only matters ahead of
// something that changes it.
func TestCleanup_PendingDeletion_ReadsNothingWhileWaiting(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	client.tables[cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)].itemCount = 5
	cleanup := func() ([]depsv1alpha1.ManagedResource, []CleanupResult) {
		t.Helper()
		updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		return updated, results
	}

	ledger, _ = cleanup()
	reads := client.listTagsCalls
	for _, phase := range []string{"quiet window", "backoff"} {
		if phase == "backoff" {
			advancePastEmptyCheckBackoffStart(t, ledger, "sessions")
			ledger, _ = cleanup() // the real check, which stamps LastEmptyCheckAt
			reads = client.listTagsCalls
		}
		var results []CleanupResult
		ledger, results = cleanup()
		if got := client.listTagsCalls; got != reads {
			t.Errorf("%s: %d reads while waiting, want none", phase, got-reads)
		}
		if r := findResult(results, "sessions"); r == nil || r.Reason != CleanupReasonPendingDeletion {
			t.Errorf("%s: expected PendingDeletion, got %+v", phase, results)
		}
	}
}

// Tables the operator creates are deletion protected, so its own delete, once
// every gate has approved it, has to switch the protection off first.
func TestCleanup_DeletesAProtectedTableAfterTurningOffItsProtection(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
	if !client.tables[tableName].deletionProtection {
		t.Fatal("setup: expected the created table to be protected")
	}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, stillExists := client.tables[tableName]; stillExists {
		t.Error("expected the protected table to be deleted")
	}
	if status.HasManagedResource(updated, resourceType, "sessions") {
		t.Error("expected the ledger entry to be removed")
	}
}

func TestCleanup_DoesNotTurnOffProtectionWhileTheTableIsStillBlocked(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", "sessions", 255)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	// First pass after removal from spec only marks the table pending.
	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if !client.tables[tableName].deletionProtection {
		t.Error("deletion protection was turned off before the table was approved for deletion")
	}
}
