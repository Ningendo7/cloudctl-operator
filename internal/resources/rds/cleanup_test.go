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
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// setupInstance creates and verifies one rds ledger entry via two Ensure
// passes (create, then verify) exactly as a real reconcile loop would,
// bypassing the security-group/subnet-group-grant machinery that isn't
// what cleanup_test.go is exercising.
func setupInstance(t *testing.T, client *fakeRDS, namespace, crName, name string, deletionPolicy depsv1alpha1.DeletionPolicy) []depsv1alpha1.ManagedResource {
	t.Helper()
	instanceID := cloudctlaws.ResourceName(namespace, crName, resourceType, name, 63)
	ledger, err := createInstance(context.Background(), client, namespace, crName, "uid-1", instanceID, name,
		instanceOptions{deletionPolicy: deletionPolicy, engine: "postgres", engineVersion: "16.3", instanceClass: "db.t4g.micro", dbSubnetGroupName: "sg", securityGroupID: "sg-1"},
		nil, nil)
	if err != nil {
		t.Fatalf("setup createInstance() error = %v", err)
	}
	ledger, err = ensureInstance(context.Background(), client, namespace, crName, "uid-1", name,
		instanceOptions{deletionPolicy: deletionPolicy, engine: "postgres", engineVersion: "16.3", instanceClass: "db.t4g.micro", dbSubnetGroupName: "sg", securityGroupID: "sg-1"},
		ledger, nil)
	if err != nil {
		t.Fatalf("setup ensureInstance() error = %v", err)
	}
	return ledger
}

func findCleanupResult(results []CleanupResult, name string) *CleanupResult {
	for i := range results {
		if results[i].Name == name {
			return &results[i]
		}
	}
	return nil
}

func backdatePendingDeletion(ledger []depsv1alpha1.ManagedResource, name string, d time.Duration) []depsv1alpha1.ManagedResource {
	entry := status.FindManagedResource(ledger, resourceType, name)
	past := metav1.NewTime(time.Now().Add(-d))
	updated := *entry
	updated.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, updated)
	return ledger
}

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected orders-db to be reported Retained, got %+v", results)
	}
	if status.FindManagedResource(updated, resourceType, "orders-db") == nil {
		t.Error("expected retained entry to stay in the ledger")
	}
	if client.createDBSnapshotCalls != 0 {
		t.Error("a Retain-policy instance should never trigger a final snapshot")
	}
}

func TestCleanup_RelinquishesOwnershipTagOnRetainedInstance(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	entry := status.FindManagedResource(ledger, resourceType, "orders-db")
	instance := client.findByARN(entry.ARN)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonRetained {
		t.Fatalf("expected Retained, got %+v", results)
	}
	if _, stillOwned := instance.tags[cloudctlaws.OwnerTagKey]; stillOwned {
		t.Error("expected the ownership tag to be removed from a relinquished instance")
	}
}

func TestCleanup_RelinquishIsIdempotent(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	// Second pass: ownership tag is already gone - must not error just
	// because there's nothing left to relinquish.
	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected Retained again, got %+v", results)
	}
}

func TestCleanup_ReappearingInSpecCancelsPendingDeletion(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	entry := status.FindManagedResource(ledger, resourceType, "orders-db")
	if entry.PendingDeletionSince == nil {
		t.Fatal("expected PendingDeletionSince to be set once removed from spec")
	}

	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "orders-db"}}}
	ledger, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no CleanupResult for a re-declared instance, got %+v", results)
	}
	entry = status.FindManagedResource(ledger, resourceType, "orders-db")
	if entry.PendingDeletionSince != nil {
		t.Error("expected PendingDeletionSince to be cleared once the instance reappeared in spec")
	}
}

func TestCleanup_DeletePolicy_FirstPassMarksPendingDeletion(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)

	ledger, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected PendingDeletion, got %+v", results)
	}
	if status.FindManagedResource(ledger, resourceType, "orders-db") == nil {
		t.Error("expected the instance to remain in the ledger - this package never deletes it")
	}
	if client.createDBSnapshotCalls != 0 {
		t.Error("should not take a snapshot before the grace period elapses")
	}
}

func TestCleanup_DeletePolicy_WithinQuietWindowStaysPending(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = backdatePendingDeletion(setMarked(t, client, ledger, "orders-db"), "orders-db", deletionQuietWindow/2)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected PendingDeletion within the quiet window, got %+v", results)
	}
}

// setMarked runs one Cleanup pass purely to get PendingDeletionSince set on
// the ledger, so backdatePendingDeletion has something to overwrite -
// kept separate from TestCleanup_DeletePolicy_FirstPassMarksPendingDeletion
// so each test's intent stays singular.
func setMarked(t *testing.T, client *fakeRDS, ledger []depsv1alpha1.ManagedResource, name string) []depsv1alpha1.ManagedResource {
	t.Helper()
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("setup Cleanup() error = %v", err)
	}
	return ledger
}

func TestCleanup_DeletePolicy_PastGracePeriodEscalatesToStuck(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)

	ledger, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Fatalf("expected StuckPendingDeletion, got %+v", results)
	}
	if client.createDBSnapshotCalls != 1 {
		t.Errorf("expected exactly one CreateDBSnapshot call, got %d", client.createDBSnapshotCalls)
	}
	snapshotEntry := status.FindManagedResource(ledger, snapshotResourceType, "orders-db")
	if snapshotEntry == nil {
		t.Fatal("expected a proactive final-snapshot ledger entry")
	}
	if snapshotEntry.State != depsv1alpha1.ManagedResourceStateCreating {
		t.Errorf("expected the fresh snapshot entry to start Creating, got %s", snapshotEntry.State)
	}
	if snapshotEntry.DeletionPolicy != depsv1alpha1.DeletionPolicyRetain {
		t.Errorf("a final snapshot must never be auto-deleted itself, got policy %s", snapshotEntry.DeletionPolicy)
	}
	// The instance itself is never actually deleted.
	if status.FindManagedResource(ledger, resourceType, "orders-db") == nil {
		t.Error("expected the stuck instance to remain in the ledger - DeleteDBInstance is never called")
	}
}

func TestCleanup_StuckInstance_SnapshotRequestedOnlyOnce(t *testing.T) {
	client := newFakeRDS()
	client.snapshotStatus = "creating"
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("first stuck Cleanup() error = %v", err)
	}
	if client.createDBSnapshotCalls != 1 {
		t.Fatalf("expected one CreateDBSnapshot call after the first stuck pass, got %d", client.createDBSnapshotCalls)
	}

	// Second pass: snapshot is still "creating" - must poll via
	// DescribeDBSnapshots, not request a brand new one.
	ledger, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("second stuck Cleanup() error = %v", err)
	}
	if client.createDBSnapshotCalls != 1 {
		t.Errorf("expected CreateDBSnapshot still only called once, got %d", client.createDBSnapshotCalls)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Errorf("expected to remain StuckPendingDeletion, got %+v", results)
	}
	snapshotEntry := status.FindManagedResource(ledger, snapshotResourceType, "orders-db")
	if snapshotEntry.State != depsv1alpha1.ManagedResourceStateCreating {
		t.Errorf("expected snapshot entry to remain Creating while AWS reports %q, got %s", "creating", snapshotEntry.State)
	}
}

func TestCleanup_StuckInstance_SnapshotBecomesAvailable(t *testing.T) {
	client := newFakeRDS()
	client.snapshotStatus = "creating"
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("first stuck Cleanup() error = %v", err)
	}
	snapshotID := status.FindManagedResource(ledger, snapshotResourceType, "orders-db").ARN
	client.setSnapshotStatus(snapshotID, "available")

	var events []string
	recordEvent := func(eventType, reason, message string) { events = append(events, reason) }
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, recordEvent)
	if err != nil {
		t.Fatalf("second stuck Cleanup() error = %v", err)
	}
	snapshotEntry := status.FindManagedResource(ledger, snapshotResourceType, "orders-db")
	if snapshotEntry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected snapshot entry to become Verified once AWS reports available, got %s", snapshotEntry.State)
	}
	if snapshotEntry.LastVerifiedAt == nil {
		t.Error("expected LastVerifiedAt to be set once the snapshot is confirmed available")
	}
	if !containsString(events, "FinalSnapshotReady") {
		t.Errorf("expected a FinalSnapshotReady event, got %v", events)
	}

	// Third pass: already Verified - must not re-describe or re-create.
	callsBefore := client.createDBSnapshotCalls
	_, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("third stuck Cleanup() error = %v", err)
	}
	if client.createDBSnapshotCalls != callsBefore {
		t.Error("expected no further CreateDBSnapshot calls once the snapshot is Verified")
	}
}

func TestCleanup_StuckInstance_SnapshotFailureIsSurfacedAsEvent(t *testing.T) {
	client := newFakeRDS()
	client.snapshotStatus = "failed"
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)
	// First pass creates it still reporting "creating" internally via
	// CreateDBSnapshotOutput, then the second pass's Describe call is what
	// actually observes the failed status - matching how AWS's own
	// snapshot status is only ever knowable asynchronously.
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("first stuck Cleanup() error = %v", err)
	}

	var events []string
	recordEvent := func(eventType, reason, message string) { events = append(events, reason) }
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, recordEvent)
	if err != nil {
		t.Fatalf("second stuck Cleanup() error = %v", err)
	}
	if !containsString(events, "FinalSnapshotFailed") {
		t.Errorf("expected a FinalSnapshotFailed event, got %v", events)
	}
	snapshotEntry := status.FindManagedResource(ledger, snapshotResourceType, "orders-db")
	if snapshotEntry.State == depsv1alpha1.ManagedResourceStateVerified {
		t.Error("a failed snapshot must never be marked Verified")
	}
}

func TestCleanup_CreateDBSnapshotAlreadyExists_RecoversGracefully(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)

	// Simulate a snapshot that already exists in AWS (e.g. a previous
	// Cleanup pass's CreateDBSnapshot call succeeded but crashed before the
	// ledger write landed) by pre-creating it under the exact deterministic
	// ID this package will compute - which folds in this episode's own
	// PendingDeletionSince, not just the resource name (see
	// ensureFinalSnapshot's own comment for why).
	instanceEntry := status.FindManagedResource(ledger, resourceType, "orders-db")
	episodeID := strconv.FormatInt(instanceEntry.PendingDeletionSince.Unix(), 10)
	snapshotID := cloudctlaws.DerivedResourceName("default", "checkout-service", resourceType, 255, "orders-db", "final-snapshot", episodeID)
	instanceID, err := instanceIDFromARN(instanceEntry.ARN)
	if err != nil {
		t.Fatalf("instanceIDFromARN() error = %v", err)
	}
	client.snapshots[snapshotID] = &fakeSnapshot{id: snapshotID, arn: "arn:aws:rds:us-east-1:123456789012:snapshot:" + snapshotID, instanceID: instanceID, status: "available"}

	ledger, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Fatalf("expected StuckPendingDeletion despite the AlreadyExists recovery, got %+v", results)
	}
	snapshotEntry := status.FindManagedResource(ledger, snapshotResourceType, "orders-db")
	if snapshotEntry == nil {
		t.Fatal("expected the ledger to record the pre-existing snapshot rather than erroring out")
	}
	if snapshotEntry.ARN != snapshotID {
		t.Errorf("expected the ledger to adopt the pre-existing snapshot ID %q, got %q", snapshotID, snapshotEntry.ARN)
	}
	if client.createDBSnapshotCalls != 1 {
		t.Errorf("expected exactly one CreateDBSnapshot attempt (the one that hit AlreadyExists), got %d", client.createDBSnapshotCalls)
	}
}

// TestCleanup_SecondStuckEpisode_TakesFreshSnapshotNotStaleOne proves the
// full redeclare-then-stuck-again cycle end to end: once an instance
// reappears in spec (clearing the first episode's snapshot bookkeeping,
// per the declared[entry.Name] branch above) and is later removed and
// left stuck a second time, it gets a genuinely new snapshot rather than
// silently reusing or colliding with the first episode's already-Retained
// one - the exact scenario a purely name-based (not episode-based) ID
// would get wrong.
func TestCleanup_SecondStuckEpisode_TakesFreshSnapshotNotStaleOne(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)

	// First episode: removed, stuck, snapshotted.
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("first-episode Cleanup() error = %v", err)
	}
	firstSnapshotARN := status.FindManagedResource(ledger, snapshotResourceType, "orders-db").ARN
	if client.createDBSnapshotCalls != 1 {
		t.Fatalf("expected one CreateDBSnapshot call after the first episode, got %d", client.createDBSnapshotCalls)
	}

	// Redeclared - clears the first episode's snapshot bookkeeping entry,
	// but the AWS-side snapshot itself must still exist afterward.
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "orders-db"}}}
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, false, nil)
	if err != nil {
		t.Fatalf("redeclare Cleanup() error = %v", err)
	}
	if status.FindManagedResource(ledger, snapshotResourceType, "orders-db") != nil {
		t.Fatal("expected the first episode's snapshot bookkeeping entry to be cleared on redeclare")
	}
	if _, stillExists := client.snapshots[firstSnapshotARN]; !stillExists {
		t.Error("expected the first episode's AWS-side snapshot to still exist - redeclaring must never delete it")
	}

	// Removed and left stuck a second time - a real second episode,
	// separated in time from the first.
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+2*time.Hour)
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("second-episode Cleanup() error = %v", err)
	}
	if client.createDBSnapshotCalls != 2 {
		t.Errorf("expected a second, fresh CreateDBSnapshot call for the second episode, got %d total calls", client.createDBSnapshotCalls)
	}
	secondSnapshotARN := status.FindManagedResource(ledger, snapshotResourceType, "orders-db").ARN
	if secondSnapshotARN == firstSnapshotARN {
		t.Errorf("expected the second episode's snapshot to have a different ID than the first, got the same %q for both", secondSnapshotARN)
	}
	if _, stillExists := client.snapshots[firstSnapshotARN]; !stillExists {
		t.Error("expected the first episode's snapshot to still exist alongside the second - neither is ever deleted")
	}
}

func TestCleanup_PropagatesCreateDBSnapshotFailure(t *testing.T) {
	client := newFakeRDS()
	client.createDBSnapshotErr = &fakeAWSError{code: "ThrottlingException"}
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err == nil {
		t.Fatal("expected CreateDBSnapshot's failure to propagate")
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Errorf("expected the result to still report StuckPendingDeletion even though the snapshot step failed, got %+v", results)
	}
}

func TestCleanup_PropagatesDescribeDBSnapshotsFailure(t *testing.T) {
	client := newFakeRDS()
	client.snapshotStatus = "creating"
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	ledger = setMarked(t, client, ledger, "orders-db")
	ledger = backdatePendingDeletion(ledger, "orders-db", PendingDeletionGracePeriod+time.Hour)
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("first stuck Cleanup() error = %v", err)
	}

	client.describeDBSnapshotsErr = &fakeAWSError{code: "ThrottlingException"}
	_, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err == nil {
		t.Fatal("expected DescribeDBSnapshots' failure to propagate")
	}
}

func TestCleanup_PropagatesRemoveTagsFailure(t *testing.T) {
	client := newFakeRDS()
	client.removeTagsFromResourceErr = &fakeAWSError{code: "ThrottlingException"}
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err == nil {
		t.Fatal("expected RemoveTagsFromResource's failure to propagate")
	}
}

func TestCleanup_PropagatesListTagsFailureDuringRelinquish(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	client.listTagsForResourceErr = &fakeAWSError{code: "ThrottlingException"}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err == nil {
		t.Fatal("expected ListTagsForResource's failure during relinquish to propagate")
	}
}

func TestCleanup_DeletingTrue_IgnoresSpecEntirely(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyDelete)
	// Even though spec still declares the instance, deleting=true (the
	// finalize path) must treat every ledger entry as no longer declared -
	// a CR being deleted doesn't care what its own spec still says.
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "orders-db"}}}

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, true, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findCleanupResult(results, "orders-db"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected PendingDeletion during finalize regardless of spec, got %+v", results)
	}
}

func TestCleanup_IgnoresLedgerEntriesOfOtherResourceTypes(t *testing.T) {
	client := newFakeRDS()
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "iam", Name: "role", ARN: "arn:aws:iam::123456789012:role/foo", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName, ARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.RDSSpec{}, ledger, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results for non-rds ledger entries, got %+v", results)
	}
	if len(updated) != 2 {
		t.Errorf("expected both unrelated entries left untouched, got %+v", updated)
	}
}

func TestInstanceIDFromARN(t *testing.T) {
	tests := []struct {
		name    string
		arn     string
		want    string
		wantErr bool
	}{
		{name: "well-formed", arn: "arn:aws:rds:us-east-1:123456789012:db:default-checkout-service-orders-db-abc123", want: "default-checkout-service-orders-db-abc123"},
		{name: "empty string", arn: "", wantErr: true},
		{name: "trailing colon", arn: "arn:aws:rds:us-east-1:123456789012:db:", wantErr: true},
		{name: "no colon at all", arn: "not-an-arn", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := instanceIDFromARN(tc.arn)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %q, got id %q", tc.arn, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("instanceIDFromARN(%q) error = %v", tc.arn, err)
			}
			if got != tc.want {
				t.Errorf("instanceIDFromARN(%q) = %q, want %q", tc.arn, got, tc.want)
			}
		})
	}
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
