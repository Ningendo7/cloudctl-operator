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

package sqs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func setupQueue(t *testing.T, client *fakeSQS, namespace, crName, name string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	t.Helper()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: name, DeletionPolicy: deletionPolicy, Force: force},
	}}
	ledger, err := Ensure(context.Background(), client, namespace, crName, "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
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

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyRetain, false)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected orders to be reported Retained, got %+v", results)
	}
	if status.FindManagedResource(updated, "sqs", "orders") == nil {
		t.Error("expected retained entry to stay in the ledger")
	}
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected Retain policy to leave the AWS queue in place")
	}
}

func TestCleanup_RelinquishesOwnershipTagForRetainedResource(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyRetain, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	if !cloudctlaws.IsOwnedBy(client.queues[queueName].tags, "default", "checkout-service", "uid-1") {
		t.Fatal("test setup broken: expected the queue to start out owned by us")
	}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if cloudctlaws.IsOwnedBy(client.queues[queueName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the ownership tag to be relinquished once a Retain resource is no longer declared")
	}
}

func TestCleanup_RelinquishIsIdempotent(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyRetain, false)

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if _, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false); err != nil {
		t.Fatalf("second Cleanup() on an already-relinquished resource should be a no-op, got error: %v", err)
	}
}

func TestCleanup_DeletesEmptyQueueWhenPolicyIsDelete(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no pending results for a clean delete, got %v", results)
	}
	if status.FindManagedResource(updated, "sqs", "orders") != nil {
		t.Error("expected ledger entry to be removed after deletion")
	}
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	if _, stillExists := client.queues[queueName]; stillExists {
		t.Error("expected the AWS queue to actually be deleted")
	}
}

func TestCleanup_KeepsDeclaredResourcesAlone(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	stillDeclared := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", stillDeclared, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results for a still-declared resource, got %v", results)
	}
	if status.FindManagedResource(updated, "sqs", "orders") == nil {
		t.Error("expected the still-declared entry to remain in the ledger")
	}
}

func TestCleanup_MakesNoAWSCallsForDeclaredResourceWithNoPendingMarker(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	// If Cleanup does anything beyond an in-memory check for a still-
	// declared resource that was never marked pending deletion, this
	// induced error surfaces it - a still-declared, never-pending resource
	// should be a pure no-op with zero AWS calls.
	client.getQueueUrlErr = errors.New("should not be called: no AWS call is needed here")

	stillDeclared := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", stillDeclared, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected a still-declared, never-pending resource to require no AWS calls at all", err)
	}
}

func TestCleanup_BlocksDeletingNonEmptyQueueWithoutForce(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "5"

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected orders to be PendingDeletion, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the non-empty queue to NOT be deleted")
	}
	entry := status.FindManagedResource(updated, "sqs", "orders")
	if entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected PendingDeletionSince to be recorded on first non-empty encounter")
	}
}

func TestCleanup_AddsDenyPolicyWhenMarkingPendingDeletion(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "5"

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if !strings.Contains(client.queues[queueName].policy, pendingDeletionDenySid) {
		t.Errorf("expected a send-blocking deny policy to be attached once pending deletion, got policy=%s", client.queues[queueName].policy)
	}
}

func TestCleanup_RemovesDenyPolicyWhenResourceReturnsToSpec(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "5"

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if !strings.Contains(client.queues[queueName].policy, pendingDeletionDenySid) {
		t.Fatal("test setup broken: expected the deny policy to be attached after the first pass")
	}

	backInSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete}}}
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", backInSpec, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}

	if strings.Contains(client.queues[queueName].policy, pendingDeletionDenySid) {
		t.Errorf("expected the deny policy to be removed once the resource is declared again, got policy=%s", client.queues[queueName].policy)
	}
	if entry := status.FindManagedResource(ledger, "sqs", "orders"); entry == nil || entry.PendingDeletionSince != nil {
		t.Errorf("expected PendingDeletionSince to be cleared once the resource is declared again, got %+v", entry)
	}
}

func TestCleanup_EscalatesToStuckAfterGracePeriod(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "5"

	longAgo := metav1.NewTime(time.Now().Add(-2 * PendingDeletionGracePeriod))
	entry := status.FindManagedResource(ledger, "sqs", "orders")
	entry.PendingDeletionSince = &longAgo
	status.UpsertManagedResource(&ledger, *entry)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Errorf("expected orders to escalate to StuckPendingDeletion, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the stuck queue to still NOT be force-deleted automatically - that decision is left to a human")
	}
}

func TestCleanup_ForceDeletesNonEmptyQueue(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, true)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "5"

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected force to override the non-empty guard, got %v", results)
	}
	if _, stillExists := client.queues[queueName]; stillExists {
		t.Error("expected force:true to delete the non-empty queue")
	}
}

func TestCleanup_RefusesDeletingUnverifiedOwnership(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	// Simulate the queue's tags having changed out-of-band since we last
	// verified it - e.g. manually re-tagged, or replaced by something else
	// entirely under the same name.
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].tags = map[string]string{"team": "someone-else"}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected Cleanup to refuse deleting a queue whose ownership tags no longer verify, even though the ledger says we created it")
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the queue to NOT be deleted when ownership can't be re-verified")
	}
}

func TestCleanup_KeepsDLQWhenStillDeclared(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results for a still-declared queue and its DLQ, got %v", results)
	}
	dlqQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders-dlq")
	if _, stillExists := client.queues[dlqQueueName]; !stillExists {
		t.Error("expected the DLQ to remain untouched while dlq:true is still declared")
	}
}

func TestCleanup_RemovesDLQWhenDLQDisabled(t *testing.T) {
	client := newFakeSQS()
	withDLQ := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", withDLQ, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	// dlq toggled off, main queue still declared.
	withoutDLQ := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: false, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", withoutDLQ, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	dlqQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders-dlq")
	if _, stillExists := client.queues[dlqQueueName]; stillExists {
		t.Error("expected the DLQ to be deleted once dlq is toggled off (deletionPolicy inherited as Delete)")
	}
	if status.FindManagedResource(updated, "sqs", "orders-dlq") != nil {
		t.Error("expected the DLQ's ledger entry to be removed")
	}
	mainQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	if _, stillExists := client.queues[mainQueueName]; !stillExists {
		t.Error("expected the main queue to be untouched by disabling its DLQ")
	}
}
