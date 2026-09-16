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

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
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

func TestCleanup_TreatsUnsetDeletionPolicyAsRetain(t *testing.T) {
	// Defensive-coding verification: Cleanup checks "!= Delete", not
	// "== Retain", specifically so a DeletionPolicy that's somehow truly
	// unset (Go's zero value, bypassing the CRD's own default) still falls
	// to the safe side rather than an undefined one. Every other test
	// exercises an explicitly-set DeletionPolicyRetain, which never
	// actually proves this - this one constructs the ledger entry directly
	// with the field left at its zero value.
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: &queueName,
		Tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	})
	if err != nil {
		t.Fatalf("test setup: CreateQueue() error = %v", err)
	}
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type: resourceType,
			Name: "orders",
			ARN:  "arn:aws:sqs:us-east-1:000000000000:" + queueName,
			// DeletionPolicy deliberately left unset (zero value).
		},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected an unset DeletionPolicy to be treated as Retained, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the queue to survive - an unset DeletionPolicy must never be treated as Delete")
	}
	if status.FindManagedResource(updated, "sqs", "orders") == nil {
		t.Error("expected the retained entry to stay in the ledger")
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

func TestCleanup_HoldsNewlyEligibleQueueForQuietWindowBeforeDeleting(t *testing.T) {
	// Regression-shaped test for the GetQueueAttributes consistency gap:
	// AWS documents its message counters as approximate/eventually
	// consistent, so a queue that looks empty on the very first pass it
	// comes up for deletion must NOT be deleted immediately - only denied
	// and held. Only once the quiet window has elapsed AND it's still
	// confirmed empty should it actually delete.
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected orders to be held PendingDeletion on first encounter, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Fatal("expected the queue to survive the first pass even though it's empty right now")
	}
	entry := status.FindManagedResource(updated, "sqs", "orders")
	if entry == nil || entry.PendingDeletionSince == nil {
		t.Fatal("expected PendingDeletionSince to be recorded on first encounter")
	}

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, updated, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected orders to still be PendingDeletion while inside the quiet window, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Fatal("expected the queue to still survive while inside the quiet window")
	}

	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry = status.FindManagedResource(updated, "sqs", "orders")
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&updated, *entry)

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, updated, false)
	if err != nil {
		t.Fatalf("third Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r != nil {
		t.Errorf("expected no pending result once actually deleted, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; stillExists {
		t.Error("expected the queue to be deleted once the quiet window elapsed and it's confirmed empty")
	}
	if status.FindManagedResource(updated, "sqs", "orders") != nil {
		t.Error("expected the ledger entry to be removed after deletion")
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

// advancePastQuietWindow pushes a ledger entry's PendingDeletionSince into
// the past so a subsequent Cleanup() call evaluates the actual emptiness
// check instead of just holding through the mandatory quiet window.
func advancePastQuietWindow(t *testing.T, ledger []depsv1alpha1.ManagedResource, name string) {
	t.Helper()
	entry := status.FindManagedResource(ledger, "sqs", name)
	if entry == nil {
		t.Fatalf("test setup broken: no ledger entry named %q", name)
	}
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)
}

func TestCleanup_BlocksDeletingNonEmptyQueueWithoutForce(t *testing.T) {
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "5"

	// First pass only enters the mandatory quiet window - the counters
	// aren't evaluated yet on first encounter.
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "orders")

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected orders to be PendingDeletion, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the non-empty queue to NOT be deleted")
	}
	entry := status.FindManagedResource(updated, "sqs", "orders")
	if entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected PendingDeletionSince to remain recorded")
	}
}

func TestCleanup_BlocksDeletingQueueWithOnlyInFlightMessages(t *testing.T) {
	// Regression test: ApproximateNumberOfMessages (visible) can read zero
	// while ApproximateNumberOfMessagesNotVisible (in-flight, sent to a
	// consumer but not yet deleted or expired) is non-zero - these are
	// independent counters. A guard checking only the first one would
	// delete a queue a consumer is actively mid-processing on.
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "0"
	client.queues[queueName].approxMessagesHidden = "3"

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "orders")

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected orders to be PendingDeletion due to in-flight messages, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the queue with in-flight messages to NOT be deleted despite zero visible messages")
	}
}

func TestCleanup_BlocksDeletingQueueWithOnlyDelayedMessages(t *testing.T) {
	// Same regression, for ApproximateNumberOfMessagesDelayed - messages
	// scheduled via DelaySeconds that aren't yet readable.
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)

	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[queueName].approxMessages = "0"
	client.queues[queueName].approxMessagesDelayed = "2"

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "orders")

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "orders"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected orders to be PendingDeletion due to delayed messages, got %+v", results)
	}
	if _, stillExists := client.queues[queueName]; !stillExists {
		t.Error("expected the queue with delayed messages to NOT be deleted despite zero visible messages")
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

	policy := client.queues[queueName].policy
	if !strings.Contains(policy, pendingDeletionDenySid) {
		t.Errorf("expected a send-blocking deny policy to be attached once pending deletion, got policy=%s", policy)
	}
	// Exact quoted matches - "sqs:SendMessage" alone is a literal substring
	// of "sqs:SendMessageBatch", so an unquoted Contains check would pass
	// even if the batch action were the only one actually denied.
	if !strings.Contains(policy, `"sqs:SendMessage"`) || !strings.Contains(policy, `"sqs:SendMessageBatch"`) {
		t.Errorf("expected the pending-deletion deny to block both SendMessage and SendMessageBatch, got policy=%s", policy)
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

func TestCleanup_TreatsAlreadyGoneQueueAsSuccess(t *testing.T) {
	// Regression-shaped test carried over from a real bug found reviewing a
	// sibling project's S3 cleanup: without treating "already gone" as
	// success, retrying cleanup on a queue a previous attempt had already
	// deleted (e.g. after a transient failure removing the finalizer
	// itself) would wrongly report a failure for a queue that was, in
	// fact, correctly cleaned up already.
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")

	advancePastQuietWindow(t, ledger, "orders")
	// Simulate the queue already having been deleted out from under us.
	delete(client.queues, queueName)

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v — expected an already-gone queue to be treated as already cleaned up, not a failure", err)
	}
	if status.FindManagedResource(updated, "sqs", "orders") != nil {
		t.Error("expected the ledger entry to be removed for an already-gone queue")
	}
}

func TestCleanup_DoesNotForgetQueueOnTransientLookupError(t *testing.T) {
	// Regression test for a real bug: GetQueueUrl failing for any reason
	// (throttling, a permission gap, a network blip) must NOT be treated
	// the same as "the queue doesn't exist" - doing so silently dropped a
	// queue that still exists from the ledger, abandoning it without ever
	// actually deleting or retaining it per policy.
	client := newFakeSQS()
	ledger := setupQueue(t, client, "default", "checkout-service", "orders", depsv1alpha1.DeletionPolicyDelete, false)
	advancePastQuietWindow(t, ledger, "orders")
	client.getQueueUrlErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultServer}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected a transient GetQueueUrl failure to be reported as an error, not silently swallowed")
	}
	if status.FindManagedResource(updated, "sqs", "orders") == nil {
		t.Error("expected the ledger entry to survive a transient lookup failure, not be silently forgotten")
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
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", withoutDLQ, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "orders-dlq")

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", withoutDLQ, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
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

func TestCleanup_DeletesFIFOQueueByARNDerivedName(t *testing.T) {
	// Regression test: Cleanup must not recompute the AWS queue name from
	// namespace/crName/key (which wouldn't know to append .fifo once the
	// spec entry, and its fifo:true, is gone) - it must derive the real
	// name from the ledger's stored ARN instead.
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", FIFO: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	fifoName := cloudctlaws.ResourceName("default", "checkout-service", "orders") + ".fifo"
	if _, exists := client.queues[fifoName]; !exists {
		t.Fatalf("test setup broken: expected FIFO queue %q to exist", fifoName)
	}

	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	advancePastQuietWindow(t, ledger, "orders")

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected a clean delete with no pending results, got %v", results)
	}
	if _, stillExists := client.queues[fifoName]; stillExists {
		t.Error("expected the FIFO queue to actually be deleted, not silently treated as already-gone due to a name mismatch")
	}
}

func TestCleanup_ContinuesToOtherResourcesAfterOneFails(t *testing.T) {
	// Regression test: a failure cleaning up one ledger entry must not
	// prevent an unrelated entry from being processed in the same pass.
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	// Corrupt orders' ownership out-of-band so its cleanup fails.
	ordersName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	client.queues[ordersName].tags = map[string]string{"team": "someone-else"}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected an error reported for the corrupted-ownership queue")
	}

	// receipts is a fresh delete candidate, so this first pass only enters
	// its mandatory quiet window rather than deleting it outright - what
	// matters for this regression test is that it was processed at all
	// (i.e. orders failing didn't stop the loop before reaching it).
	if entry := status.FindManagedResource(updated, "sqs", "receipts"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected receipts to still be processed into its own pending-deletion window despite orders failing")
	}
	if status.FindManagedResource(updated, "sqs", "orders") == nil {
		t.Error("expected the failed orders entry to remain in the ledger for retry")
	}
}
