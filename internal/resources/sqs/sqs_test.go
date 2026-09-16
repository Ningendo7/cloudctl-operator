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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func TestEnsure_CreatesNewQueue(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{
		Resources: []depsv1alpha1.SQSQueueSpec{
			{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
		},
	}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, "sqs", "orders")
	if entry == nil {
		t.Fatal("expected a ledger entry for orders")
	}
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected Verified state, got %s", entry.State)
	}
	if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyRetain {
		t.Errorf("expected captured deletion policy Retain, got %s", entry.DeletionPolicy)
	}

	wantName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	if _, ok := client.queues[wantName]; !ok {
		t.Errorf("expected queue %q to have been created", wantName)
	}
}

func TestEnsure_IsIdempotentAndPreservesCreatedAt(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	firstCreatedAt := status.FindManagedResource(ledger, "sqs", "orders").CreatedAt

	ledger, err = Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(client.queues) != 1 {
		t.Fatalf("expected exactly one queue after re-reconciling, got %d", len(client.queues))
	}
	got := status.FindManagedResource(ledger, "sqs", "orders").CreatedAt
	if !got.Equal(&firstCreatedAt) {
		t.Errorf("expected CreatedAt to be preserved across reconciles, got %v want %v", got, firstCreatedAt)
	}
}

func TestEnsure_RefusesUnownedExistingQueue(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	// Simulate a pre-existing queue this controller did not create - e.g.
	// hand-created, or owned by something else entirely.
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: &queueName,
		Tags:      map[string]string{"team": "someone-else"},
	})

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error when a same-named queue exists without our ownership tag")
	}
}

func TestEnsure_AdoptsUntaggedEmptyQueue(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	// A queue that exists, has no tags at all, and is empty - the only case
	// adopt:true is meant to cover.
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	client.queues[queueName].tags = nil

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", Adopt: true}}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if entry := status.FindManagedResource(ledger, "sqs", "orders"); entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected a Verified ledger entry after adoption, got %+v", entry)
	}
	if !cloudctlaws.IsOwnedBy(client.queues[queueName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the queue to be tagged as owned by this CR after adoption")
	}
}

func TestEnsure_AdoptsQueueWithUnrelatedExistingTags(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	// A legacy queue with ordinary organizational tags (cost-center, team)
	// predating this operator - the primary case adopt:true exists for.
	// Not owned by us, but also nothing suggesting active conflicting
	// management - this should adopt cleanly, preserving the existing tags.
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: &queueName,
		Tags:      map[string]string{"cost-center": "1234", "team": "checkout"},
	})

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", Adopt: true}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("expected adoption to succeed for a queue with only unrelated organizational tags, got error: %v", err)
	}
	tags := client.queues[queueName].tags
	if tags["cost-center"] != "1234" || tags["team"] != "checkout" {
		t.Errorf("expected pre-existing tags to be preserved through adoption, got %v", tags)
	}
	if !cloudctlaws.IsOwnedBy(tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the queue to be tagged as owned by this CR after adoption")
	}
}

func TestEnsure_AdoptsQueueEvenWithMessagesInFlight(t *testing.T) {
	// Deliberate: adoption only checks for a conflicting cloudctl-owner tag,
	// not resource activity. adopt:true is already an explicit, deliberate
	// human act naming a specific resource - the tool doesn't second-guess
	// that with an activity heuristic, which also wouldn't generalize
	// cleanly to other resource types (S3, KMS, ...) anyway.
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	client.queues[queueName].tags = nil
	client.queues[queueName].approxMessages = "3"

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", Adopt: true}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("expected adoption to succeed regardless of message activity, got error: %v", err)
	}
	if !cloudctlaws.IsOwnedBy(client.queues[queueName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the queue to be tagged as owned by this CR after adoption")
	}
}

func TestEnsure_RefusesAdoptingQueueOwnedByDifferentCR(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: &queueName,
		Tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "some-other-cr"),
			cloudctlaws.OwnerUIDTagKey: "different-uid",
		},
	})

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", Adopt: true}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to never override a resource already owned by a different AppDependencies CR")
	}
}

func TestEnsure_ClassifiesTransientAWSErrorsAsRetryable(t *testing.T) {
	client := newFakeSQS()
	client.getQueueUrlErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error when the queue lookup fails")
	}

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if !reconcileErr.Retryable {
		t.Error("expected a throttling-style error to be classified as retryable")
	}
}

func TestEnsure_ClassifiesPermissionErrorsAsNotRetryable(t *testing.T) {
	client := newFakeSQS()
	client.getQueueUrlErr = &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error when the queue lookup fails")
	}

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if reconcileErr.Retryable {
		t.Error("expected a permission-denied error to be classified as not retryable — retrying won't fix an IAM gap")
	}
}

func TestEnsure_TreatsQueueDeletedRecentlyAsRetryable(t *testing.T) {
	client := newFakeSQS()
	client.createQueueErr = &types.QueueDeletedRecently{}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error when the queue was deleted too recently to recreate")
	}

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if !reconcileErr.Retryable {
		t.Error("expected QueueDeletedRecently to be classified as retryable, not a hard failure")
	}
}

func TestEnsure_CreatesDLQAndSetsRedrivePolicy(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: true},
	}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	dlqEntry := status.FindManagedResource(ledger, "sqs", "orders-dlq")
	if dlqEntry == nil {
		t.Fatal("expected a ledger entry for the DLQ")
	}
	dlqQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders-dlq")
	dlqQueue, ok := client.queues[dlqQueueName]
	if !ok {
		t.Fatalf("expected DLQ queue %q to have been created", dlqQueueName)
	}
	if dlqEntry.ARN != dlqQueue.arn {
		t.Errorf("expected ledger ARN to match the created DLQ's ARN, got %q want %q", dlqEntry.ARN, dlqQueue.arn)
	}

	mainQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	mainQueue, ok := client.queues[mainQueueName]
	if !ok {
		t.Fatalf("expected main queue %q to have been created", mainQueueName)
	}
	redrivePolicy := mainQueue.attributes["RedrivePolicy"]
	if redrivePolicy == "" {
		t.Fatal("expected the main queue to have a RedrivePolicy attribute set")
	}
	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
		MaxReceiveCount     string `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(redrivePolicy), &policy); err != nil {
		t.Fatalf("failed to parse redrive policy: %v", err)
	}
	if policy.DeadLetterTargetArn != dlqQueue.arn {
		t.Errorf("expected redrive policy to target the DLQ's ARN, got %q want %q", policy.DeadLetterTargetArn, dlqQueue.arn)
	}
	if policy.MaxReceiveCount != "5" {
		t.Errorf("expected the default maxReceiveCount of 5, got %q", policy.MaxReceiveCount)
	}
}

func TestEnsure_DLQRespectsMaxReceiveCountOverride(t *testing.T) {
	client := newFakeSQS()
	override := int32(10)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: true, Overrides: &depsv1alpha1.SQSOverrides{MaxReceiveCount: &override}},
	}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	mainQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	var policy struct {
		MaxReceiveCount string `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(client.queues[mainQueueName].attributes["RedrivePolicy"]), &policy); err != nil {
		t.Fatalf("failed to parse redrive policy: %v", err)
	}
	if policy.MaxReceiveCount != "10" {
		t.Errorf("expected the overridden maxReceiveCount of 10, got %q", policy.MaxReceiveCount)
	}
}

func TestEnsure_NoDLQMeansNoRedrivePolicy(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	mainQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	dlqQueueName := cloudctlaws.ResourceName("default", "checkout-service", "orders-dlq")
	if _, exists := client.queues[dlqQueueName]; exists {
		t.Error("expected no DLQ to be created when dlq is false")
	}
	if _, ok := client.queues[mainQueueName].attributes["RedrivePolicy"]; ok {
		t.Error("expected no RedrivePolicy attribute when dlq is false")
	}
}

func TestEnsure_SkipsRevalidationWithinTrustWindow(t *testing.T) {
	client := newFakeSQS()
	// Never actually register the queue in the fake at all - if Ensure
	// makes any AWS call here, it fails (not-found -> tries to create,
	// which would still succeed, so instead force GetQueueUrl itself to
	// error) - proving the skip happened by making any call unmistakably
	// visible as a failure.
	client.getQueueUrlErr = errors.New("should not be called: trust window should have skipped this")

	fresh := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type:           resourceType,
			Name:           "orders",
			ARN:            "arn:aws:sqs:us-east-1:000000000000:default-checkout-service-orders",
			State:          depsv1alpha1.ManagedResourceStateVerified,
			DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt:      fresh,
			LastVerifiedAt: &fresh,
		},
	}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
	}}
	updatedLedger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("Ensure() error = %v — expected the trust window to skip the AWS call entirely", err)
	}

	entry := status.FindManagedResource(updatedLedger, "sqs", "orders")
	if entry == nil {
		t.Fatal("expected the ledger entry to survive the skip path")
	}
	if entry.ARN != "arn:aws:sqs:us-east-1:000000000000:default-checkout-service-orders" {
		t.Errorf("expected the cached ARN to be preserved, got %s", entry.ARN)
	}
	if entry.LastVerifiedAt == nil || !entry.LastVerifiedAt.Equal(&fresh) {
		t.Error("expected LastVerifiedAt to stay unchanged since no real verification occurred")
	}
}

func TestEnsure_UpdatesLocalFieldsEvenWhenSkippingRevalidation(t *testing.T) {
	client := newFakeSQS()
	client.getQueueUrlErr = errors.New("should not be called: trust window should have skipped this")

	fresh := metav1.Now()
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type:           resourceType,
			Name:           "orders",
			ARN:            "arn:aws:sqs:us-east-1:000000000000:default-checkout-service-orders",
			State:          depsv1alpha1.ManagedResourceStateVerified,
			DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			Force:          false,
			CreatedAt:      fresh,
			LastVerifiedAt: &fresh,
		},
	}

	// deletionPolicy changed in spec - a pure local edit that shouldn't
	// need an AWS call to take effect, even while skipping revalidation.
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	updatedLedger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(updatedLedger, "sqs", "orders")
	if entry.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete {
		t.Errorf("expected deletionPolicy to update to Delete even while skipping revalidation, got %s", entry.DeletionPolicy)
	}
	if !entry.Force {
		t.Error("expected force to update to true even while skipping revalidation")
	}
}

func TestEnsure_RevalidatesAfterTrustWindowExpires(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	entry := status.FindManagedResource(ledger, "sqs", "orders")
	entry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, *entry)

	updatedLedger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	updatedEntry := status.FindManagedResource(updatedLedger, "sqs", "orders")
	if updatedEntry.LastVerifiedAt.Equal(&stale) {
		t.Error("expected LastVerifiedAt to be refreshed once the trust window expired and revalidation ran")
	}
}
