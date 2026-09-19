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
// The fakes only ever encode our own beliefs about how SQS's API behaves;
// these tests catch the case where that belief is simply wrong. Excluded
// from `go test ./...` by the "integration" build tag — see
// docs/testing.md for how to run them (LOCALSTACK_ENDPOINT, or
// `make test-integration`).
package sqs

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// newIntegrationClient builds a real SQS client pointed at LocalStack.
// Deliberately never uses config.LoadDefaultConfig or picks up the
// environment's own AWS credentials/profile — an integration test must be
// structurally incapable of ever reaching real AWS by accident.
func newIntegrationClient(t *testing.T) *sqs.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return sqs.New(sqs.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})
}

func TestIntegration_Ensure_CreatesRealQueueWithAttributesAndTags(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{
			Name:           "orders",
			DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
			Force:          true,
			Overrides:      &depsv1alpha1.SQSOverrides{VisibilityTimeoutSeconds: aws.Int32(45)},
		},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := findEntry(ledger, "orders")
	if entry == nil {
		t.Fatal("expected a ledger entry for orders")
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "orders")
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v — queue wasn't actually created against LocalStack", err)
	}

	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout, types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if got := attrsOut.Attributes["VisibilityTimeout"]; got != "45" {
		t.Errorf("real VisibilityTimeout = %q, want 45 — Ensure's attribute wiring doesn't match what SQS actually accepted", got)
	}
	if got := attrsOut.Attributes[string(types.QueueAttributeNameQueueArn)]; got != entry.ARN {
		t.Errorf("ledger ARN %q doesn't match the real queue's ARN %q", entry.ARN, got)
	}

	tagsOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: urlOut.QueueUrl})
	if err != nil {
		t.Fatalf("real ListQueueTags() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, "uid-1") {
		t.Errorf("real queue tags don't satisfy IsOwnedBy: %+v", tagsOut.Tags)
	}
}

func TestIntegration_Ensure_IsIdempotentAgainstRealAWS(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected exactly one ledger entry after two reconciles against real AWS, got %d", len(ledger))
	}
}

func TestIntegration_Ensure_AdoptsRealUntaggedQueue(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	queueName := cloudctlaws.ResourceName(namespace, crName, "orders")
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	// Create the queue directly via the real SDK, with no ownership tags at
	// all — simulating a resource that already existed before this CR ever
	// reconciled.
	if _, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName}); err != nil {
		t.Fatalf("setting up pre-existing real queue: %v", err)
	}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	tagsOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: urlOut.QueueUrl})
	if err != nil {
		t.Fatalf("real ListQueueTags() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, "uid-1") {
		t.Errorf("expected adopt:true to tag the pre-existing real queue as owned, got tags %+v", tagsOut.Tags)
	}
}

func TestIntegration_Ensure_CorrectsAttributeDriftOnRealQueue(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true,
			Overrides: &depsv1alpha1.SQSOverrides{VisibilityTimeoutSeconds: aws.Int32(30)}},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	spec.Resources[0].Overrides.VisibilityTimeoutSeconds = aws.Int32(120)
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger); err != nil {
		t.Fatalf("drift-correcting Ensure() error = %v", err)
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "orders")
	urlOut, _ := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if got := attrsOut.Attributes["VisibilityTimeout"]; got != "120" {
		t.Errorf("real VisibilityTimeout after drift correction = %q, want 120", got)
	}
}

func TestIntegration_Cleanup_DeletesRealQueueImmediatelyWhenForced(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", spec, ledger, true)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger entry to be removed, got %+v", ledger)
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "orders")
	if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName}); err == nil {
		t.Error("expected the real queue to be gone after Cleanup, but GetQueueUrl succeeded")
	}
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

// uniqueSuffix keeps each test's queue name distinct so parallel/repeated
// runs against the same long-lived LocalStack container never collide.
func uniqueSuffix(t *testing.T) string {
	return "test-" + t.Name()[len("TestIntegration_"):]
}

func deleteQueueIfExists(t *testing.T, client *sqs.Client, namespace, crName, resourceKey string) {
	t.Helper()
	queueName := cloudctlaws.ResourceName(namespace, crName, resourceKey)
	urlOut, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		return
	}
	_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl})
}
