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
// The fakes only ever encode our own beliefs about how SNS's API behaves;
// these tests catch the case where that belief is simply wrong. Excluded
// from `go test ./...` by the "integration" build tag — see
// docs/testing.md for how to run them (LOCALSTACK_ENDPOINT, or
// `make test-integration`).
//
// One real gap this tier can't close: this LocalStack version's
// ListTagsForResource returns a plain empty-tags success for a topic ARN
// that doesn't exist yet, instead of the ResourceNotFoundException real AWS
// raises — a LocalStack bug, not a bug in our code (confirmed working
// correctly against real AWS by the live tier instead; see
// docs/testing.md). Ensure can therefore never observe "doesn't exist yet"
// against LocalStack, so tests that don't specifically need to exercise
// CreateTopic itself pre-create and pre-tag their topic directly via
// createTaggedTopic, exercising every other real code path as normal.
package sns

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

const (
	testRegionIntegration    = "us-east-1"
	testAccountIDIntegration = "000000000000"
)

// newIntegrationClient builds a real SNS client pointed at LocalStack.
// Deliberately never uses config.LoadDefaultConfig or picks up the
// environment's own AWS credentials/profile — an integration test must be
// structurally incapable of ever reaching real AWS by accident.
func newIntegrationClient(t *testing.T) *sns.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return sns.New(sns.Options{
		Region:       testRegionIntegration,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

// uniqueSuffix keeps each test's topic name distinct so parallel/repeated
// runs against the same long-lived LocalStack container never collide.
func uniqueSuffix(t *testing.T) string {
	return "test-" + t.Name()[len("TestIntegration_"):]
}

func deleteTopicIfExists(t *testing.T, client *sns.Client, topicArn string) {
	t.Helper()
	_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: &topicArn})
}

// createTaggedTopic creates and tags a topic directly via the real SDK,
// bypassing Ensure's own create-path detection entirely. LocalStack's
// ListTagsForResource returns a plain empty-tags success for a topic ARN
// that doesn't exist yet, instead of the ResourceNotFoundException real AWS
// raises (confirmed directly against this LocalStack version; production
// code is correct, proven against real AWS by the live tier) - so Ensure
// can never observe "doesn't exist yet" against LocalStack and always takes
// the "already exists" ownership-check branch. Tests that don't specifically
// need to exercise CreateTopic itself pre-create and pre-tag their topic
// with this instead, exercising every other real code path normally.
func createTaggedTopic(t *testing.T, client *sns.Client, namespace, crName, crUID, topicName string) string {
	t.Helper()
	ctx := context.Background()
	createOut, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicName})
	if err != nil {
		t.Fatalf("real CreateTopic() (setup) error = %v", err)
	}
	if _, err := client.TagResource(ctx, &sns.TagResourceInput{
		ResourceArn: createOut.TopicArn,
		Tags: mapToTags(map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
			cloudctlaws.OwnerUIDTagKey: crUID,
		}),
	}); err != nil {
		t.Fatalf("real TagResource() (setup) error = %v", err)
	}
	return *createOut.TopicArn
}

// TestIntegration_Ensure_ReconcilesTagsOnExistingTopic exercises the same
// ownership-check-then-reconcile path Ensure takes for a topic that already
// exists (regardless of who created it) — see createTaggedTopic's own doc
// comment for why this doesn't go through Ensure's own CreateTopic call.
func TestIntegration_Ensure_ReconcilesTagsOnExistingTopic(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	topicName := cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256)
	topicArn := createTaggedTopic(t, client, namespace, crName, "uid-1", topicName)
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := findEntry(ledger, "events")
	if entry == nil {
		t.Fatal("expected a ledger entry for events")
	}
	if entry.ARN != topicArn {
		t.Fatalf("ledger ARN = %q, want the constructed ARN %q — LocalStack didn't accept/return the topic the way Ensure assumes", entry.ARN, topicArn)
	}

	if _, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn}); err != nil {
		t.Fatalf("real GetTopicAttributes() error = %v — topic wasn't actually created against LocalStack", err)
	}
	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &topicArn})
	if err != nil {
		t.Fatalf("real ListTagsForResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.Tags), namespace, crName, "uid-1") {
		t.Errorf("real topic tags don't satisfy IsOwnedBy: %+v", tagsOut.Tags)
	}
}

func TestIntegration_Ensure_IsIdempotentAgainstRealAWS(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	topicName := cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256)
	topicArn := createTaggedTopic(t, client, namespace, crName, "uid-1", topicName)
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected exactly one ledger entry after two reconciles against real AWS, got %d", len(ledger))
	}
}

func TestIntegration_Ensure_AdoptsRealUntaggedTopic(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	topicName := cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256)
	topicArn := cloudctlaws.TopicARN(testRegionIntegration, testAccountIDIntegration, topicName)
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	// Create the topic directly via the real SDK, with no ownership tags at
	// all — simulating a resource that already existed before this CR ever
	// reconciled.
	if _, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicName}); err != nil {
		t.Fatalf("setting up pre-existing real topic: %v", err)
	}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &topicArn})
	if err != nil {
		t.Fatalf("real ListTagsForResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.Tags), namespace, crName, "uid-1") {
		t.Errorf("expected adopt:true to tag the pre-existing real topic as owned, got tags %+v", tagsOut.Tags)
	}
}

func TestIntegration_Ensure_CorrectsAttributeDriftOnRealTopic(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	topicName := cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256-len(".fifo")) + ".fifo"
	// createTaggedTopic's plain CreateTopic won't do here - a FIFO topic
	// needs the FifoTopic attribute set at creation, which a bare Name-only
	// call doesn't provide.
	createOut, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicName, Attributes: map[string]string{"FifoTopic": "true"}})
	if err != nil {
		t.Fatalf("real CreateTopic() (setup) error = %v", err)
	}
	topicArn := *createOut.TopicArn
	if _, err := client.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &topicArn, Tags: mapToTags(map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: "uid-1",
	})}); err != nil {
		t.Fatalf("real TagResource() (setup) error = %v", err)
	}
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	dedup := false
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{
			Name: "events", FIFO: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true,
			Overrides: &depsv1alpha1.SNSOverrides{ContentBasedDeduplication: &dedup},
		},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	dedup = true
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, ledger, nil, nil); err != nil {
		t.Fatalf("drift-correcting Ensure() error = %v", err)
	}

	attrsOut, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn})
	if err != nil {
		t.Fatalf("real GetTopicAttributes() error = %v", err)
	}
	if got := attrsOut.Attributes["ContentBasedDeduplication"]; got != "true" {
		t.Errorf("real ContentBasedDeduplication after drift correction = %q, want true", got)
	}
}

func TestIntegration_Cleanup_DeletesRealTopicImmediatelyWhenForced(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	topicName := cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256)
	topicArn := createTaggedTopic(t, client, namespace, crName, "uid-1", topicName)
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", testRegionIntegration, testAccountIDIntegration, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", spec, ledger, true, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger entry to be removed, got %+v", ledger)
	}

	if _, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn}); err == nil {
		t.Error("expected the real topic to be gone after Cleanup, but GetTopicAttributes succeeded")
	}
}
