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

package sns

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func newSchemeForKMSKeyRefTest(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func TestEnsure_CreatesNewTopic(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{
		Resources: []depsv1alpha1.SNSTopicSpec{
			{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
		},
	}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry := status.FindManagedResource(ledger, "sns", "events")
	if entry == nil {
		t.Fatal("expected a ledger entry for events")
	}
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected Verified state, got %s", entry.State)
	}

	wantName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	wantArn := cloudctlaws.TopicARN(testRegion, testAccountID, wantName)
	if _, ok := client.topics[wantArn]; !ok {
		t.Errorf("expected topic %q to have been created", wantArn)
	}
	if entry.ARN != wantArn {
		t.Errorf("expected ledger ARN %q, got %q", wantArn, entry.ARN)
	}
}

func TestEnsure_ProvisionsDedicatedKeyWhenEncryptionEnabled(t *testing.T) {
	client := newFakeSNS()
	kmsClient := newFakeKMSClient()
	spec := &depsv1alpha1.SNSSpec{
		Resources: []depsv1alpha1.SNSTopicSpec{
			{Name: "events", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
		},
	}

	ledger, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", "events-key")
	if keyEntry == nil {
		t.Fatal("expected a dedicated KMS key ledger entry named \"events-key\"")
	}

	wantName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	wantArn := cloudctlaws.TopicARN(testRegion, testAccountID, wantName)
	topic, ok := client.topics[wantArn]
	if !ok {
		t.Fatal("expected the topic to have been created")
	}
	if topic.attributes["KmsMasterKeyId"] != keyEntry.ARN {
		t.Errorf("KmsMasterKeyId = %q, want %q", topic.attributes["KmsMasterKeyId"], keyEntry.ARN)
	}
}

func TestEnsure_CorrectsKmsMasterKeyIdDriftOnExistingTopic(t *testing.T) {
	client := newFakeSNS()
	kmsClient := newFakeKMSClient()
	topicName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, topicName)
	client.topics[topicArn] = &fakeTopic{
		arn:        topicArn,
		tags:       map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("default", "checkout-service"), cloudctlaws.OwnerUIDTagKey: "uid-1"},
		attributes: map[string]string{},
	}
	spec := &depsv1alpha1.SNSSpec{
		Resources: []depsv1alpha1.SNSTopicSpec{
			{Name: "events", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
		},
	}

	ledger, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	keyEntry := status.FindManagedResource(ledger, "kms", "events-key")
	if client.topics[topicArn].attributes["KmsMasterKeyId"] != keyEntry.ARN {
		t.Errorf("expected drift correction to set KmsMasterKeyId to %q, got %q", keyEntry.ARN, client.topics[topicArn].attributes["KmsMasterKeyId"])
	}
}

func TestEnsure_KMSKeyRefRetriesWhenNotYetAuthorized(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{
		Resources: []depsv1alpha1.SNSTopicSpec{
			{Name: "events", Encryption: &depsv1alpha1.EncryptionSpec{
				KMSKeyRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"},
			}},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).Build()

	_, err := Ensure(context.Background(), client, nil, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error - the producer CR doesn't exist yet")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError (self-resolving forward reference), got %v", err)
	}
}

func TestEnsure_KMSKeyRefResolvesWhenAuthorized(t *testing.T) {
	client := newFakeSNS()
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
				{Name: "shared-key", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "default", Name: "checkout-service"},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "kms", Name: "shared-key", ARN: "arn:aws:kms:us-east-1:123456789012:key/shared-id"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()
	spec := &depsv1alpha1.SNSSpec{
		Resources: []depsv1alpha1.SNSTopicSpec{
			{Name: "events", Encryption: &depsv1alpha1.EncryptionSpec{
				KMSKeyRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"},
			}},
		},
	}

	_, err := Ensure(context.Background(), client, nil, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	wantName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	wantArn := cloudctlaws.TopicARN(testRegion, testAccountID, wantName)
	topic, ok := client.topics[wantArn]
	if !ok {
		t.Fatal("expected the topic to have been created")
	}
	if got := topic.attributes["KmsMasterKeyId"]; got != "arn:aws:kms:us-east-1:123456789012:key/shared-id" {
		t.Errorf("KmsMasterKeyId = %q, want the shared key's ARN", got)
	}
}

func TestEnsure_IsIdempotentAndPreservesCreatedAt(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	firstCreatedAt := status.FindManagedResource(ledger, "sns", "events").CreatedAt

	ledger, err = Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(client.topics) != 1 {
		t.Fatalf("expected exactly one topic after re-reconciling, got %d", len(client.topics))
	}
	got := status.FindManagedResource(ledger, "sns", "events").CreatedAt
	if !got.Equal(&firstCreatedAt) {
		t.Errorf("expected CreatedAt to be preserved across reconciles, got %v want %v", got, firstCreatedAt)
	}
}

func TestEnsure_RefusesUnownedExistingTopic(t *testing.T) {
	client := newFakeSNS()
	topicName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, topicName)
	client.topics[topicArn] = &fakeTopic{arn: topicArn, tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error when a same-named topic exists without our ownership tag")
	}
}

func TestEnsure_AdoptsUntaggedTopic(t *testing.T) {
	client := newFakeSNS()
	topicName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, topicName)
	client.topics[topicArn] = &fakeTopic{arn: topicArn, tags: map[string]string{"cost-center": "1234"}}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", Adopt: true}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("expected adoption to succeed, got error: %v", err)
	}

	tags := client.topics[topicArn].tags
	if tags["cost-center"] != "1234" {
		t.Errorf("expected pre-existing tags to be preserved through adoption, got %v", tags)
	}
	if !cloudctlaws.IsOwnedBy(tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the topic to be tagged as owned by this CR after adoption")
	}
}

func TestEnsure_RefusesAdoptingTopicOwnedByDifferentCR(t *testing.T) {
	client := newFakeSNS()
	topicName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, topicName)
	client.topics[topicArn] = &fakeTopic{arn: topicArn, tags: map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "some-other-cr"),
		cloudctlaws.OwnerUIDTagKey: "different-uid",
	}}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", Adopt: true}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to never override a topic already owned by a different AppDependencies CR")
	}
}

func TestEnsure_CreatesFIFOTopicWithSuffixAndAttribute(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", FIFO: true}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	fifoName := cloudctlaws.ResourceName("default", "checkout-service", "events") + ".fifo"
	fifoArn := cloudctlaws.TopicARN(testRegion, testAccountID, fifoName)
	topic, ok := client.topics[fifoArn]
	if !ok {
		t.Fatalf("expected FIFO topic %q to have been created", fifoArn)
	}
	if topic.attributes["FifoTopic"] != "true" {
		t.Errorf("expected FifoTopic attribute to be set, got attributes=%v", topic.attributes)
	}
}

func TestEnsure_ClassifiesTransientAWSErrorsAsRetryable(t *testing.T) {
	client := newFakeSNS()
	client.listTagsForResourceErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultClient}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error when the tag lookup fails")
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
	client := newFakeSNS()
	client.listTagsForResourceErr = &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error when the tag lookup fails")
	}

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if reconcileErr.Retryable {
		t.Error("expected a permission-denied error to be classified as not retryable — retrying won't fix an IAM gap")
	}
}

func TestEnsure_PropagatesCreateTopicErrors(t *testing.T) {
	client := newFakeSNS()
	client.createTopicErr = &fakeAWSError{code: "InternalError", fault: smithy.FaultServer}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error when topic creation fails")
	}

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if !reconcileErr.Retryable {
		t.Error("expected a server-fault error to be classified as retryable")
	}
}

func TestEnsure_CorrectsContentBasedDeduplicationDriftOnExistingFIFOTopic(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", FIFO: true}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	fifoArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events")+".fifo")
	if got := client.topics[fifoArn].attributes["ContentBasedDeduplication"]; got != "false" {
		t.Fatalf("test setup broken: expected ContentBasedDeduplication=false initially, got %q", got)
	}

	dedup := true
	spec.Resources[0].Overrides = &depsv1alpha1.SNSOverrides{ContentBasedDeduplication: &dedup}
	_, err = Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if got := client.topics[fifoArn].attributes["ContentBasedDeduplication"]; got != "true" {
		t.Errorf("expected ContentBasedDeduplication drift to be corrected to true, got %q", got)
	}
}

func TestEnsure_SkipsAttributeDriftForNonFIFOTopics(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	if _, ok := client.topics[topicArn].attributes["ContentBasedDeduplication"]; ok {
		t.Error("expected no ContentBasedDeduplication attribute to be touched for a non-FIFO topic")
	}
}

func TestEnsure_ContinuesToOtherTopicsAfterOneFails(t *testing.T) {
	// Regression test: a failure on one declared topic must not prevent an
	// unrelated topic later in the same list from being attempted.
	client := newFakeSNS()
	badTopicName := cloudctlaws.ResourceName("default", "checkout-service", "orders-events")
	badArn := cloudctlaws.TopicARN(testRegion, testAccountID, badTopicName)
	client.topics[badArn] = &fakeTopic{arn: badArn, tags: map[string]string{"team": "someone-else"}}

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "orders-events"}, // fails: owned by nobody we recognize, no adopt
		{Name: "user-events"},   // unrelated, should still succeed
	}}

	ledger, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error from the failing topic")
	}

	goodArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "user-events"))
	if _, ok := client.topics[goodArn]; !ok {
		t.Error("expected the second topic to still be created despite the first one failing")
	}
	if status.FindManagedResource(ledger, "sns", "user-events") == nil {
		t.Error("expected a ledger entry for the successfully-created second topic")
	}
}

func TestEnsure_RejectsTopicNameExceedingSNSLimit(t *testing.T) {
	// SNS's 256-character limit is generous enough that realistic names
	// won't hit it the way SQS's 80-character limit does - this needs a
	// deliberately long resource key to actually exercise the check.
	client := newFakeSNS()
	tooLongKey := strings.Repeat("a", 250)
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: tooLongKey}}}

	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err == nil {
		t.Fatal("expected an error for a computed topic name exceeding SNS's 256-character limit")
	}
	if len(client.topics) != 0 {
		t.Error("expected no AWS call to have been attempted for a name that's already known to be too long")
	}
}
