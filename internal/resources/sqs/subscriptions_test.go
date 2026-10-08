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
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// policyStatementCount parses a queue's resource-policy JSON (empty string
// is valid - a cleared policy) and returns how many statements it has.
func policyStatementCount(t *testing.T, policyJSON string) int {
	t.Helper()
	if policyJSON == "" {
		return 0
	}
	var doc policyDocument
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		t.Fatalf("parsing policy JSON: %v", err)
	}
	return len(doc.Statement)
}

// fakeSNSSubscriber is a minimal stand-in for cloudctlaws.SNSClient,
// implementing only Subscribe/Unsubscribe for real (every other method
// panics if called - this package's subscription logic never calls them,
// and a panic surfaces that loudly instead of a silently-wrong zero value).
type fakeSNSSubscriber struct {
	subscriptions  map[string]string // topicArn+"|"+endpoint -> subscriptionArn
	nextID         int
	subscribeErr   error
	unsubscribeErr error
	unsubscribed   []string // subscriptionArns passed to Unsubscribe, in order
}

func newFakeSNSSubscriber() *fakeSNSSubscriber {
	return &fakeSNSSubscriber{subscriptions: map[string]string{}}
}

func (f *fakeSNSSubscriber) Subscribe(_ context.Context, in *sns.SubscribeInput, _ ...func(*sns.Options)) (*sns.SubscribeOutput, error) {
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	f.nextID++
	arn := fmt.Sprintf("%s:subscription-%d", *in.TopicArn, f.nextID)
	f.subscriptions[*in.TopicArn+"|"+*in.Endpoint] = arn
	return &sns.SubscribeOutput{SubscriptionArn: &arn}, nil
}

func (f *fakeSNSSubscriber) Unsubscribe(_ context.Context, in *sns.UnsubscribeInput, _ ...func(*sns.Options)) (*sns.UnsubscribeOutput, error) {
	if f.unsubscribeErr != nil {
		return nil, f.unsubscribeErr
	}
	f.unsubscribed = append(f.unsubscribed, *in.SubscriptionArn)
	for k, v := range f.subscriptions {
		if v == *in.SubscriptionArn {
			delete(f.subscriptions, k)
		}
	}
	return &sns.UnsubscribeOutput{}, nil
}

func (f *fakeSNSSubscriber) CreateTopic(context.Context, *sns.CreateTopicInput, ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) GetTopicAttributes(context.Context, *sns.GetTopicAttributesInput, ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) SetTopicAttributes(context.Context, *sns.SetTopicAttributesInput, ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) ListTagsForResource(context.Context, *sns.ListTagsForResourceInput, ...func(*sns.Options)) (*sns.ListTagsForResourceOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) TagResource(context.Context, *sns.TagResourceInput, ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) UntagResource(context.Context, *sns.UntagResourceInput, ...func(*sns.Options)) (*sns.UntagResourceOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) DeleteTopic(context.Context, *sns.DeleteTopicInput, ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	panic("not used by subscription tests")
}
func (f *fakeSNSSubscriber) ListSubscriptionsByTopic(context.Context, *sns.ListSubscriptionsByTopicInput, ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error) {
	panic("not used by subscription tests")
}

// newSubscriptionTestQueue seeds a fake SQS queue directly (bypassing
// Ensure/CreateQueue) with a deterministic ARN, mirroring how a real
// queue would already exist by the time subscription management runs
// against it.
func newSubscriptionTestQueue(sqsClient *fakeSQS, name string) (queueURL, queueARN string) {
	queueURL = "https://sqs.us-east-1.amazonaws.com/000000000000/" + name
	queueARN = "arn:aws:sqs:us-east-1:000000000000:" + name
	sqsClient.queues[name] = &fakeQueue{url: queueURL, arn: queueARN}
	return queueURL, queueARN
}

// newAuthorizedTopicProducer builds a producer CR that owns an SNS topic
// and has granted it to the given consumer - the fixture every
// authorized-resolution test in this file starts from.
func newAuthorizedTopicProducer(producerNS, producerName, topicName, topicARN, consumerNS, consumerName string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: producerNS, Name: producerName},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
				{Name: topicName, SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: consumerNS, Name: consumerName},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sns", Name: topicName, ARN: topicARN},
			},
		},
	}
}

func TestEnsureSubscriptions_CreatesSubscriptionAndGrantsQueuePolicy(t *testing.T) {
	sqsClient := newFakeSQS()
	snsClient := newFakeSNSSubscriber()
	queueURL, queueARN := newSubscriptionTestQueue(sqsClient, "orders")

	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events",
		"arn:aws:sns:us-east-1:123456789012:events", "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	ledger, err := EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}

	ledgerName := subscriptionLedgerName("orders", ref)
	entry := status.FindManagedResource(ledger, subscriptionResourceType, ledgerName)
	if entry == nil {
		t.Fatal("expected a ledger entry for the new subscription")
	}
	if entry.ARN == "" {
		t.Error("expected the ledger entry to record the real SubscriptionArn")
	}

	q := sqsClient.queues["orders"]
	if policyStatementCount(t, q.policy) == 0 {
		t.Error("expected a queue policy statement granting the topic send access")
	}
}

func TestEnsureSubscriptions_NotYetAuthorized_SkipsSilently(t *testing.T) {
	sqsClient := newFakeSQS()
	snsClient := newFakeSNSSubscriber()
	queueURL, queueARN := newSubscriptionTestQueue(sqsClient, "orders")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).Build() // producer doesn't exist yet

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	ledger, err := EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, nil, nil)
	if err != nil {
		t.Fatalf("expected no error for an unauthorized/not-yet-resolved ref, got %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected no ledger entry to be created, got %d", len(ledger))
	}
	if len(snsClient.subscriptions) != 0 {
		t.Error("expected no real Subscribe call to have been made")
	}
}

func TestEnsureSubscriptions_AlreadySubscribed_DoesNotResubscribeButReassertsPolicy(t *testing.T) {
	sqsClient := newFakeSQS()
	snsClient := newFakeSNSSubscriber()
	queueURL, queueARN := newSubscriptionTestQueue(sqsClient, "orders")
	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events",
		"arn:aws:sns:us-east-1:123456789012:events", "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	ledger, err := EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, nil, nil)
	if err != nil {
		t.Fatalf("first EnsureSubscriptions() error = %v", err)
	}
	if len(snsClient.subscriptions) != 1 {
		t.Fatalf("expected exactly one real subscription after the first call, got %d", len(snsClient.subscriptions))
	}

	// Drift: something external wiped the queue policy. A second
	// reconcile should NOT call Subscribe again (already ledger-tracked),
	// but SHOULD repair the policy grant.
	sqsClient.queues["orders"].policy = ""
	ledger, err = EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, ledger, nil)
	if err != nil {
		t.Fatalf("second EnsureSubscriptions() error = %v", err)
	}
	if len(snsClient.subscriptions) != 1 {
		t.Errorf("expected no additional Subscribe call, got %d total subscriptions", len(snsClient.subscriptions))
	}
	if sqsClient.queues["orders"].policy == "" {
		t.Error("expected the queue policy grant to be reasserted after drift")
	}
	if len(ledger) != 1 {
		t.Errorf("expected exactly one ledger entry, got %d", len(ledger))
	}
}

func TestCleanupSubscriptions_RemovedFromSpec_Unsubscribes(t *testing.T) {
	sqsClient := newFakeSQS()
	snsClient := newFakeSNSSubscriber()
	queueURL, queueARN := newSubscriptionTestQueue(sqsClient, "orders")
	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events",
		"arn:aws:sns:us-east-1:123456789012:events", "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	ledger, err := EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}

	// subscribesTo is now empty - the ref was removed from spec.
	ledger, err = CleanupSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL,
		nil, ledger, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() error = %v", err)
	}

	if len(snsClient.unsubscribed) != 1 {
		t.Fatalf("expected exactly one Unsubscribe call, got %d", len(snsClient.unsubscribed))
	}
	ledgerName := subscriptionLedgerName("orders", ref)
	if status.FindManagedResource(ledger, subscriptionResourceType, ledgerName) != nil {
		t.Error("expected the ledger entry to be removed")
	}
	if policyStatementCount(t, sqsClient.queues["orders"].policy) != 0 {
		t.Error("expected the queue policy grant to be revoked")
	}
}

func TestCleanupSubscriptions_SharedWithRevoked_StillDeclared_Unsubscribes(t *testing.T) {
	sqsClient := newFakeSQS()
	snsClient := newFakeSNSSubscriber()
	queueURL, queueARN := newSubscriptionTestQueue(sqsClient, "orders")
	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events",
		"arn:aws:sns:us-east-1:123456789012:events", "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	ledger, err := EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}

	// Producer revokes sharedWith, but the consumer's spec still declares
	// the ref (hasn't noticed/updated yet) - must still be torn down.
	producer.Spec.SNS.Resources[0].SharedWith = nil
	if err := k8sClient.Update(context.Background(), producer); err != nil {
		t.Fatalf("revoking sharedWith: %v", err)
	}

	ledger, err = CleanupSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL,
		[]depsv1alpha1.ConsumeRef{ref}, ledger, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() error = %v", err)
	}

	if len(snsClient.unsubscribed) != 1 {
		t.Fatalf("expected the revoked subscription to be torn down, got %d Unsubscribe calls", len(snsClient.unsubscribed))
	}
	ledgerName := subscriptionLedgerName("orders", ref)
	if status.FindManagedResource(ledger, subscriptionResourceType, ledgerName) != nil {
		t.Error("expected the ledger entry to be removed")
	}
}

func TestCleanupSubscriptions_StillDeclaredAndAuthorized_LeavesAlone(t *testing.T) {
	sqsClient := newFakeSQS()
	snsClient := newFakeSNSSubscriber()
	queueURL, queueARN := newSubscriptionTestQueue(sqsClient, "orders")
	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events",
		"arn:aws:sns:us-east-1:123456789012:events", "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	ledger, err := EnsureSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL, queueARN,
		[]depsv1alpha1.ConsumeRef{ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}

	ledger, err = CleanupSubscriptions(context.Background(), snsClient, sqsClient, k8sClient,
		"default", "checkout-service", "orders", queueURL,
		[]depsv1alpha1.ConsumeRef{ref}, ledger, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() error = %v", err)
	}

	if len(snsClient.unsubscribed) != 0 {
		t.Errorf("expected the still-authorized subscription to be left alone, got %d Unsubscribe calls", len(snsClient.unsubscribed))
	}
	ledgerName := subscriptionLedgerName("orders", ref)
	if status.FindManagedResource(ledger, subscriptionResourceType, ledgerName) == nil {
		t.Error("expected the ledger entry to still be present")
	}
}

func TestSubscriptionLedgerName_UniquePerQueueAndRef(t *testing.T) {
	refA := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	refB := depsv1alpha1.ConsumeRef{Namespace: "team-c", Name: "billing-service", ResourceName: "invoices"}

	if subscriptionLedgerName("orders", refA) == subscriptionLedgerName("orders", refB) {
		t.Error("expected different refs on the same queue to produce different ledger names")
	}
	if subscriptionLedgerName("orders", refA) == subscriptionLedgerName("shipments", refA) {
		t.Error("expected the same ref on different queues to produce different ledger names")
	}
}
