//go:build live

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

// Live tests run Subscribe/Unsubscribe and the queue policy grant against
// a real AWS account - no LocalStack. The integration tier
// (subscriptions_integration_test.go) already proves Subscribe and the
// queue policy grant interoperate against LocalStack's own simulation;
// this tier catches the case where that simulation itself diverges from
// real SNS/SQS. Skipped entirely unless real credentials resolve via the
// standard AWS credential chain. Run explicitly with whatever already
// authenticates your AWS CLI:
//
//	go test -tags=live ./internal/resources/sqs/... -run Subscription -v
package sqs

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func newLiveSNSClient(t *testing.T) *sns.Client {
	t.Helper()
	return sns.NewFromConfig(skipUnlessLiveAWSCredentials(t))
}

// liveSubscriptionFixture mirrors subscriptionFixture from the integration
// tier, but against real AWS: a real queue, a real topic, and an
// authorized producer CR resolved through a fake controller-runtime
// client (that resolution logic has no AWS dependency to verify here
// either).
type liveSubscriptionFixture struct {
	sqsClient                     *sqs.Client
	snsClient                     *sns.Client
	k8sClient                     client.Client
	queueName, queueURL, queueARN string
	topicARN                      string
	ref                           depsv1alpha1.ConsumeRef
}

func setupLiveSubscriptionFixture(t *testing.T) liveSubscriptionFixture {
	t.Helper()
	ctx := context.Background()
	sqsClient := newLiveClient(t)
	snsClient := newLiveSNSClient(t)

	queueName := "live-" + liveUniqueSuffix(t) + "-orders"
	createQueueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real CreateQueue() (setup) error = %v", err)
	}
	queueURL := *createQueueOut.QueueUrl
	t.Cleanup(func() { _, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: &queueURL}) })

	attrsOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() (setup) error = %v", err)
	}
	queueARN := attrsOut.Attributes[string(types.QueueAttributeNameQueueArn)]

	topicName := "live-" + liveUniqueSuffix(t) + "-events"
	createTopicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicName})
	if err != nil {
		t.Fatalf("real CreateTopic() (setup) error = %v", err)
	}
	topicARN := *createTopicOut.TopicArn
	t.Cleanup(func() { _, _ = snsClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: &topicARN}) })

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events", topicARN, "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	return liveSubscriptionFixture{
		sqsClient: sqsClient,
		snsClient: snsClient,
		k8sClient: k8sClient,
		queueName: queueName,
		queueURL:  queueURL,
		queueARN:  queueARN,
		topicARN:  topicARN,
		ref:       ref,
	}
}

func TestLive_EnsureSubscriptions_RealSubscribeAndQueuePolicyGrant(t *testing.T) {
	ctx := context.Background()
	f := setupLiveSubscriptionFixture(t)

	ledger, err := EnsureSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL, f.queueARN,
		[]depsv1alpha1.ConsumeRef{f.ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}
	ledgerName := subscriptionLedgerName(f.queueName, f.ref)
	entry := status.FindManagedResource(ledger, subscriptionResourceType, ledgerName)
	if entry == nil {
		t.Fatal("expected a ledger entry for the new subscription")
	}

	subsOut, err := f.snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &f.topicARN})
	if err != nil {
		t.Fatalf("real ListSubscriptionsByTopic() error = %v", err)
	}
	if len(subsOut.Subscriptions) != 1 {
		t.Fatalf("expected exactly one real subscription on the topic, got %d", len(subsOut.Subscriptions))
	}
	if *subsOut.Subscriptions[0].SubscriptionArn != entry.ARN {
		t.Errorf("ledger ARN %q doesn't match the real subscription's ARN %q", entry.ARN, *subsOut.Subscriptions[0].SubscriptionArn)
	}

	policyOut, err := f.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &f.queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes(Policy) error = %v", err)
	}
	policy := policyOut.Attributes[string(types.QueueAttributeNamePolicy)]
	if policy == "" {
		t.Fatal("expected a real queue policy granting the topic send access, got none")
	}
	if !strings.Contains(policy, f.topicARN) {
		t.Errorf("expected the queue policy to scope the grant to topic ARN %q, got: %s", f.topicARN, policy)
	}
}

func TestLive_CleanupSubscriptions_RealUnsubscribeAndPolicyRevoke(t *testing.T) {
	ctx := context.Background()
	f := setupLiveSubscriptionFixture(t)

	ledger, err := EnsureSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL, f.queueARN,
		[]depsv1alpha1.ConsumeRef{f.ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() (setup) error = %v", err)
	}

	// subscribesTo is now empty - the ref was removed from spec.
	ledger, err = CleanupSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL,
		nil, ledger, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() error = %v", err)
	}

	ledgerName := subscriptionLedgerName(f.queueName, f.ref)
	if status.FindManagedResource(ledger, subscriptionResourceType, ledgerName) != nil {
		t.Error("expected the ledger entry to be removed")
	}

	// Real SNS unsubscribe is asynchronous - poll briefly rather than
	// asserting immediately, the same pattern used elsewhere in this
	// tier for other eventually-consistent AWS behavior.
	var subsOut *sns.ListSubscriptionsByTopicOutput
	deadline := time.Now().Add(30 * time.Second)
	for {
		var listErr error
		subsOut, listErr = f.snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &f.topicARN})
		if listErr != nil {
			t.Fatalf("real ListSubscriptionsByTopic() error = %v", listErr)
		}
		if len(subsOut.Subscriptions) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if len(subsOut.Subscriptions) != 0 {
		t.Errorf("expected the real subscription to be gone, got %d still on the topic", len(subsOut.Subscriptions))
	}

	policyOut, err := f.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &f.queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes(Policy) error = %v", err)
	}
	if policy := policyOut.Attributes[string(types.QueueAttributeNamePolicy)]; policy != "" && strings.Contains(policy, f.topicARN) {
		t.Errorf("expected the queue policy grant for %q to be revoked, got: %s", f.topicARN, policy)
	}
}
