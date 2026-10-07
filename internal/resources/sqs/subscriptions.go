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
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const resourceTypeSNS = "sns"
const subscriptionResourceType = "sqssubscription"
const subscriptionLedgerSeparator = "/subscribes-to/"

// subscriptionLedgerName derives a deterministic, unique ledger key for
// one (queue, topic-reference) pair - unique per queue (the prefix
// CleanupSubscriptions matches on to scope itself to one queue's own
// entries) and per ref (so a queue subscribed to two topics gets two
// independent ledger entries).
func subscriptionLedgerName(queueName string, ref depsv1alpha1.ConsumeRef) string {
	return queueName + subscriptionLedgerSeparator + ref.Namespace + "/" + ref.Name + "/" + ref.ResourceName
}

// subscriptionAllowSid derives the queue resource-policy Sid for one
// subscription from its own ledger name, rather than from the ConsumeRef
// separately - so Ensure and Cleanup always compute the identical Sid
// from the same string, with nothing to keep in sync by hand.
func subscriptionAllowSid(ledgerName string) string {
	return "cloudctl-sub-" + strings.ReplaceAll(ledgerName, "/", "-")
}

// resolveTopicARN resolves a ConsumeRef against a producer's sns section,
// authorized through sharedWith exactly like any other cross-CR
// reference. Builds a minimal stand-in CR from just namespace/name -
// resolveConsume (via iam.ResolveConsumeARN) only ever reads those two
// fields off the consumer - the same pattern kms.ResolveSharedKeyARN
// already establishes for this package's other cross-CR reference
// (encryption.kmsKeyRef), reused here rather than threading the whole CR
// object through Ensure/Cleanup's per-queue call chain.
func resolveTopicARN(ctx context.Context, k8sClient client.Client, namespace, crName string, ref depsv1alpha1.ConsumeRef) (string, bool) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName}}
	return iam.ResolveConsumeARN(ctx, k8sClient, consumer, resourceTypeSNS, ref)
}

// EnsureSubscriptions creates a real SNS subscription (protocol "sqs")
// for each subscribesTo entry that's currently authorized, and grants
// the matching queue resource-policy statement so delivery actually
// works - an SQS-endpoint subscription needs both halves; AWS doesn't
// fail the Subscribe call if the policy grant is missing, it just
// silently drops every message the topic tries to deliver.
//
// An entry that doesn't resolve yet (producer CR not created, topic not
// reconciled there yet, sharedWith not granted) is skipped silently, the
// same forward-reference tolerance as collectGrants - the next reconcile
// picks it up once it resolves. An already-ledger-tracked entry is not
// re-subscribed, but the queue policy grant is reasserted every
// reconcile regardless, correcting any out-of-band drift on it cheaply.
func EnsureSubscriptions(
	ctx context.Context,
	snsClient cloudctlaws.SNSClient,
	sqsClient sqsAPI,
	k8sClient client.Client,
	namespace, crName, queueName, queueURL, queueARN string,
	subscribesTo []depsv1alpha1.ConsumeRef,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	var firstErr error
	for _, ref := range subscribesTo {
		topicARN, ok := resolveTopicARN(ctx, k8sClient, namespace, crName, ref)
		if !ok {
			continue
		}

		ledgerName := subscriptionLedgerName(queueName, ref)
		if status.FindManagedResource(ledger, subscriptionResourceType, ledgerName) == nil {
			protocol := "sqs"
			endpoint := queueARN
			topicARNCopy := topicARN
			out, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: &topicARNCopy,
				Protocol: &protocol,
				Endpoint: &endpoint,
			})
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("subscribing queue %q to %s/%s/%s: %w", queueName, ref.Namespace, ref.Name, ref.ResourceName, err)
				}
				continue
			}
			status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
				Type:           subscriptionResourceType,
				Name:           ledgerName,
				ARN:            *out.SubscriptionArn,
				DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				State:          depsv1alpha1.ManagedResourceStateVerified,
				CreatedAt:      metav1.Now(),
			})
			if recordEvent != nil {
				recordEvent(
					"Normal",
					"SubscriptionCreated",
					fmt.Sprintf("Subscribed queue %s to %s/%s/%s", queueName, ref.Namespace, ref.Name, ref.ResourceName),
				)
			}
		}

		if err := addSubscriptionAllow(ctx, sqsClient, queueURL, queueARN, topicARN, subscriptionAllowSid(ledgerName)); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("granting %s/%s/%s send access to queue %q: %w", ref.Namespace, ref.Name, ref.ResourceName, queueName, err)
			}
		}
	}
	return ledger, firstErr
}

// CleanupSubscriptions unsubscribes and revokes the queue-policy grant
// for any ledger-tracked subscription belonging to this queue that's
// either no longer declared in subscribesTo, or no longer authorized
// (the producer's sharedWith was revoked) - re-checked here, not just in
// Ensure, because a subscription is an active delivery path: unlike an
// IAM grant, which just stops being offered, a revoked subscription has
// to be actively torn down or the consumer keeps receiving messages it's
// no longer allowed to see.
func CleanupSubscriptions(
	ctx context.Context,
	snsClient cloudctlaws.SNSClient,
	sqsClient sqsAPI,
	k8sClient client.Client,
	namespace, crName, queueName, queueURL string,
	subscribesTo []depsv1alpha1.ConsumeRef,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	authorized := map[string]bool{}
	for _, ref := range subscribesTo {
		if _, ok := resolveTopicARN(ctx, k8sClient, namespace, crName, ref); ok {
			authorized[subscriptionLedgerName(queueName, ref)] = true
		}
	}

	prefix := queueName + subscriptionLedgerSeparator
	var firstErr error
	for _, entry := range ledger {
		if entry.Type != subscriptionResourceType || !strings.HasPrefix(entry.Name, prefix) {
			continue
		}
		if authorized[entry.Name] {
			continue
		}

		arn := entry.ARN
		if _, err := snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: &arn}); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("unsubscribing %q: %w", entry.Name, err)
			}
			continue
		}
		if err := removeSubscriptionAllow(ctx, sqsClient, queueURL, subscriptionAllowSid(entry.Name)); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("revoking queue policy grant for %q: %w", entry.Name, err)
			}
			continue
		}
		status.RemoveManagedResource(&ledger, subscriptionResourceType, entry.Name)
		if recordEvent != nil {
			recordEvent("Normal", "SubscriptionRemoved", fmt.Sprintf("Unsubscribed queue %s: %s no longer declared or authorized", queueName, entry.Name))
		}
	}
	return ledger, firstErr
}

// subscriptionAllowStatement grants one SNS topic permission to deliver
// into this queue - scoped by SourceArn so a grant for one topic can
// never be (ab)used by a different one.
type subscriptionAllowStatement struct {
	Sid       string                       `json:"Sid"`
	Effect    string                       `json:"Effect"`
	Principal map[string]string            `json:"Principal"`
	Action    []string                     `json:"Action"`
	Resource  string                       `json:"Resource"`
	Condition map[string]map[string]string `json:"Condition"`
}

// addSubscriptionAllow merges an Allow statement for sqs:SendMessage,
// scoped to the one topic via a SourceArn condition, into the queue's
// resource policy - the delivery half of an SNS->SQS subscription; AWS
// doesn't fail Subscribe if this is missing, it just silently drops
// every message. Idempotent under the same Sid, like
// addPendingDeletionDeny.
func addSubscriptionAllow(ctx context.Context, sqsClient sqsAPI, queueURL, queueArn, topicArn, sid string) error {
	doc, err := readPolicy(ctx, sqsClient, queueURL)
	if err != nil {
		return err
	}
	doc.Statement = removeStatementBySid(doc.Statement, sid)
	stmt, err := json.Marshal(subscriptionAllowStatement{
		Sid:       sid,
		Effect:    "Allow",
		Principal: map[string]string{"Service": "sns.amazonaws.com"},
		Action:    []string{"sqs:SendMessage"},
		Resource:  queueArn,
		Condition: map[string]map[string]string{"ArnEquals": {"aws:SourceArn": topicArn}},
	})
	if err != nil {
		return fmt.Errorf("encoding subscription allow statement: %w", err)
	}
	doc.Statement = append(doc.Statement, stmt)
	return writePolicy(ctx, sqsClient, queueURL, doc)
}

// removeSubscriptionAllow removes exactly the Sid addSubscriptionAllow
// added, leaving any other pre-existing policy statements (including
// other topics' own grants) untouched.
func removeSubscriptionAllow(ctx context.Context, sqsClient sqsAPI, queueURL, sid string) error {
	doc, err := readPolicy(ctx, sqsClient, queueURL)
	if err != nil {
		return err
	}
	before := len(doc.Statement)
	doc.Statement = removeStatementBySid(doc.Statement, sid)
	if len(doc.Statement) == before {
		return nil
	}
	if len(doc.Statement) == 0 {
		_, err := sqsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl:   &queueURL,
			Attributes: map[string]string{string(types.QueueAttributeNamePolicy): ""},
		})
		return err
	}
	return writePolicy(ctx, sqsClient, queueURL, doc)
}
