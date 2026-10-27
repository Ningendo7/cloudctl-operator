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

package controller

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/alarms"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const conditionTypeKeyPolicyReady = "KeyPolicyReady"

// keyPolicySection keeps what AWS services may do with this CR's keys in step
// with what is encrypted with them. Registered after the resource sections,
// so the subscriptions and topics it names exist in the same pass: a queue
// encrypted with a key needs SNS to use that key the moment a topic delivers
// into it.
func keyPolicySection(r *AppDependenciesReconciler) section {
	return section{
		name: conditionTypeKeyPolicyReady,
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			keys := managedKeyARNs(cr)
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, resourceTypeKMS, len(keys))
			defer cancel()

			err := syncKeyPolicies(ctx, r, cr, keys)
			setSectionCondition(ctx, cr, conditionTypeKeyPolicyReady, err, eventRecorderFor(r, cr))
			return err
		},
		// A key's statements go when the operator lets go of the key, in the
		// kms section's own cleanup.
		finalize: func(context.Context, *depsv1alpha1.AppDependencies) (bool, []string, error) {
			return true, nil, nil
		},
	}
}

// managedKeyARNs lists this CR's keys the operator still manages and can
// still change: not released, and not already scheduled for deletion.
func managedKeyARNs(cr *depsv1alpha1.AppDependencies) []string {
	var arns []string
	for _, e := range cr.Status.ManagedResources {
		if e.Type == resourceTypeKMS && e.ARN != "" && e.State != depsv1alpha1.ManagedResourceStateRelinquished && e.PendingDeletionSince == nil {
			arns = append(arns, e.ARN)
		}
	}
	return arns
}

// syncKeyPolicies gives every key exactly the service statements the
// resources encrypted with it need. If what they need cannot be worked out,
// nothing is written: guessing "nothing" would take away a grant that is
// delivering messages right now.
func syncKeyPolicies(ctx context.Context, r *AppDependenciesReconciler, cr *depsv1alpha1.AppDependencies, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	grants, err := keyServiceGrants(ctx, r.Client, cr, r.AWSClients.Region, r.AWSClients.AccountID)
	if err != nil {
		return err
	}
	var firstErr error
	for _, arn := range keys {
		if err := kms.SyncServiceGrants(ctx, r.AWSClients.KMS, arn, grants[arn], eventRecorderFor(r, cr)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// keyServiceGrants works out, per key ARN, which topics SNS delivers from
// into queues encrypted with it, and which alarms CloudWatch publishes from
// to topics encrypted with it.
func keyServiceGrants(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies, region, accountID string) (map[string]kms.ServiceGrants, error) {
	grants := map[string]kms.ServiceGrants{}
	for _, res := range encryptedResources(cr) {
		key, ok := status.FindManagedResource(cr.Status.ManagedResources, resourceTypeKMS, kmsKeyLedgerName(res))
		if !ok || key.ARN == "" {
			continue
		}
		g := grants[key.ARN]
		switch res.resourceType {
		case resourceTypeSQS:
			topics, err := queueSourceTopics(ctx, k8sClient, cr, res.name)
			if err != nil {
				return nil, err
			}
			g.SNSTopicARNs = append(g.SNSTopicARNs, topics...)
		case resourceTypeSNS:
			alarmARNs, err := topicPublishingAlarms(ctx, k8sClient, cr, res.name, region, accountID)
			if err != nil {
				return nil, err
			}
			g.AlarmARNs = append(g.AlarmARNs, alarmARNs...)
		}
		grants[key.ARN] = g
	}
	return grants, nil
}

// queueSourceTopics returns the topics a queue subscribes to that are
// authorized and exist, so SNS can use the queue's key for exactly those. A
// reference that does not resolve yet is not an error: no subscription exists
// for it, so nothing delivers.
func queueSourceTopics(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies, queueName string) ([]string, error) {
	var topics []string
	for _, q := range cr.Spec.SQS.Resources {
		if q.Name != queueName {
			continue
		}
		for _, ref := range q.SubscribesTo {
			arn, ok, err := iam.ResolveConsumeARN(ctx, k8sClient, cr, resourceTypeSNS, ref)
			if err != nil {
				return nil, err
			}
			if ok {
				topics = append(topics, arn)
			}
		}
	}
	return topics, nil
}

// topicPublishingAlarms returns the alarms that publish to a topic: this CR's
// own, when its alarms.snsTopicRef names the topic, and those of every CR the
// topic is shared with that names it. The alarm and the topic are usually in
// different CRs, and it is the topic's key that must let CloudWatch in.
func topicPublishingAlarms(ctx context.Context, k8sClient client.Client, owner *depsv1alpha1.AppDependencies, topicName, region, accountID string) ([]string, error) {
	self := depsv1alpha1.ConsumeRef{Namespace: owner.Namespace, Name: owner.Name, ResourceName: topicName}
	publishes := func(c *depsv1alpha1.AppDependencies) bool {
		a := c.Spec.Alarms
		return a != nil && a.Enabled && a.SnsTopicRef != nil && *a.SnsTopicRef == self
	}
	alarmARNsOf := func(c *depsv1alpha1.AppDependencies) []string {
		return alarms.AlarmARNs(region, accountID, c.Namespace, c.Name, c.Spec.Alarms, c.Spec.SQS, c.Spec.SNS, c.Spec.DynamoDB)
	}

	var arns []string
	if publishes(owner) {
		arns = append(arns, alarmARNsOf(owner)...)
	}
	for _, grantee := range sharedWithOf(owner, topicName) {
		if grantee.Namespace == owner.Namespace && grantee.Name == owner.Name {
			continue
		}
		var consumer depsv1alpha1.AppDependencies
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: grantee.Namespace, Name: grantee.Name}, &consumer)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, &cloudctlaws.ReconcileError{Err: fmt.Errorf("reading %s/%s, which may publish alarms to topic %q: %w", grantee.Namespace, grantee.Name, topicName, err), Retryable: true}
		}
		if consumer.DeletionTimestamp.IsZero() && publishes(&consumer) {
			arns = append(arns, alarmARNsOf(&consumer)...)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(arns))), nil
}

func sharedWithOf(cr *depsv1alpha1.AppDependencies, topicName string) []depsv1alpha1.SharedWithEntry {
	if cr.Spec.SNS == nil {
		return nil
	}
	for _, t := range cr.Spec.SNS.Resources {
		if t.Name == topicName {
			return t.SharedWith
		}
	}
	return nil
}
