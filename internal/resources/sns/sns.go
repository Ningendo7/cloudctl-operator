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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// snsAPI is the subset of the SNS client this package needs. It's a type
// alias to the exported cloudctlaws.SNSClient rather than its own separate
// interface, since that interface now has two consumers — this package's
// own unit tests, and controller-level envtest suites that need to fake the
// whole AWS backend to exercise Reconcile() end-to-end. Aliasing keeps every
// reference to snsAPI in this file unchanged (same pattern as sqsAPI).
type snsAPI = cloudctlaws.SNSClient

const resourceType = "sns"

// Ensure reconciles every declared SNS topic against AWS, updating the
// ownership ledger as it goes.
//
// Known gaps, deliberately deferred (same pattern as sqs's initial pass):
// drift correction on an existing topic's attributes, and the trust-window
// skip-on-recheck optimization.
func Ensure(
	ctx context.Context,
	client snsAPI,
	namespace,
	crName,
	crUID,
	region,
	accountID string,
	spec *depsv1alpha1.SNSSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, t := range spec.Resources {
		var err error
		ledger, err = ensureTopic(ctx, client, namespace, crName, crUID, region, accountID, t, ledger)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("topic %q: %w", t.Name, err)
		}
	}
	return ledger, firstErr
}

func ensureTopic(
	ctx context.Context,
	client snsAPI,
	namespace,
	crName,
	crUID,
	region,
	accountID string,
	t depsv1alpha1.SNSTopicSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	topicName := cloudctlaws.ResourceName(namespace, crName, t.Name)
	if t.FIFO {
		topicName += ".fifo"
	}
	if err := cloudctlaws.ValidateNameLength(topicName, 256, "SNS topic"); err != nil {
		return ledger, err
	}
	topicArn := cloudctlaws.TopicARN(region, accountID, topicName)

	ownerTags := map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}

	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: &topicArn,
	})

	var notFound *types.NotFoundException
	if errors.As(err, &notFound) {
		attrs := map[string]string{}
		if t.FIFO {
			attrs["FifoTopic"] = "true"
			dedup := "false"
			if t.Overrides != nil && t.Overrides.ContentBasedDeduplication != nil && *t.Overrides.ContentBasedDeduplication {
				dedup = "true"
			}
			attrs["ContentBasedDeduplication"] = dedup
		}

		createOut, cErr := client.CreateTopic(ctx, &sns.CreateTopicInput{
			Name:       &topicName,
			Attributes: attrs,
			Tags:       mapToTags(ownerTags),
		})
		if cErr != nil {
			return ledger, wrapAWSError(cErr, "creating topic")
		}
		return recordVerified(ledger, t.Name, *createOut.TopicArn, t.DeletionPolicy, t.Force), nil
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up topic")

	}

	// Topic already exists — verify we actually own it before trusting it.
	currentTags := tagsToMap(tagsOut.Tags)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		if existingOwner, ok := currentTags[cloudctlaws.OwnerTagKey]; ok && existingOwner != cloudctlaws.OwnerTagValue(namespace, crName) {
			return ledger, fmt.Errorf("topic %q is already owned by a different AppDependencies CR (%s) — this looks like a naming collision, not adopting", topicName, existingOwner)
		}
		if !t.Adopt {
			return ledger, fmt.Errorf("topic %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", topicName)
		}

		merged := cloudctlaws.MergeTags(currentTags, ownerTags)
		if _, tagErr := client.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &topicArn, Tags: mapToTags(merged)}); tagErr != nil {
			return ledger, wrapAWSError(tagErr, "adopting topic (tagging)")
		}
	}

	if err := reconcileTopicAttributes(ctx, client, topicArn, t); err != nil {
		return ledger, wrapAWSError(err, "reconciling topic attributes")
	}

	return recordVerified(ledger, t.Name, topicArn, t.DeletionPolicy, t.Force), nil
}

// reconcileTopicAttributes corrects drift on ContentBasedDeduplication, the
// only mutable attribute this package currently exposes (confirmed
// mutable via SetTopicAttributes per AWS's docs). Only meaningful for FIFO
// topics — standard topics don't have this attribute at all.
func reconcileTopicAttributes(ctx context.Context, client snsAPI, topicArn string, t depsv1alpha1.SNSTopicSpec) error {
	if !t.FIFO {
		return nil
	}

	desired := "false"
	if t.Overrides != nil && t.Overrides.ContentBasedDeduplication != nil && *t.Overrides.ContentBasedDeduplication {
		desired = "true"
	}

	current, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: &topicArn,
	})
	if err != nil {
		return fmt.Errorf("reading current attributes: %w", err)
	}
	if current.Attributes["ContentBasedDeduplication"] == desired {
		return nil
	}

	attrName := "ContentBasedDeduplication"
	_, err = client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
		TopicArn:       &topicArn,
		AttributeName:  &attrName,
		AttributeValue: &desired,
	})
	return err
}

func recordVerified(ledger []depsv1alpha1.ManagedResource, ledgerName, arn string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	if existing := status.FindManagedResource(ledger, resourceType, ledgerName); existing != nil {
		createdAt = existing.CreatedAt
	}

	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           ledgerName,
		ARN:            arn,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		DeletionPolicy: deletionPolicy,
		Force:          force,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
	})
	return ledger
}

// mapToTags/tagsToMap bridge our map[string]string tag model (shared with
// sqs via internal/aws's tag helpers) and SNS's []types.Tag shape - unlike
// SQS, SNS's tagging APIs use a slice of {Key,Value} structs, not a map.
func mapToTags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{Key: &k, Value: &v})
	}
	return tags
}

func tagsToMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func wrapAWSError(err error, context string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", context, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
