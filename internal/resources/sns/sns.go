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
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
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
// ownership ledger as it goes. kmsClient is only ever touched when a
// resource actually declares encryption.enabled — a CR that never uses it
// can pass nil.
//
// Known gaps, deliberately deferred (same pattern as sqs's initial pass):
// drift correction on an existing topic's attributes, and the trust-window
// skip-on-recheck optimization.
func Ensure(
	ctx context.Context,
	client snsAPI,
	kmsClient cloudctlaws.KMSClient,
	k8sClient client.Client,
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
		ledger, err = ensureTopic(ctx, client, kmsClient, k8sClient, namespace, crName, crUID, region, accountID, t, ledger)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("topic %q: %w", t.Name, err)
		}
	}
	return ledger, firstErr
}

func ensureTopic(
	ctx context.Context,
	client snsAPI,
	kmsClient cloudctlaws.KMSClient,
	k8sClient client.Client,
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

	var kmsKeyARN *string
	if t.Encryption != nil {
		if t.Encryption.KMSKeyRef != nil {
			arn, ok := kms.ResolveSharedKeyARN(ctx, k8sClient, namespace, crName, *t.Encryption.KMSKeyRef)
			if !ok {
				return ledger, &cloudctlaws.ReconcileError{
					Err:       fmt.Errorf("encryption.kmsKeyRef %s/%s/%s is not yet authorized (producer must list this CR in the key's sharedWith) or does not exist yet", t.Encryption.KMSKeyRef.Namespace, t.Encryption.KMSKeyRef.Name, t.Encryption.KMSKeyRef.ResourceName),
					Retryable: true,
				}
			}
			kmsKeyARN = &arn
		}
		if t.Encryption.Enabled {
			arn, updatedLedger, err := kms.EnsureDedicatedKey(ctx, kmsClient, namespace, crName, crUID, t.Name, t.DeletionPolicy, ledger)
			ledger = updatedLedger
			if err != nil {
				return ledger, fmt.Errorf("encryption key: %w", err)
			}
			kmsKeyARN = &arn
		}
	}

	ownerTags := map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}

	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: &topicArn,
	})

	var notFound *types.NotFoundException
	if errors.As(err, &notFound) {
		attrs := desiredTopicAttributes(t, kmsKeyARN)
		if t.FIFO {
			attrs["FifoTopic"] = "true"
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

	if err := reconcileTopicAttributes(ctx, client, topicArn, t, kmsKeyARN); err != nil {
		return ledger, wrapAWSError(err, "reconciling topic attributes")
	}

	return recordVerified(ledger, t.Name, topicArn, t.DeletionPolicy, t.Force), nil
}

// desiredTopicAttributes computes the mutable attributes this package
// manages, without making any AWS call — used both at creation and as the
// comparison target inside reconcileTopicAttributes. FifoTopic is never
// included here since it's immutable after creation (enforced via CEL) —
// only ever set at creation time, in ensureTopic's own create path.
func desiredTopicAttributes(t depsv1alpha1.SNSTopicSpec, kmsKeyARN *string) map[string]string {
	desired := map[string]string{}
	if t.FIFO {
		dedup := "false"
		if t.Overrides != nil && t.Overrides.ContentBasedDeduplication != nil && *t.Overrides.ContentBasedDeduplication {
			dedup = "true"
		}
		desired["ContentBasedDeduplication"] = dedup
	}
	if kmsKeyARN != nil {
		desired["KmsMasterKeyId"] = *kmsKeyARN
	}
	return desired
}

// reconcileTopicAttributes corrects drift on ContentBasedDeduplication and
// KmsMasterKeyId, the only mutable attributes this package currently
// exposes (both confirmed mutable via SetTopicAttributes per AWS's docs).
// Unlike SQS's bulk SetQueueAttributes, SNS's SetTopicAttributes takes
// exactly one attribute name/value per call, so a changed attribute is set
// individually rather than in one batched request.
func reconcileTopicAttributes(ctx context.Context, client snsAPI, topicArn string, t depsv1alpha1.SNSTopicSpec, kmsKeyARN *string) error {
	desired := desiredTopicAttributes(t, kmsKeyARN)
	if len(desired) == 0 {
		return nil
	}

	attrNames := make([]string, 0, len(desired))
	for name := range desired {
		attrNames = append(attrNames, name)
	}
	current, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: &topicArn,
	})
	if err != nil {
		return fmt.Errorf("reading current attributes: %w", err)
	}

	for _, name := range attrNames {
		value := desired[name]
		if current.Attributes[name] == value {
			continue
		}
		n, v := name, value
		if _, err := client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
			TopicArn:       &topicArn,
			AttributeName:  &n,
			AttributeValue: &v,
		}); err != nil {
			return err
		}
	}
	return nil
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
