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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// snsAPI is the subset of the SNS client this package needs. It's a type
// alias to the exported cloudctlaws.SNSClient rather than its own separate
// interface, since that interface has two consumers — this package's own
// unit tests, and controller-level envtest suites that fake the whole AWS
// backend to exercise Reconcile() end-to-end (same pattern as sqsAPI).
type snsAPI = cloudctlaws.SNSClient

const resourceType = "sns"

// awsAttrTrue is the string form SNS's attribute API expects for a boolean
// attribute - these aren't real JSON/Go booleans, just string values AWS
// happens to parse as one.
const awsAttrTrue = "true"

// Ensure reconciles every declared SNS topic against AWS, updating the
// ownership ledger as it goes. kmsClient is only ever touched when a
// resource actually declares encryption.enabled — a CR that never uses it
// can pass nil.
func Ensure(
	ctx context.Context,
	awsClient snsAPI,
	kmsClient cloudctlaws.KMSClient,
	namespace,
	crName,
	crUID,
	region,
	accountID string,
	spec *depsv1alpha1.SNSSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, t := range spec.Resources {
		var err error
		ledger, err = ensureTopic(ctx, awsClient, kmsClient, namespace, crName, crUID, region, accountID, t, ledger, checkpoint, recordEvent)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("topic %q: %w", t.Name, err)
		}
	}
	return ledger, firstErr
}

func ensureTopic(
	ctx context.Context,
	awsClient snsAPI,
	kmsClient cloudctlaws.KMSClient,
	namespace,
	crName,
	crUID,
	region,
	accountID string,
	t depsv1alpha1.SNSTopicSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	before, _ := status.FindManagedResource(ledger, resourceType, t.Name)
	ledger, err := ensureTopicExists(ctx, awsClient, kmsClient, namespace, crName, crUID, region, accountID, t, ledger, checkpoint, recordEvent)
	if err != nil {
		return ledger, err
	}
	entry, found := status.FindManagedResource(ledger, resourceType, t.Name)
	if !found {
		return ledger, fmt.Errorf("expected a ledger entry for topic %q after ensuring it, found none", t.Name)
	}
	// Once per trust window, like the other checks that need a read of their
	// own: a topic trusted from the ledger on this pass is left alone.
	if !status.VerifiedSince(before, entry) {
		return ledger, nil
	}
	return ledger, reconcileDeclaredPolicy(ctx, awsClient, entry.ARN, recordEvent)
}

// ensureTopicExists is ensureTopic without the policy check.
func ensureTopicExists(
	ctx context.Context,
	awsClient snsAPI,
	kmsClient cloudctlaws.KMSClient,
	namespace,
	crName,
	crUID,
	region,
	accountID string,
	t depsv1alpha1.SNSTopicSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	const snsNameMaxLen = 256
	topicName := cloudctlaws.QueueOrTopicName(namespace, crName, resourceType, t.FIFO, snsNameMaxLen, t.Name)
	topicArn := cloudctlaws.TopicARN(region, accountID, topicName)

	kmsKeyARN, ledger, err := resolveTopicKey(ctx, kmsClient, namespace, crName, crUID, t, ledger, checkpoint, recordEvent)
	if err != nil {
		return ledger, err
	}

	if err := checkFIFOUnchanged(ledger, t.Name, t.FIFO); err != nil {
		return ledger, err
	}

	if existing, existingFound := status.FindManagedResource(ledger, resourceType, t.Name); existingFound && !status.NeedsRevalidation(existing) {
		trustedLedger, present, trustErr := ensureWithinTrust(ctx, awsClient, topicArn, existing, t, kmsKeyARN, ledger, recordEvent)
		if trustErr != nil || present {
			return trustedLedger, trustErr
		}
		// The topic is gone; the full path below recreates it.
		ledger = trustedLedger
	}

	ownerTags := map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}

	tagsOut, err := awsClient.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: &topicArn,
	})

	// ListTagsForResource is one of the tagging-family APIs (Tag/Untag/
	// ListTagsForResource) — those raise ResourceNotFoundException, not the
	// plain NotFoundException GetTopicAttributes/SetTopicAttributes use for
	// the same "no such topic" condition.
	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return createTopic(ctx, awsClient, topicName, ownerTags, t, kmsKeyARN, ledger, recordEvent)
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up topic")
	}

	// Topic already exists — verify we actually own it before trusting it.
	currentTags := tagsToMap(tagsOut.Tags)
	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		if err := adoptTopic(ctx, awsClient, kmsClient, namespace, crName, crUID, topicName, topicArn, currentTags, ownerTags, t, kmsKeyARN, recordEvent); err != nil {
			return ledger, err
		}
	}

	if err := reconcileTopicAttributes(ctx, awsClient, topicArn, t, kmsKeyARN); err != nil {
		return ledger, wrapAWSError(err, "reconciling topic attributes")
	}

	return recordVerified(ledger, t.Name, topicArn, t.DeletionPolicy, t.Force), nil
}

// resolveTopicKey returns the key the topic is encrypted with: a shared one
// from the ledger, or a dedicated one created for it. The ledger returned is
// current even on error.
func resolveTopicKey(
	ctx context.Context,
	kmsClient cloudctlaws.KMSClient,
	namespace, crName, crUID string,
	t depsv1alpha1.SNSTopicSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) (kmsKeyARN *string, updatedLedger []depsv1alpha1.ManagedResource, err error) {
	if t.Encryption == nil {
		return nil, ledger, nil
	}
	if t.Encryption.KMSKeyRef != "" {
		arn, ok := kms.SharedKeyARN(ledger, t.Encryption.KMSKeyRef)
		if !ok {
			return nil, ledger, &cloudctlaws.ReconcileError{
				Err:           fmt.Errorf("encryption.kmsKeyRef %q is not ready yet - declare it under kms.resources", t.Encryption.KMSKeyRef),
				Retryable:     true,
				WakesOnChange: true,
			}
		}
		kmsKeyARN = &arn
	}
	if t.Encryption.Enabled {
		arn, updated, keyErr := kms.EnsureDedicatedKey(ctx, kmsClient, namespace, crName, crUID, resourceType, t.Name, t.DeletionPolicy, ledger, checkpoint, recordEvent)
		if keyErr != nil {
			return nil, updated, fmt.Errorf("encryption key: %w", keyErr)
		}
		ledger = updated
		kmsKeyARN = &arn
	}
	return kmsKeyARN, ledger, nil
}

// ensureWithinTrust handles a topic whose ownership was verified recently.
// Ownership is not re-checked, but attribute drift correction is a different
// concern and must still run every reconcile regardless of the trust window.
// Local-only fields (deletionPolicy/force) can still change from a spec edit
// with no AWS call needed, so those are refreshed against the cached entry;
// ARN and LastVerifiedAt stay as they were until the window actually expires
// and a real ownership check runs again. present is false when the topic no
// longer exists, and the caller recreates it.
func ensureWithinTrust(
	ctx context.Context,
	awsClient snsAPI,
	topicArn string,
	existing depsv1alpha1.ManagedResource,
	t depsv1alpha1.SNSTopicSpec,
	kmsKeyARN *string,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, present bool, err error) {
	updated := existing
	updated.DeletionPolicy = t.DeletionPolicy
	updated.Force = t.Force
	status.UpsertManagedResource(&ledger, updated)

	// Existence is checked on every pass, trust window or not: the
	// window only exempts the ownership-tag re-check, and a topic
	// deleted out-of-band must not stay reported Ready until it lapses.
	var probeErr error
	if len(desiredTopicAttributes(t, kmsKeyARN)) == 0 {
		_, probeErr = awsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn})
	} else {
		probeErr = reconcileTopicAttributes(ctx, awsClient, topicArn, t, kmsKeyARN)
	}
	var gone *types.NotFoundException
	switch {
	case errors.As(probeErr, &gone):
		if recordEvent != nil {
			recordEvent("Warning", "TopicMissing", fmt.Sprintf("SNS topic %s no longer exists in AWS; recreating it", topicArn))
		}
		return ledger, false, nil
	case probeErr != nil:
		return ledger, false, wrapAWSError(probeErr, "reconciling topic attributes")
	}
	return ledger, true, nil
}

// createTopic creates a topic that does not exist yet.
func createTopic(
	ctx context.Context,
	awsClient snsAPI,
	topicName string,
	ownerTags map[string]string,
	t depsv1alpha1.SNSTopicSpec,
	kmsKeyARN *string,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	attrs := desiredTopicAttributes(t, kmsKeyARN)
	if t.FIFO {
		attrs["FifoTopic"] = awsAttrTrue
	}

	createOut, cErr := awsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name:       &topicName,
		Attributes: attrs,
		Tags:       mapToTags(ownerTags),
	})
	if cErr != nil {
		return ledger, wrapAWSError(cErr, "creating topic")
	}
	if recordEvent != nil {
		recordEvent("Normal", "TopicCreated", fmt.Sprintf("Created SNS topic %s", *createOut.TopicArn))
	}
	return recordVerified(ledger, t.Name, *createOut.TopicArn, t.DeletionPolicy, t.Force), nil
}

// adoptTopic brings an existing topic not tagged as this CR's under
// management, once the ownership rules allow it.
func adoptTopic(
	ctx context.Context,
	awsClient snsAPI,
	kmsClient cloudctlaws.KMSClient,
	namespace, crName, crUID, topicName, topicArn string,
	currentTags, ownerTags map[string]string,
	t depsv1alpha1.SNSTopicSpec,
	kmsKeyARN *string,
	recordEvent status.EventRecorder,
) error {
	transferred, ownErr := cloudctlaws.CheckExistingOwnership(ctx, "topic", topicName, currentTags, namespace, crName, crUID, t.Adopt)
	if ownErr != nil {
		return ownErr
	}
	attrsOut, aErr := awsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn})
	if aErr != nil {
		return wrapAWSError(aErr, "reading the topic's encryption before adopting it")
	}
	if err := kms.CheckAdoptedKey(ctx, kmsClient, attrsOut.Attributes["KmsMasterKeyId"], aws.ToString(kmsKeyARN)); err != nil {
		return fmt.Errorf("topic %q can't be adopted: %w", topicName, err)
	}

	merged := cloudctlaws.MergeTags(currentTags, ownerTags)
	if _, tagErr := awsClient.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &topicArn, Tags: mapToTags(merged)}); tagErr != nil {
		return wrapAWSError(tagErr, "adopting topic (tagging)")
	}
	if cloudctlaws.HasReleaseTags(currentTags) {
		if _, untagErr := awsClient.UntagResource(ctx, &sns.UntagResourceInput{ResourceArn: &topicArn, TagKeys: cloudctlaws.ReleasedTagKeys()}); untagErr != nil {
			cloudctlaws.ReleaseTagFailed(recordEvent, "topic", topicName, untagErr)
		}
	}
	if recordEvent != nil {
		if transferred {
			recordEvent("Normal", cloudctlaws.EventOwnershipTransferred, fmt.Sprintf("Took over SNS topic %s from a previous incarnation of this CR", topicArn))
		} else {
			recordEvent("Normal", "TopicAdopted", fmt.Sprintf("Adopted existing SNS topic %s under management", topicArn))
		}
	}
	return nil
}

// desiredTopicAttributes computes the mutable attributes this package
// manages, without making any AWS call — used both at creation and as the
// comparison target inside reconcileTopicAttributes. FifoTopic is never
// included here since it's immutable after creation (enforced via CEL) —
// only ever set at creation time, in createTopic.
func desiredTopicAttributes(t depsv1alpha1.SNSTopicSpec, kmsKeyARN *string) map[string]string {
	desired := map[string]string{}
	if t.FIFO {
		dedup := "false"
		if t.Overrides != nil && t.Overrides.ContentBasedDeduplication != nil && *t.Overrides.ContentBasedDeduplication {
			dedup = awsAttrTrue
		}
		desired["ContentBasedDeduplication"] = dedup
	}
	if kmsKeyARN != nil {
		desired["KmsMasterKeyId"] = *kmsKeyARN
	}
	return desired
}

// reconcileTopicAttributes corrects drift on ContentBasedDeduplication and
// KmsMasterKeyId, the only mutable attributes this package manages (both
// mutable via SetTopicAttributes per AWS's docs).
// Unlike SQS's bulk SetQueueAttributes, SNS's SetTopicAttributes takes
// exactly one attribute name/value per call, so a changed attribute is set
// individually rather than in one batched request.
func reconcileTopicAttributes(ctx context.Context, awsClient snsAPI, topicArn string, t depsv1alpha1.SNSTopicSpec, kmsKeyARN *string) error {
	desired := desiredTopicAttributes(t, kmsKeyARN)
	if len(desired) == 0 {
		return nil
	}

	attrNames := make([]string, 0, len(desired))
	for name := range desired {
		attrNames = append(attrNames, name)
	}
	current, err := awsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
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
		if _, err := awsClient.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
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
	if existing, existingFound := status.FindManagedResource(ledger, resourceType, ledgerName); existingFound {
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

func wrapAWSError(err error, errContext string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", errContext, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}

// checkFIFOUnchanged refuses a topic entry whose type the spec has flipped,
// except one that was let go, which no longer stands for a topic.
func checkFIFOUnchanged(ledger []depsv1alpha1.ManagedResource, resourceName string, fifo bool) error {
	existing, found := status.FindManagedResource(ledger, resourceType, resourceName)
	if !found || existing.State == depsv1alpha1.ManagedResourceStateRelinquished {
		return nil
	}
	return cloudctlaws.CheckFIFOUnchanged("topic", existing.ARN, fifo)
}
