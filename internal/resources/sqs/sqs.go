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
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// sqsAPI is the subset of the SQS client this package needs. It's a type
// alias to the exported cloudctlaws.SQSClient rather than its own separate
// interface, since that interface now has two consumers — this package's
// own unit tests, and controller-level envtest suites that need to fake
// the whole AWS backend to exercise Reconcile() end-to-end. Aliasing keeps
// every reference to sqsAPI in this file unchanged.
type sqsAPI = cloudctlaws.SQSClient

const resourceType = "sqs"
const defaultMaxReceiveCount = int32(5)

// Ensure reconciles every declared SQS queue (and its DLQ, if requested)
// against AWS, updating the ownership ledger as it goes.
//
// Known gap deliberately deferred: drift correction on an existing queue's
// attributes (we verify ownership and read the ARN, but don't yet
// reconcile attribute changes on a queue that already exists — a DLQ's
// redrive policy is only ever set at creation time).
func Ensure(
	ctx context.Context,
	client sqsAPI,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SQSSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	for _, q := range spec.Resources {
		var err error
		ledger, err = ensureQueue(ctx, client, namespace, crName, crUID, q, ledger)
		if err != nil {
			return ledger, fmt.Errorf("queue %q: %w", q.Name, err)
		}
	}

	return ledger, nil
}

// ensureQueue orchestrates a spec entry: ensures its DLQ first (if
// requested), computes the resulting redrive policy, then ensures the main
// queue itself. Both the DLQ and the main queue go through the same
// ensureSingleQueue path — a DLQ is just another queue we own.
func ensureQueue(
	ctx context.Context,
	client sqsAPI,
	namespace,
	crName,
	crUID string,
	q depsv1alpha1.SQSQueueSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	var dlqArn string
	if q.DLQ {
		dlqName := q.Name + "-dlq"
		var err error
		ledger, err = ensureSingleQueue(ctx, client, namespace, crName, crUID, dlqName, q.DeletionPolicy, q.Force, q.Adopt, nil, ledger)
		if err != nil {
			return ledger, fmt.Errorf("dlq: %w", err)
		}
		entry := status.FindManagedResource(ledger, resourceType, dlqName)
		if entry == nil {
			return ledger, fmt.Errorf("dlq %q: expected a ledger entry after ensuring it, found none", dlqName)
		}
		dlqArn = entry.ARN
	}

	var redrivePolicy *string
	if dlqArn != "" {
		maxReceiveCount := defaultMaxReceiveCount
		if q.Overrides != nil && q.Overrides.MaxReceiveCount != nil {
			maxReceiveCount = *q.Overrides.MaxReceiveCount
		}
		// AWS's RedrivePolicy attribute is itself a JSON-encoded string,
		// and maxReceiveCount within it is conventionally encoded as a
		// JSON string too (not a bare number) - matching AWS's own
		// documented examples.
		encoded, err := json.Marshal(map[string]string{
			"deadLetterTargetArn": dlqArn,
			"maxReceiveCount":     fmt.Sprintf("%d", maxReceiveCount),
		})
		if err != nil {
			return ledger, fmt.Errorf("encoding redrive policy: %w", err)
		}
		s := string(encoded)
		redrivePolicy = &s
	}

	return ensureSingleQueue(ctx, client, namespace, crName, crUID, q.Name, q.DeletionPolicy, q.Force, q.Adopt, redrivePolicy, ledger)
}

// ensureSingleQueue creates the named queue if it doesn't exist (tagging is
// atomic with creation via CreateQueue's Tags field, so unlike S3 there's
// no TagPending window here), verifies ownership and adopts if requested
// when it already exists, and records it in the ledger either way.
func ensureSingleQueue(
	ctx context.Context,
	client sqsAPI,
	namespace,
	crName,
	crUID string,
	resourceName string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	force,
	adopt bool,
	redrivePolicy *string,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if existing := status.FindManagedResource(ledger, resourceType, resourceName); existing != nil && !status.NeedsRevalidation(*existing) {
		// Still within the trust window - skip the AWS round trip entirely.
		// Local-only fields (deletionPolicy/force) can still change from a
		// spec edit with no AWS call needed, so refresh those against the
		// cached entry; ARN and LastVerifiedAt stay as they were until the
		// window actually expires and a real check runs again.
		updated := *existing
		updated.DeletionPolicy = deletionPolicy
		updated.Force = force
		status.UpsertManagedResource(&ledger, updated)
		return ledger, nil
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, resourceName)
	ownerTags := map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}

	getOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})

	var notFound *types.QueueDoesNotExist
	if errors.As(err, &notFound) {
		attrs := map[string]string{}
		if redrivePolicy != nil {
			attrs["RedrivePolicy"] = *redrivePolicy
		}

		createOut, cErr := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName:  &queueName,
			Attributes: attrs,
			Tags:       ownerTags,
		})
		if cErr != nil {
			var deletedRecently *types.QueueDeletedRecently
			if errors.As(cErr, &deletedRecently) {
				return ledger, &cloudctlaws.ReconcileError{
					Err:       fmt.Errorf("queue %q was deleted too recently to recreate yet (AWS enforces a cooldown): %w", queueName, cErr),
					Retryable: true,
				}
			}
			return ledger, wrapAWSError(cErr, "creating queue")
		}
		return recordVerified(
			ctx,
			client,
			*createOut.QueueUrl,
			resourceName,
			deletionPolicy,
			force,
			ledger,
		)
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up queue")
	}

	// Queue already exists — verify we actually own it before trusting it.
	queueURL := *getOut.QueueUrl
	tagsOut, tErr := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &queueURL})
	if tErr != nil {
		return ledger, wrapAWSError(tErr, "reading queue tags")
	}

	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, crUID) {
		if existingOwner, ok := tagsOut.Tags[cloudctlaws.OwnerTagKey]; ok && existingOwner != cloudctlaws.OwnerTagValue(namespace, crName) {
			return ledger, fmt.Errorf("queue %q is already owned by a different AppDependencies CR (%s) — this looks like a naming collision, not adopting", queueName, existingOwner)
		}
		if !adopt {
			return ledger, fmt.Errorf("queue %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", queueName)
		}

		merged := cloudctlaws.MergeTags(tagsOut.Tags, ownerTags)
		if _, tagErr := client.TagQueue(ctx, &sqs.TagQueueInput{
			QueueUrl: &queueURL,
			Tags:     merged,
		}); tagErr != nil {
			return ledger, wrapAWSError(tagErr, "adopting queue (tagging)")
		}
	}

	return recordVerified(
		ctx,
		client,
		queueURL,
		resourceName,
		deletionPolicy,
		force,
		ledger,
	)
}

func recordVerified(
	ctx context.Context,
	client sqsAPI,
	queueURL,
	ledgerName string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	force bool,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	out, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return ledger, wrapAWSError(err, "reading queue ARN")
	}

	now := metav1.Now()
	createdAt := now
	if existing := status.FindManagedResource(ledger, resourceType, ledgerName); existing != nil {
		createdAt = existing.CreatedAt
	}

	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           ledgerName,
		ARN:            out.Attributes[string(types.QueueAttributeNameQueueArn)],
		State:          depsv1alpha1.ManagedResourceStateVerified,
		DeletionPolicy: deletionPolicy,
		Force:          force,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
	})
	return ledger, nil
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
