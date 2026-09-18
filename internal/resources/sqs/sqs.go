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
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
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

type queueOptions struct {
	deletionPolicy            depsv1alpha1.DeletionPolicy
	force                     bool
	adopt                     bool
	fifo                      bool
	contentBasedDeduplication bool
	visibilityTimeoutSeconds  *int32
	redrivePolicy             *string
	kmsKeyARN                 *string
}

// Ensure reconciles every declared SQS queue (and its DLQ, if requested)
// against AWS, updating the ownership ledger as it goes. kmsClient is only
// ever touched when a resource actually declares encryption.enabled — a
// CR that never uses it can pass nil. k8sClient is only ever touched when a
// resource declares encryption.kmsKeyRef (the shared-key half of the
// hybrid design) — a CR that never uses it can pass nil too.
func Ensure(
	ctx context.Context,
	client sqsAPI,
	kmsClient cloudctlaws.KMSClient,
	k8sClient client.Client,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SQSSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, q := range spec.Resources {
		var err error
		ledger, err = ensureQueue(ctx, client, kmsClient, k8sClient, namespace, crName, crUID, q, ledger)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("queue %q: %w", q.Name, err)
		}
	}

	return ledger, firstErr
}

// ensureQueue orchestrates a spec entry: resolves its encryption key (if
// any) once so the same key protects both halves of a DLQ pair, ensures
// the DLQ first (if requested), computes the resulting redrive policy,
// then ensures the main queue itself. Both the DLQ and the main queue go
// through the same ensureSingleQueue path — a DLQ is just another queue
// we own.
func ensureQueue(
	ctx context.Context,
	client sqsAPI,
	kmsClient cloudctlaws.KMSClient,
	k8sClient client.Client,
	namespace,
	crName,
	crUID string,
	q depsv1alpha1.SQSQueueSpec,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	var kmsKeyARN *string
	if q.Encryption != nil {
		if q.Encryption.KMSKeyRef != nil {
			arn, ok := kms.ResolveSharedKeyARN(ctx, k8sClient, namespace, crName, *q.Encryption.KMSKeyRef)
			if !ok {
				return ledger, &cloudctlaws.ReconcileError{
					Err:       fmt.Errorf("encryption.kmsKeyRef %s/%s/%s is not yet authorized (producer must list this CR in the key's sharedWith) or does not exist yet", q.Encryption.KMSKeyRef.Namespace, q.Encryption.KMSKeyRef.Name, q.Encryption.KMSKeyRef.ResourceName),
					Retryable: true,
				}
			}
			kmsKeyARN = &arn
		}
		if q.Encryption.Enabled {
			arn, updatedLedger, err := kms.EnsureDedicatedKey(ctx, kmsClient, namespace, crName, crUID, q.Name, q.DeletionPolicy, ledger)
			ledger = updatedLedger
			if err != nil {
				return ledger, fmt.Errorf("encryption key: %w", err)
			}
			kmsKeyARN = &arn
		}
	}

	var dlqArn string
	if q.DLQ {
		dlqName := q.Name + "-dlq"
		var err error
		// A FIFO source queue requires a FIFO DLQ - AWS rejects mismatched
		// pairs - so fifo is inherited here, not independently configurable.
		// The DLQ shares the main queue's own key rather than getting its
		// own dedicated one - it holds a copy of the exact same sensitive
		// data, so a second key would add cost and complexity with no
		// actual isolation benefit.
		ledger, err = ensureSingleQueue(ctx, client, namespace, crName, crUID, dlqName, queueOptions{
			deletionPolicy: q.DeletionPolicy,
			force:          q.Force,
			adopt:          q.Adopt,
			fifo:           q.FIFO,
			kmsKeyARN:      kmsKeyARN,
		}, ledger)
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

	contentBasedDedup := false
	var visibilityTimeout *int32
	if q.Overrides != nil {
		if q.Overrides.ContentBasedDeduplication != nil {
			contentBasedDedup = *q.Overrides.ContentBasedDeduplication
		}
		visibilityTimeout = q.Overrides.VisibilityTimeoutSeconds
	}

	return ensureSingleQueue(ctx, client, namespace, crName, crUID, q.Name, queueOptions{
		deletionPolicy:            q.DeletionPolicy,
		force:                     q.Force,
		adopt:                     q.Adopt,
		fifo:                      q.FIFO,
		contentBasedDeduplication: contentBasedDedup,
		visibilityTimeoutSeconds:  visibilityTimeout,
		redrivePolicy:             redrivePolicy,
		kmsKeyARN:                 kmsKeyARN,
	}, ledger)
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
	opts queueOptions,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	if existing := status.FindManagedResource(ledger, resourceType, resourceName); existing != nil && !status.NeedsRevalidation(*existing) {
		// Still within the trust window - skip re-verifying ownership, but
		// attribute drift correction is a different concern and must still
		// run every reconcile regardless of the trust window. Local-only
		// fields (deletionPolicy/force) can still change from a spec edit
		// with no AWS call needed, so refresh those against the cached
		// entry; ARN and LastVerifiedAt stay as they were until the window
		// actually expires and a real ownership check runs again.
		updated := *existing
		updated.DeletionPolicy = opts.deletionPolicy
		updated.Force = opts.force
		status.UpsertManagedResource(&ledger, updated)

		if len(desiredAttributes(opts)) == 0 {
			// Nothing this package manages could have drifted - skip the
			// AWS round trip entirely rather than resolving a URL just to
			// find there's nothing to compare.
			return ledger, nil
		}

		queueName, nameErr := queueNameFromARN(existing.ARN)
		if nameErr != nil {
			return ledger, nameErr
		}
		urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: &queueName,
		})
		if err != nil {
			return ledger, wrapAWSError(err, "resolving queue URL for drift correction")
		}
		if err := reconcileAttributes(ctx, client, *urlOut.QueueUrl, opts); err != nil {
			return ledger, wrapAWSError(err, "reconciling queue attributes")
		}
		return ledger, nil
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, resourceName)
	if opts.fifo {
		queueName += ".fifo"
	}
	if err := cloudctlaws.ValidateNameLength(queueName, 80, "SQS queue"); err != nil {
		return ledger, err
	}
	ownerTags := map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}

	getOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})

	var notFound *types.QueueDoesNotExist
	if errors.As(err, &notFound) {
		// Reuses desiredAttributes rather than building a second, separate
		// map here - FifoQueue is the one attribute desiredAttributes
		// deliberately excludes (immutable after creation, so it's only
		// ever relevant on this create path, never drift-corrected).
		attrs := desiredAttributes(opts)
		if opts.fifo {
			attrs["FifoQueue"] = "true"
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
		// Attributes were just set atomically at creation — nothing to
		// drift-correct yet.
		return recordVerified(
			ctx,
			client,
			*createOut.QueueUrl,
			resourceName,
			opts.deletionPolicy,
			opts.force,
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
		if !opts.adopt {
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

	if err := reconcileAttributes(ctx, client, queueURL, opts); err != nil {
		return ledger, wrapAWSError(err, "reconciling queue attributes")
	}

	return recordVerified(
		ctx,
		client,
		queueURL,
		resourceName,
		opts.deletionPolicy,
		opts.force,
		ledger,
	)
}

// desiredAttributes computes the mutable attributes this package manages
// from queueOptions, without making any AWS calls. Used both to decide
// whether a drift-correction round trip is worth making at all (see the
// trust-window branch in ensureSingleQueue) and, once one is, as the
// comparison target inside reconcileAttributes.
func desiredAttributes(opts queueOptions) map[string]string {
	desired := map[string]string{}
	if opts.redrivePolicy != nil {
		desired["RedrivePolicy"] = *opts.redrivePolicy
	}
	if opts.fifo {
		dedup := "false"
		if opts.contentBasedDeduplication {
			dedup = "true"
		}
		desired["ContentBasedDeduplication"] = dedup
	}
	if opts.visibilityTimeoutSeconds != nil {
		desired["VisibilityTimeout"] = fmt.Sprintf("%d", *opts.visibilityTimeoutSeconds)
	}
	if opts.kmsKeyARN != nil {
		desired["KmsMasterKeyId"] = *opts.kmsKeyARN
	}
	return desired
}

// reconcileAttributes corrects drift on an already-existing queue's mutable
// attributes against desired state.
//
// Known limitation: it can set or update RedrivePolicy, but doesn't attempt
// to clear it when a DLQ is removed from spec (dlq: true -> false). AWS's
// SetQueueAttributes docs don't confirm that an empty value unsets
// RedrivePolicy, so rather than guess at unverified behavior, this is left
// as an explicit gap — the DLQ queue itself still gets deleted correctly
// (that's Cleanup's job), just the main queue's RedrivePolicy attribute
// referencing it may linger until this is verified and fixed.
//
// FifoQueue is never included here since it's immutable after creation
// (enforced via CEL) — only ever set at creation time, never corrected.
func reconcileAttributes(ctx context.Context, client sqsAPI, queueURL string, opts queueOptions) error {
	desired := desiredAttributes(opts)
	if len(desired) == 0 {
		return nil
	}

	attrNames := make([]types.QueueAttributeName, 0, len(desired))
	for k := range desired {
		attrNames = append(attrNames, types.QueueAttributeName(k))
	}

	current, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: attrNames,
	})
	if err != nil {
		return fmt.Errorf("reading current attributes: %w", err)
	}

	changed := map[string]string{}
	for k, v := range desired {
		if current.Attributes[k] != v {
			changed[k] = v
		}
	}

	if len(changed) == 0 {
		return nil
	}

	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   &queueURL,
		Attributes: changed,
	})
	return err
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
