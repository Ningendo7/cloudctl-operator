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
	"strings"

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
// interface, since that interface has two consumers — this package's own
// unit tests, and controller-level envtest suites that fake the whole AWS
// backend to exercise Reconcile() end-to-end.
type sqsAPI = cloudctlaws.SQSClient

const resourceType = "sqs"

// awsAttrTrue is the string form SQS's attribute API expects for a boolean
// attribute - these aren't real JSON/Go booleans, just string values AWS
// happens to parse as one.
const awsAttrTrue = "true"
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

	// derivedFromKey/derivedRole, if both set, mean this queue's real AWS
	// name must be built via DerivedResourceName (role hashed as its own
	// tuple element) rather than ResourceName - required for a queue this
	// operator derives from another one it owns (a DLQ), whose ledger key
	// already has role baked into the string via DerivedKey and so can't
	// safely double as the ResourceName hash input too (see
	// DerivedResourceName's own doc comment for why).
	derivedFromKey string
	derivedRole    string

	// checkAdoptedKey vets the key an existing queue is encrypted with
	// before it is adopted; nil skips the check.
	checkAdoptedKey func(ctx context.Context, currentKey string) error
}

// Ensure reconciles every declared SQS queue (and its DLQ, if requested)
// against AWS, updating the ownership ledger as it goes. kmsClient is only
// ever touched when a resource actually declares encryption.enabled — a
// CR that never uses it can pass nil. k8sClient is only ever touched when a
// resource declares encryption.kmsKeyRef (the shared-key half of the
// hybrid design) — a CR that never uses it can pass nil too.
func Ensure(
	ctx context.Context,
	awsClient sqsAPI,
	kmsClient cloudctlaws.KMSClient,
	snsClient cloudctlaws.SNSClient,
	k8sClient client.Client,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.SQSSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, q := range spec.Resources {
		var err error
		ledger, err = ensureQueue(ctx, awsClient, kmsClient, snsClient, k8sClient, namespace, crName, crUID, q, ledger, checkpoint, recordEvent)
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
	awsClient sqsAPI,
	kmsClient cloudctlaws.KMSClient,
	snsClient cloudctlaws.SNSClient,
	k8sClient client.Client,
	namespace,
	crName,
	crUID string,
	q depsv1alpha1.SQSQueueSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	var kmsKeyARN *string
	if q.Encryption != nil {
		if q.Encryption.KMSKeyRef != "" {
			arn, ok := kms.SharedKeyARN(ledger, q.Encryption.KMSKeyRef)
			if !ok {
				return ledger, &cloudctlaws.ReconcileError{
					Err:           fmt.Errorf("encryption.kmsKeyRef %q is not ready yet - declare it under kms.resources", q.Encryption.KMSKeyRef),
					Retryable:     true,
					WakesOnChange: true,
				}
			}
			kmsKeyARN = &arn
		}
		if q.Encryption.Enabled {
			arn, updatedLedger, err := kms.EnsureDedicatedKey(ctx, kmsClient, namespace, crName, crUID, resourceType, q.Name, q.DeletionPolicy, ledger, checkpoint, recordEvent)
			ledger = updatedLedger
			if err != nil {
				return ledger, fmt.Errorf("encryption key: %w", err)
			}
			kmsKeyARN = &arn
		}
	}

	checkAdoptedKey := kms.AdoptedKeyCheck(kmsClient, kmsKeyARN)

	var dlqArn string
	if q.DLQ {
		dlqName := cloudctlaws.DerivedKey(q.Name, "dlq")
		var err error
		// A FIFO source queue requires a FIFO DLQ - AWS rejects mismatched
		// pairs - so fifo is inherited here, not independently configurable.
		// The DLQ shares the main queue's own key rather than getting its
		// own dedicated one - it holds a copy of the exact same sensitive
		// data, so a second key would add cost and complexity with no
		// actual isolation benefit.
		ledger, _, err = ensureSingleQueue(ctx, awsClient, namespace, crName, crUID, dlqName, queueOptions{
			deletionPolicy:  q.DeletionPolicy,
			force:           q.Force,
			adopt:           q.Adopt,
			fifo:            q.FIFO,
			kmsKeyARN:       kmsKeyARN,
			derivedFromKey:  q.Name,
			derivedRole:     "dlq",
			checkAdoptedKey: checkAdoptedKey,
		}, ledger, recordEvent)
		if err != nil {
			return ledger, fmt.Errorf("dlq: %w", err)
		}
		entry, entryFound := status.FindManagedResource(ledger, resourceType, dlqName)
		if !entryFound {
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

	ledger, queueURL, err := ensureSingleQueue(ctx, awsClient, namespace, crName, crUID, q.Name, queueOptions{
		deletionPolicy:            q.DeletionPolicy,
		force:                     q.Force,
		adopt:                     q.Adopt,
		fifo:                      q.FIFO,
		contentBasedDeduplication: contentBasedDedup,
		visibilityTimeoutSeconds:  visibilityTimeout,
		redrivePolicy:             redrivePolicy,
		kmsKeyARN:                 kmsKeyARN,
		checkAdoptedKey:           checkAdoptedKey,
	}, ledger, recordEvent)
	if err != nil {
		return ledger, err
	}

	if len(q.SubscribesTo) == 0 {
		return ledger, nil
	}

	entry, entryFound := status.FindManagedResource(ledger, resourceType, q.Name)
	if !entryFound {
		return ledger, errors.New("expected a ledger entry after ensuring it, found none")
	}
	queueName, err := queueNameFromARN(entry.ARN)
	if err != nil {
		return ledger, fmt.Errorf("resolving queue name for subscription management: %w", err)
	}
	return EnsureSubscriptions(ctx, snsClient, awsClient, k8sClient, namespace, crName, queueName, queueURL, entry.ARN, q.SubscribesTo, ledger, recordEvent)
}

// ensureSingleQueue creates the named queue if it doesn't exist (tagging is
// atomic with creation via CreateQueue's Tags field), verifies ownership and
// adopts if requested when it already exists, and records it in the ledger
// either way.
func ensureSingleQueue(
	ctx context.Context,
	awsClient sqsAPI,
	namespace,
	crName,
	crUID string,
	resourceName string,
	opts queueOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, queueURL string, err error) {
	before, _ := status.FindManagedResource(ledger, resourceType, resourceName)
	var policy *string
	ledger, queueURL, policy, err = ensureQueueExists(ctx, awsClient, namespace, crName, crUID, resourceName, opts, ledger, recordEvent)
	if err != nil {
		return ledger, queueURL, err
	}
	entry, found := status.FindManagedResource(ledger, resourceType, resourceName)
	if !found {
		return ledger, queueURL, fmt.Errorf("expected a ledger entry for queue %q after ensuring it, found none", resourceName)
	}
	// Once per trust window, like the other checks that need a read of their
	// own: a queue trusted from the ledger on this pass is left alone.
	if !status.VerifiedSince(before, entry) {
		return ledger, queueURL, nil
	}
	if policy != nil {
		return ledger, queueURL, reconcileDeclaredPolicyFrom(ctx, awsClient, queueURL, entry.ARN, *policy, recordEvent)
	}
	return ledger, queueURL, reconcileDeclaredPolicy(ctx, awsClient, queueURL, entry.ARN, recordEvent)
}

// ensureQueueExists is ensureSingleQueue without the transport-security
// statement. policy is the queue's policy when this pass read it, so the
// caller needn't read it again.
func ensureQueueExists(
	ctx context.Context,
	awsClient sqsAPI,
	namespace,
	crName,
	crUID string,
	resourceName string,
	opts queueOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, queueURL string, policy *string, err error) {
	if err := checkFIFOUnchanged(ledger, resourceName, opts.fifo); err != nil {
		return ledger, "", nil, err
	}

	if existing, existingFound := status.FindManagedResource(ledger, resourceType, resourceName); existingFound && !status.NeedsRevalidation(existing) {
		trustedLedger, trustedURL, present, trustErr := ensureWithinTrust(ctx, awsClient, existing, opts, ledger, recordEvent)
		if trustErr != nil || present {
			return trustedLedger, trustedURL, nil, trustErr
		}
		// The queue is gone; the full path below recreates it.
		ledger = trustedLedger
	}

	keyParts := []string{resourceName}
	if opts.derivedRole != "" {
		keyParts = []string{opts.derivedFromKey, opts.derivedRole}
	}
	queueName := cloudctlaws.QueueName(namespace, crName, opts.fifo, keyParts...)
	ownerTags := cloudctlaws.CreationTags(namespace, crName, crUID)

	getOut, err := awsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})

	if _, ok := errors.AsType[*types.QueueDoesNotExist](err); ok {
		ledger, queueURL, err = createQueue(ctx, awsClient, queueName, resourceName, ownerTags, opts, ledger, recordEvent)
		return ledger, queueURL, nil, err
	}
	if err != nil {
		return ledger, "", nil, wrapAWSError(err, "looking up queue")
	}

	// Queue already exists — verify we actually own it before trusting it.
	queueURL = *getOut.QueueUrl
	tagsOut, tErr := awsClient.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &queueURL})
	if tErr != nil {
		return ledger, "", nil, wrapAWSError(tErr, "reading queue tags")
	}

	if !cloudctlaws.IsClaimedBy(tagsOut.Tags, namespace, crName, crUID) {
		if err := adoptQueue(ctx, awsClient, namespace, crName, crUID, queueName, queueURL, tagsOut.Tags, opts, recordEvent); err != nil {
			return ledger, "", nil, err
		}
	}

	// One read for the drift check, the ARN and the policy: SQS's rate limit
	// is what bounds how many CRs one operator can keep up with.
	current, err := readAttributes(ctx, awsClient, queueURL, opts,
		types.QueueAttributeNameQueueArn, types.QueueAttributeNamePolicy)
	if err != nil {
		return ledger, "", nil, wrapAWSError(err, "reconciling queue attributes")
	}
	if err := applyAttributeDrift(ctx, awsClient, queueURL, opts, current); err != nil {
		return ledger, "", nil, wrapAWSError(err, "reconciling queue attributes")
	}

	ledger = recordVerifiedARN(current[string(types.QueueAttributeNameQueueArn)], resourceName, opts.deletionPolicy, opts.force, ledger)
	queuePolicy := current[string(types.QueueAttributeNamePolicy)]
	return ledger, queueURL, &queuePolicy, nil
}

// ensureWithinTrust handles a queue whose ownership was verified recently.
// Ownership is not re-checked, but attribute drift correction is a different
// concern and must still run every reconcile regardless of the trust window.
// Local-only fields (deletionPolicy/force) can still change from a spec edit
// with no AWS call needed, so those are refreshed against the cached entry;
// ARN and LastVerifiedAt stay as they were until the window actually expires
// and a real ownership check runs again. present is false when the queue no
// longer exists, and the caller recreates it.
func ensureWithinTrust(
	ctx context.Context,
	awsClient sqsAPI,
	existing depsv1alpha1.ManagedResource,
	opts queueOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) (updatedLedger []depsv1alpha1.ManagedResource, queueURL string, present bool, err error) {
	updated := existing
	updated.DeletionPolicy = opts.deletionPolicy
	updated.Force = opts.force
	status.UpsertManagedResource(&ledger, updated)

	// Existence is checked on every pass, trust window or not: the
	// window only exempts the ownership-tag re-check, and a queue
	// deleted out-of-band must not stay reported Ready until it lapses.
	queueName, nameErr := queueNameFromARN(existing.ARN)
	if nameErr != nil {
		return ledger, "", false, nameErr
	}
	urlOut, urlErr := awsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	var gone *types.QueueDoesNotExist
	switch {
	case errors.As(urlErr, &gone):
		if recordEvent != nil {
			recordEvent("Warning", "QueueMissing", fmt.Sprintf("SQS queue %s no longer exists in AWS; recreating it", queueName))
		}
		return ledger, "", false, nil
	case urlErr != nil:
		return ledger, "", false, wrapAWSError(urlErr, "resolving queue URL for drift correction")
	}
	if len(desiredAttributes(opts)) > 0 {
		if err := reconcileAttributes(ctx, awsClient, *urlOut.QueueUrl, opts); err != nil {
			return ledger, "", false, wrapAWSError(err, "reconciling queue attributes")
		}
	}
	return ledger, *urlOut.QueueUrl, true, nil
}

// createQueue creates a queue that does not exist yet.
func createQueue(
	ctx context.Context,
	awsClient sqsAPI,
	queueName, resourceName string,
	ownerTags map[string]string,
	opts queueOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, string, error) {
	// Reuses desiredAttributes rather than building a second, separate
	// map here - FifoQueue is the one attribute desiredAttributes
	// deliberately excludes (immutable after creation, so it's only
	// ever relevant on this create path, never drift-corrected).
	attrs := desiredAttributes(opts)
	if opts.fifo {
		attrs["FifoQueue"] = awsAttrTrue
	}

	createOut, cErr := awsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  &queueName,
		Attributes: attrs,
		Tags:       ownerTags,
	})
	if cErr != nil {
		if _, ok := errors.AsType[*types.QueueDeletedRecently](cErr); ok {
			return ledger, "", &cloudctlaws.ReconcileError{
				Err:       fmt.Errorf("queue %q was deleted too recently to recreate yet (AWS enforces a cooldown): %w", queueName, cErr),
				Retryable: true,
			}
		}
		return ledger, "", wrapAWSError(cErr, "creating queue")
	}
	if recordEvent != nil {
		recordEvent("Normal", "QueueCreated", fmt.Sprintf("Created SQS queue %s", queueName))
	}
	// Attributes were just set atomically at creation — nothing to
	// drift-correct yet.
	ledger, err := recordVerified(ctx, awsClient, *createOut.QueueUrl, resourceName, opts.deletionPolicy, opts.force, ledger)
	if _, ok := errors.AsType[*types.QueueDoesNotExist](err); ok {
		// SQS may not serve a queue for about a second after creating it.
		err = &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("queue %q was just created and can't be read yet: %w", queueName, err),
			Retryable: true,
		}
	}
	return ledger, *createOut.QueueUrl, err
}

// adoptQueue brings an existing queue not tagged as this CR's under
// management, once the ownership rules allow it.
func adoptQueue(
	ctx context.Context,
	awsClient sqsAPI,
	namespace, crName, crUID, queueName, queueURL string,
	currentTags map[string]string,
	opts queueOptions,
	recordEvent status.EventRecorder,
) error {
	how, ownErr := cloudctlaws.CheckExistingOwnership(ctx, "queue", queueName, currentTags, namespace, crName, crUID, opts.adopt)
	if ownErr != nil {
		return ownErr
	}
	if opts.checkAdoptedKey != nil && how != cloudctlaws.Claimed {
		attrs, aErr := awsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       &queueURL,
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameKmsMasterKeyId},
		})
		if aErr != nil {
			return wrapAWSError(aErr, "reading the queue's encryption before adopting it")
		}
		if err := opts.checkAdoptedKey(ctx, attrs.Attributes[string(types.QueueAttributeNameKmsMasterKeyId)]); err != nil {
			return fmt.Errorf("queue %q can't be adopted: %w", queueName, err)
		}
	}

	merged := cloudctlaws.ClaimTags(currentTags, namespace, crName, crUID)
	if _, tagErr := awsClient.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: &queueURL,
		Tags:     merged,
	}); tagErr != nil {
		return wrapAWSError(tagErr, "adopting queue (tagging)")
	}
	if cloudctlaws.HasReleaseTags(currentTags) {
		if _, untagErr := awsClient.UntagQueue(ctx, &sqs.UntagQueueInput{QueueUrl: &queueURL, TagKeys: cloudctlaws.ReleasedTagKeys()}); untagErr != nil {
			cloudctlaws.ReleaseTagFailed(recordEvent, "queue", queueName, untagErr)
		}
	}
	if recordEvent != nil {
		switch how {
		case cloudctlaws.Transferred:
			recordEvent("Normal", cloudctlaws.EventOwnershipTransferred, fmt.Sprintf("Took over SQS queue %s from a previous owner", queueName))
		case cloudctlaws.Adopted:
			recordEvent("Normal", "QueueAdopted", fmt.Sprintf("Adopted existing SQS queue %s under management", queueName))
		case cloudctlaws.Claimed:
			// Only this cluster's tag was added.
		}
	}
	return nil
}

// checkFIFOUnchanged refuses a queue entry whose type the spec has flipped,
// except one that was let go, which no longer stands for a queue.
func checkFIFOUnchanged(ledger []depsv1alpha1.ManagedResource, resourceName string, fifo bool) error {
	existing, found := status.FindManagedResource(ledger, resourceType, resourceName)
	if !found || existing.State == depsv1alpha1.ManagedResourceStateRelinquished {
		return nil
	}
	return cloudctlaws.CheckFIFOUnchanged("queue", existing.ARN, fifo)
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
			dedup = awsAttrTrue
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
// RedrivePolicy is always included here, even when desiredAttributes leaves
// it out entirely (no DLQ requested) - an empty string is a valid value
// that clears the attribute, so a DLQ being removed from spec is corrected
// here rather than left lingering forever. This only runs once this
// function is actually called, though: ensureSingleQueue's own trust-window
// fast path still skips the round trip entirely when desiredAttributes(opts)
// is empty, so a DLQ removal with no other attribute override present won't
// self-correct until the trust window expires (same cadence as ownership
// re-verification), not on the very next reconcile.
//
// FifoQueue is never included here since it's immutable after creation
// (enforced via CEL) — only ever set at creation time, never corrected.
func reconcileAttributes(ctx context.Context, awsClient sqsAPI, queueURL string, opts queueOptions) error {
	current, err := readAttributes(ctx, awsClient, queueURL, opts)
	if err != nil {
		return err
	}
	return applyAttributeDrift(ctx, awsClient, queueURL, opts, current)
}

// driftAttributes is what reconcileAttributes compares: desiredAttributes,
// plus an empty RedrivePolicy when no DLQ is declared.
func driftAttributes(opts queueOptions) map[string]string {
	desired := desiredAttributes(opts)
	if opts.redrivePolicy == nil {
		desired["RedrivePolicy"] = ""
	}
	return desired
}

// readAttributes reads the attributes the drift check compares, and extra.
func readAttributes(ctx context.Context, awsClient sqsAPI, queueURL string, opts queueOptions, extra ...types.QueueAttributeName) (map[string]string, error) {
	desired := driftAttributes(opts)
	attrNames := make([]types.QueueAttributeName, 0, len(desired)+len(extra))
	for k := range desired {
		attrNames = append(attrNames, types.QueueAttributeName(k))
	}
	attrNames = append(attrNames, extra...)

	current, err := awsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: attrNames,
	})
	if err != nil {
		return nil, fmt.Errorf("reading current attributes: %w", err)
	}
	return current.Attributes, nil
}

// applyAttributeDrift writes whichever drift-checked attributes differ from
// current.
func applyAttributeDrift(ctx context.Context, awsClient sqsAPI, queueURL string, opts queueOptions, current map[string]string) error {
	changed := map[string]string{}
	for k, v := range driftAttributes(opts) {
		if !attributeMatches(k, current[k], v) {
			changed[k] = v
		}
	}

	if len(changed) == 0 {
		return nil
	}

	_, err := awsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   &queueURL,
		Attributes: changed,
	})
	return err
}

// attributeMatches compares one queue attribute the way SQS means it. SQS
// hands a RedrivePolicy back in its own form, with maxReceiveCount a number
// where we send it quoted, so that one is compared by what it says; compared
// as text it never matches and the queue is rewritten on every pass.
func attributeMatches(name, current, desired string) bool {
	if name != "RedrivePolicy" || current == "" || desired == "" {
		return current == desired
	}
	got, gotErr := parseRedrivePolicy(current)
	want, wantErr := parseRedrivePolicy(desired)
	if gotErr != nil || wantErr != nil {
		return current == desired
	}
	return got == want
}

type redrivePolicyValues struct {
	deadLetterTargetARN string
	maxReceiveCount     string
}

func parseRedrivePolicy(raw string) (redrivePolicyValues, error) {
	var doc struct {
		DeadLetterTargetArn string          `json:"deadLetterTargetArn"`
		MaxReceiveCount     json.RawMessage `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return redrivePolicyValues{}, err
	}
	return redrivePolicyValues{
		deadLetterTargetARN: doc.DeadLetterTargetArn,
		maxReceiveCount:     strings.Trim(string(doc.MaxReceiveCount), `"`),
	}, nil
}

func recordVerified(
	ctx context.Context,
	awsClient sqsAPI,
	queueURL,
	ledgerName string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	force bool,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	out, err := awsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return ledger, wrapAWSError(err, "reading queue ARN")
	}
	return recordVerifiedARN(out.Attributes[string(types.QueueAttributeNameQueueArn)], ledgerName, deletionPolicy, force, ledger), nil
}

// recordVerifiedARN is recordVerified with the ARN already read.
func recordVerifiedARN(
	arn,
	ledgerName string,
	deletionPolicy depsv1alpha1.DeletionPolicy,
	force bool,
	ledger []depsv1alpha1.ManagedResource,
) []depsv1alpha1.ManagedResource {
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

func wrapAWSError(err error, errContext string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", errContext, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}

// CheckLedgerEntry refuses a queue entry whose ARN is not the queue this CR
// would have, such as another CR's queue or one in another account.
func CheckLedgerEntry(id cloudctlaws.Identity, namespace, crName string, entry depsv1alpha1.ManagedResource) error {
	fifo := strings.HasSuffix(entry.ARN, ".fifo")
	want := id.RegionalARN("sqs", cloudctlaws.QueueName(namespace, crName, fifo, cloudctlaws.SplitDerivedKey(entry.Name)...))
	return cloudctlaws.CheckLedgerARN(entry.Type, entry.Name, entry.ARN, want)
}

// CheckSubscriptionEntry refuses a subscription entry that isn't on the topic
// its ledger name refers to. A subscription's ARN is its topic's ARN and an
// ID SNS chose, so only the topic part can be checked.
func CheckSubscriptionEntry(id cloudctlaws.Identity, _, _ string, entry depsv1alpha1.ManagedResource) error {
	_, ref, _ := strings.Cut(entry.Name, subscriptionLedgerSeparator)
	parts := strings.Split(ref, "/")
	if len(parts) != 3 {
		return &cloudctlaws.LedgerMismatchError{Type: entry.Type, Name: entry.Name, ARN: entry.ARN,
			Want: "a name of the form <queue>" + subscriptionLedgerSeparator + "<namespace>/<cr>/<topic>"}
	}
	var err error
	for _, fifo := range []bool{false, true} {
		topicARN := id.RegionalARN("sns", cloudctlaws.TopicName(parts[0], parts[1], fifo, parts[2]))
		if err = cloudctlaws.CheckLedgerARNPrefix(entry.Type, entry.Name, entry.ARN, topicARN+":"); err == nil {
			return nil
		}
	}
	return err
}
