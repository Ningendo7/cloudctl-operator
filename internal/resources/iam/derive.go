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

package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// actionSet enumerates the AWS IAM actions granted for one resource type,
// split into what every grant gets (baseline - includes whatever's
// mechanically required just to use the resource at all, like deleting a
// consumed SQS message once received, or subscribing to an SNS topic) and
// what ReadWrite access adds on top (the ability to add new data).
type actionSet struct {
	baseline  []string
	readWrite []string
}

// Resource-type tags shared across every grant/ledger lookup in this file -
// named constants rather than repeated literals purely so the same typo
// can't diverge between a grant's tag and the ledger lookup it's paired
// with.
const (
	resourceTypeSQS      = "sqs"
	resourceTypeSNS      = "sns"
	resourceTypeDynamoDB = "dynamodb"
	resourceTypeS3       = "s3"
	resourceTypeKMS      = "kms"

	kmsActionDecrypt         = "kms:Decrypt"
	kmsActionGenerateDataKey = "kms:GenerateDataKey"

	// iamPolicyVersion is the only version string IAM policy documents
	// (both trust and permissions) currently support.
	iamPolicyVersion = "2012-10-17"
	effectAllow      = "Allow"
)

var actionSets = map[string]actionSet{
	resourceTypeSQS: {
		baseline: []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ChangeMessageVisibility"},
		// sqs:SendMessage alone also governs SendMessageBatch calls -
		// "sqs:SendMessageBatch" isn't a real, recognized SQS action (AWS
		// rejects it outright in a resource policy; IAM silently accepts
		// but doesn't act on it in a role policy either).
		readWrite: []string{"sqs:SendMessage"},
	},
	resourceTypeSNS: {
		baseline:  []string{"sns:Subscribe", "sns:Unsubscribe", "sns:GetTopicAttributes"},
		readWrite: []string{"sns:Publish"},
	},
	resourceTypeDynamoDB: {
		baseline:  []string{"dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem", "dynamodb:TransactGetItems"},
		readWrite: []string{"dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:BatchWriteItem", "dynamodb:TransactWriteItems", "dynamodb:ConditionCheckItem"},
	},
	// kms grants are for a resource's own encryption key (dedicated or
	// shared via kmsKeyRef), never a standalone kms.resources entry
	// consumed on its own - AWS's own SQS SSE docs confirm this exact
	// split: a consumer only ever needs Decrypt (to verify a cached data
	// key's integrity on receive), a producer additionally needs
	// GenerateDataKey (to mint a new one on send). Since an owned resource
	// always gets full baseline+readWrite regardless of sharedWith, an
	// owned encrypted queue's own role gets both - the same "producer"
	// requirement set - which is correct since it can always send.
	resourceTypeKMS: {
		baseline:  []string{kmsActionDecrypt},
		readWrite: []string{kmsActionGenerateDataKey},
	},
}

// S3 is handled separately from actionSets: it needs two different ARN
// scopes per grant (the bare bucket ARN for ListBucket, the bucket ARN
// with a "/*" suffix for everything else), so one Resource per statement
// like every other type here doesn't fit.
var (
	s3BucketLevelActions = []string{"s3:ListBucket"}
	s3ObjectBaseline     = []string{"s3:GetObject"}
	s3ObjectReadWrite    = []string{"s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"}
)

// grantActions is actionsFor without the actions a consumer never gets.
func grantActions(g grant) []string {
	actions := actionsFor(g.resourceType, g.readWrite)
	if !g.consumed {
		return actions
	}
	return slices.DeleteFunc(actions, func(a string) bool { return slices.Contains(notForConsumers[g.resourceType], a) })
}

func actionsFor(resourceType string, readWrite bool) []string {
	set := actionSets[resourceType]
	actions := append([]string{}, set.baseline...)
	if readWrite {
		actions = append(actions, set.readWrite...)
	}
	return actions
}

func s3ObjectActions(readWrite bool) []string {
	actions := append([]string{}, s3ObjectBaseline...)
	if readWrite {
		actions = append(actions, s3ObjectReadWrite...)
	}
	return actions
}

// grant is one resource this CR's role should have access to, resolved to
// a concrete ARN, ready to become one or more policy statements.
type grant struct {
	resourceType string // "sqs", "sns", "dynamodb", "s3" - see the resourceType* constants
	arn          string
	readWrite    bool
	consumed     bool // granted to another CR through sharedWith, not owned
}

// notForConsumers are owner actions a consumer never gets. A consumer that
// could subscribe could point the owner's topic at any endpoint, and that
// subscription would outlive a revocation; consumers get messages through
// subscribesTo, which the operator removes when access is revoked.
var notForConsumers = map[string][]string{
	resourceTypeSNS: {"sns:Subscribe", "sns:Unsubscribe"},
}

// collectGrants gathers every resource this CR's IAM role needs access to:
// every resource it owns - always full read+write, unconditionally,
// regardless of whether sharedWith/consumes are used at all, since
// declaring a dependency as your own already implies needing to use it -
// plus every resource it consumes from another CR, gated on that CR's
// sharedWith grant actually naming this one.
//
// A consume reference that can't yet be resolved (the producer CR doesn't
// exist yet, hasn't reconciled that resource yet, or hasn't shared it with
// this CR) is skipped rather than treated as a hard failure for the whole
// derivation - same self-resolving-forward-reference philosophy already
// used for CEL validation of cross-object references (an Ingress pointing
// at a not-yet-created Service isn't rejected, it just doesn't route yet).
// skipped collects a human-readable reason for each one.
//
//nolint:gocyclo // four structurally-identical, deliberately-duplicated per-resource-type blocks (own+dedicated-key+shared-key+consumes); already covered by extensive TestCollectGrants_* tests, and splitting risks a correctness bug in this operator's single most security-critical function
func collectGrants(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) (grants []grant, skipped []string, err error) {
	if cr.Spec.SQS != nil {
		for _, q := range cr.Spec.SQS.Resources {
			grants, skipped = recordOwned(grants, skipped, cr, resourceTypeSQS, q.Name)
			if q.Encryption != nil {
				if q.Encryption.Enabled {
					// The dedicated key is owned by this CR exactly as much as
					// the queue it protects - same "always full access to what
					// you own" rule, via the same recordOwned helper, keyed to
					// the same ledger name kms.EnsureDedicatedKey uses.
					grants, skipped = recordOwned(grants, skipped, cr, resourceTypeKMS, cloudctlaws.DedicatedKeyLedgerName(resourceTypeSQS, q.Name))
				}
			}
		}
		for _, ref := range cr.Spec.SQS.Consumes {
			g, reason, err := resolveConsume(ctx, k8sClient, cr, resourceTypeSQS, ref)
			switch {
			case err != nil:
				return nil, nil, err
			case reason != "":
				skipped = append(skipped, reason)
			default:
				grants = append(grants, g...)
			}
		}
	}
	if cr.Spec.SNS != nil {
		for _, t := range cr.Spec.SNS.Resources {
			grants, skipped = recordOwned(grants, skipped, cr, resourceTypeSNS, t.Name)
			if t.Encryption != nil {
				if t.Encryption.Enabled {
					grants, skipped = recordOwned(grants, skipped, cr, resourceTypeKMS, cloudctlaws.DedicatedKeyLedgerName(resourceTypeSNS, t.Name))
				}
			}
		}
		for _, ref := range cr.Spec.SNS.Consumes {
			g, reason, err := resolveConsume(ctx, k8sClient, cr, resourceTypeSNS, ref)
			switch {
			case err != nil:
				return nil, nil, err
			case reason != "":
				skipped = append(skipped, reason)
			default:
				grants = append(grants, g...)
			}
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, tbl := range cr.Spec.DynamoDB.Resources {
			grants, skipped = recordOwned(grants, skipped, cr, resourceTypeDynamoDB, tbl.Name)
			if tbl.Encryption != nil {
				if tbl.Encryption.Enabled {
					grants, skipped = recordOwned(grants, skipped, cr, resourceTypeKMS, cloudctlaws.DedicatedKeyLedgerName(resourceTypeDynamoDB, tbl.Name))
				}
			}
		}
		for _, ref := range cr.Spec.DynamoDB.Consumes {
			g, reason, err := resolveConsume(ctx, k8sClient, cr, resourceTypeDynamoDB, ref)
			switch {
			case err != nil:
				return nil, nil, err
			case reason != "":
				skipped = append(skipped, reason)
			default:
				grants = append(grants, g...)
			}
		}
	}
	if cr.Spec.S3 != nil {
		for _, b := range cr.Spec.S3.Resources {
			grants, skipped = recordOwned(grants, skipped, cr, resourceTypeS3, b.Name)
			if b.Encryption != nil {
				if b.Encryption.Enabled {
					grants, skipped = recordOwned(grants, skipped, cr, resourceTypeKMS, cloudctlaws.DedicatedKeyLedgerName(resourceTypeS3, b.Name))
				}
			}
		}
		for _, ref := range cr.Spec.S3.Consumes {
			g, reason, err := resolveConsume(ctx, k8sClient, cr, resourceTypeS3, ref)
			switch {
			case err != nil:
				return nil, nil, err
			case reason != "":
				skipped = append(skipped, reason)
			default:
				grants = append(grants, g...)
			}
		}
	}
	if cr.Spec.KMS != nil {
		// Every key this CR declares gets an owned grant, which also covers
		// any of its resources that reference one through encryption.kmsKeyRef.
		// Keys are never shared across CRs, so there's nothing to resolve.
		for _, k := range cr.Spec.KMS.Resources {
			grants, skipped = recordOwned(grants, skipped, cr, resourceTypeKMS, k.Name)
		}
	}

	return grants, skipped, nil
}

// recordOwned appends a grant for an owned resource if it has a ledger
// entry (an ARN to grant access to), or a skip reason if it doesn't yet -
// e.g. a transient failure creating it, or simply not having run yet this
// reconcile. Kept symmetric with resolveConsume's own skip reasons: an
// owned resource silently missing from the derived policy with no
// explanation would be a real regression in visibility compared to a
// consumed one.
func recordOwned(grants []grant, skipped []string, cr *depsv1alpha1.AppDependencies, resourceType, name string) ([]grant, []string) {
	if entry, found := status.FindManagedResource(cr.Status.ManagedResources, resourceType, name); found {
		return append(grants, grant{resourceType: resourceType, arn: entry.ARN, readWrite: true}), skipped
	}
	return grants, append(skipped, fmt.Sprintf("owned %s resource %q not reconciled yet", resourceType, name))
}

// resolveConsume fetches the producer CR, finds the matching sharedWith
// grant naming this CR, and resolves the resource's ARN from the
// producer's own ledger, plus its encryption key's when it has one.
// Returns a non-empty reason instead of an error for anything not yet
// resolvable - see collectGrants' doc comment. A failed read is an error,
// never a reason: it says nothing about whether access was revoked, so
// callers must not narrow or remove anything on it.
func resolveConsume(ctx context.Context, k8sClient client.Client, consumer *depsv1alpha1.AppDependencies, resourceType string, ref depsv1alpha1.ConsumeRef) ([]grant, string, error) {
	prefix := fmt.Sprintf("%s/%s consumes %s/%s's %q", consumer.Namespace, consumer.Name, ref.Namespace, ref.Name, ref.ResourceName)

	var producer depsv1alpha1.AppDependencies
	key := client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}
	if err := k8sClient.Get(ctx, key, &producer); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, prefix + ": producer CR not found yet", nil
		}
		return nil, "", &cloudctlaws.ReconcileError{Err: fmt.Errorf("reading producer %s/%s: %w", ref.Namespace, ref.Name, err), Retryable: true}
	}

	sharedWith, encryption, ok := producerResource(&producer, resourceType, ref.ResourceName)
	if !ok {
		return nil, prefix + ": resource not declared there yet", nil
	}

	var grantedTo *depsv1alpha1.SharedWithEntry
	for i := range sharedWith {
		if sharedWith[i].Namespace == consumer.Namespace && sharedWith[i].Name == consumer.Name {
			grantedTo = &sharedWith[i]
			break
		}
	}
	if grantedTo == nil {
		return nil, prefix + ": not authorized - owner hasn't granted this CR in sharedWith", nil
	}

	entry, found := status.FindManagedResource(producer.Status.ManagedResources, resourceType, ref.ResourceName)
	if !found {
		return nil, prefix + ": not reconciled there yet", nil
	}

	readWrite := grantedTo.Access == depsv1alpha1.AccessLevelReadWrite
	grants := []grant{{resourceType: resourceType, arn: entry.ARN, readWrite: readWrite, consumed: true}}

	// DynamoDB uses the key on the caller's behalf through its own grant.
	if keyName := keyLedgerName(resourceType, ref.ResourceName, encryption); keyName != "" && resourceType != resourceTypeDynamoDB {
		key, found := status.FindManagedResource(producer.Status.ManagedResources, resourceTypeKMS, keyName)
		if !found {
			return nil, prefix + ": its encryption key is not reconciled there yet", nil
		}
		grants = append(grants, grant{resourceType: resourceTypeKMS, arn: key.ARN, readWrite: readWrite})
	}
	return grants, "", nil
}

// keyLedgerName is the ledger name of the key a resource is encrypted with,
// or "" when it isn't encrypted with a customer managed key.
func keyLedgerName(resourceType, name string, encryption *depsv1alpha1.EncryptionSpec) string {
	switch {
	case encryption == nil:
		return ""
	case encryption.KMSKeyRef != "":
		return encryption.KMSKeyRef
	case encryption.Enabled:
		return cloudctlaws.DedicatedKeyLedgerName(resourceType, name)
	default:
		return ""
	}
}

// ResolveConsumeARN resolves a ConsumeRef to the ARN of the resource it
// points at, but only if the producer CR currently exists, has reconciled
// that resource, and has actually granted this consumer access via
// sharedWith - the exact same authorization check collectGrants applies
// when deriving IAM policy. Returns ("", false) if any of that isn't true
// yet, with no error (same self-resolving-forward-reference philosophy as
// collectGrants).
//
// Exported so internal/resources/configmap can reuse this authorization
// check rather than duplicate it: the ConfigMap it generates must never
// expose a resource identifier this CR wasn't actually granted IAM access
// to, and that has to stay in lockstep with collectGrants automatically,
// not by two separate implementations agreeing by coincidence.
func ResolveConsumeARN(ctx context.Context, k8sClient client.Client, consumer *depsv1alpha1.AppDependencies, resourceType string, ref depsv1alpha1.ConsumeRef) (arn string, ok bool, err error) {
	grants, reason, err := resolveConsume(ctx, k8sClient, consumer, resourceType, ref)
	if err != nil || reason != "" {
		return "", false, err
	}
	return grants[0].arn, true, nil
}

// producerResource returns the sharedWith list and encryption of one
// resource entry within the producer's given section, regardless of
// resource type.
func producerResource(producer *depsv1alpha1.AppDependencies, resourceType, resourceName string) ([]depsv1alpha1.SharedWithEntry, *depsv1alpha1.EncryptionSpec, bool) {
	switch resourceType {
	case resourceTypeSQS:
		if producer.Spec.SQS == nil {
			return nil, nil, false
		}
		for _, q := range producer.Spec.SQS.Resources {
			if q.Name == resourceName {
				return q.SharedWith, q.Encryption, true
			}
		}
	case resourceTypeSNS:
		if producer.Spec.SNS == nil {
			return nil, nil, false
		}
		for _, t := range producer.Spec.SNS.Resources {
			if t.Name == resourceName {
				return t.SharedWith, t.Encryption, true
			}
		}
	case resourceTypeDynamoDB:
		if producer.Spec.DynamoDB == nil {
			return nil, nil, false
		}
		for _, tbl := range producer.Spec.DynamoDB.Resources {
			if tbl.Name == resourceName {
				return tbl.SharedWith, tbl.Encryption, true
			}
		}
	case resourceTypeS3:
		if producer.Spec.S3 == nil {
			return nil, nil, false
		}
		for _, b := range producer.Spec.S3.Resources {
			if b.Name == resourceName {
				return b.SharedWith, b.Encryption, true
			}
		}
	}
	return nil, nil, false
}

// policyStatement mirrors the small subset of AWS policy JSON shape this
// package needs to emit. Existing statements from elsewhere are never
// parsed or preserved here, since this inline policy is entirely owned and
// overwritten by this operator every reconcile.
type policyStatement struct {
	Sid      string   `json:"Sid"`
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []policyStatement `json:"Statement"`
}

// buildPolicyDocument turns resolved grants into an IAM policy document.
func buildPolicyDocument(grants []grant) (string, error) {
	doc := policyDocument{Version: iamPolicyVersion}
	for i, g := range grants {
		if g.resourceType == resourceTypeS3 {
			doc.Statement = append(doc.Statement,
				policyStatement{
					Sid:      fmt.Sprintf("s3Bucket%d", i),
					Effect:   effectAllow,
					Action:   s3BucketLevelActions,
					Resource: []string{g.arn},
				},
				policyStatement{
					Sid:      fmt.Sprintf("s3Object%d", i),
					Effect:   effectAllow,
					Action:   s3ObjectActions(g.readWrite),
					Resource: []string{g.arn + "/*"},
				},
			)
			continue
		}
		resources := []string{g.arn}
		if g.resourceType == resourceTypeDynamoDB {
			// Queries and scans of a secondary index are authorized on the
			// index's ARN, not the table's.
			resources = append(resources, g.arn+"/index/*")
		}
		doc.Statement = append(doc.Statement, policyStatement{
			Sid:      fmt.Sprintf("%s%d", g.resourceType, i),
			Effect:   effectAllow,
			Action:   grantActions(g),
			Resource: resources,
		})
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("encoding IAM policy document: %w", err)
	}
	return string(encoded), nil
}
