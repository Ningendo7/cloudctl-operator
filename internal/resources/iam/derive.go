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
	resourceTypeRDS      = "rds"

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
		baseline:  []string{"dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem"},
		readWrite: []string{"dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:BatchWriteItem", "dynamodb:ConditionCheckItem"},
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

func collectGrants(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) (grants []grant, skipped []string, err error) {
	for _, sec := range grantSections(cr) {
		for _, owned := range sec.owned {
			grants, skipped = recordOwned(grants, skipped, cr, sec.resourceType, owned.name)
			if owned.dedicatedKey {
				// The dedicated key is owned by this CR exactly as much as
				// the resource it protects - same "always full access to what
				// you own" rule, via the same recordOwned helper, keyed to
				// the same ledger name kms.EnsureDedicatedKey uses.
				grants, skipped = recordOwned(grants, skipped, cr, resourceTypeKMS, cloudctlaws.DedicatedKeyLedgerName(sec.resourceType, owned.name))
			}
		}
		for _, ref := range sec.consumes {
			g, reason, err := resolveConsume(ctx, k8sClient, cr, sec.resourceType, ref)
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

// grantSection is what one declared section contributes: the resources the
// CR owns and the ones it consumes from other CRs.
type grantSection struct {
	resourceType string
	owned        []ownedResource
	consumes     []depsv1alpha1.ConsumeRef
}

type ownedResource struct {
	name string
	// dedicatedKey is set when the resource has a key of its own, which the
	// CR owns as well.
	dedicatedKey bool
}

// grantSections lists the declared sections that have owned resources and
// consumes (everything but KMS), in the order their grants are collected.
func grantSections(cr *depsv1alpha1.AppDependencies) []grantSection {
	var sections []grantSection
	if s := cr.Spec.SQS; s != nil {
		sections = append(sections, grantSection{resourceTypeSQS, ownedOf(s.Resources, func(q depsv1alpha1.SQSQueueSpec) (string, *depsv1alpha1.EncryptionSpec) { return q.Name, q.Encryption }), s.Consumes})
	}
	if s := cr.Spec.SNS; s != nil {
		sections = append(sections, grantSection{resourceTypeSNS, ownedOf(s.Resources, func(t depsv1alpha1.SNSTopicSpec) (string, *depsv1alpha1.EncryptionSpec) { return t.Name, t.Encryption }), s.Consumes})
	}
	if s := cr.Spec.DynamoDB; s != nil {
		sections = append(sections, grantSection{resourceTypeDynamoDB, ownedOf(s.Resources, func(t depsv1alpha1.DynamoDBTableSpec) (string, *depsv1alpha1.EncryptionSpec) {
			return t.Name, t.Encryption
		}), s.Consumes})
	}
	if s := cr.Spec.S3; s != nil {
		sections = append(sections, grantSection{resourceTypeS3, ownedOf(s.Resources, func(b depsv1alpha1.S3BucketSpec) (string, *depsv1alpha1.EncryptionSpec) { return b.Name, b.Encryption }), s.Consumes})
	}
	return sections
}

// ownedOf lists a section's resources with whether each has a dedicated key.
func ownedOf[T any](items []T, nameAndEncryption func(T) (string, *depsv1alpha1.EncryptionSpec)) []ownedResource {
	owned := make([]ownedResource, 0, len(items))
	for _, item := range items {
		name, encryption := nameAndEncryption(item)
		owned = append(owned, ownedResource{name: name, dedicatedKey: encryption != nil && encryption.Enabled})
	}
	return owned
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

	lookup, err := lookupConsume(ctx, k8sClient, consumer, resourceType, ref)
	if err != nil {
		return nil, "", err
	}
	switch lookup.problem {
	case ReferenceProducerNotFound:
		return nil, prefix + ": producer CR not found yet", nil
	case ReferenceResourceNotDeclared:
		return nil, prefix + ": resource not declared there yet", nil
	case ReferenceNotAuthorized:
		return nil, prefix + ": not authorized - owner hasn't granted this CR in sharedWith", nil
	default:
		// Resolved: go on to what the producer has published.
	}
	producer, grantedTo, encryption := lookup.producer, lookup.grantedTo, lookup.encryption

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

// ReferenceProblem says why a consumes-style reference does not resolve yet.
type ReferenceProblem string

const (
	// ReferenceResolved: the producer exists, declares the resource, and has
	// granted the consumer in sharedWith.
	ReferenceResolved ReferenceProblem = ""
	// ReferenceProducerNotFound: no AppDependencies CR has that namespace and
	// name.
	ReferenceProducerNotFound ReferenceProblem = "ProducerNotFound"
	// ReferenceResourceNotDeclared: the CR exists but declares no such
	// resource.
	ReferenceResourceNotDeclared ReferenceProblem = "ResourceNotDeclared"
	// ReferenceNotAuthorized: the resource exists but its owner has not listed
	// the consumer in sharedWith.
	ReferenceNotAuthorized ReferenceProblem = "NotAuthorized"
)

type consumeLookup struct {
	problem    ReferenceProblem
	producer   *depsv1alpha1.AppDependencies
	grantedTo  *depsv1alpha1.SharedWithEntry
	encryption *depsv1alpha1.EncryptionSpec
}

// lookupConsume is the part of resolving a reference that does not depend on
// the producer having reconciled yet: whether the CR, the resource and the
// grant exist. Policy derivation and the status condition both go through
// it, so what status calls authorized is exactly what gets IAM access. A
// failed read is an error, never a problem: it says nothing about the
// reference.
func lookupConsume(ctx context.Context, k8sClient client.Client, consumer *depsv1alpha1.AppDependencies, resourceType string, ref depsv1alpha1.ConsumeRef) (consumeLookup, error) {
	var producer depsv1alpha1.AppDependencies
	key := client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}
	if err := k8sClient.Get(ctx, key, &producer); err != nil {
		if apierrors.IsNotFound(err) {
			return consumeLookup{problem: ReferenceProducerNotFound}, nil
		}
		return consumeLookup{}, &cloudctlaws.ReconcileError{Err: fmt.Errorf("reading producer %s/%s: %w", ref.Namespace, ref.Name, err), Retryable: true}
	}

	sharedWith, encryption, ok := producerResource(&producer, resourceType, ref.ResourceName)
	if !ok {
		return consumeLookup{problem: ReferenceResourceNotDeclared}, nil
	}

	for i := range sharedWith {
		if sharedWith[i].Namespace == consumer.Namespace && sharedWith[i].Name == consumer.Name {
			return consumeLookup{producer: &producer, grantedTo: &sharedWith[i], encryption: encryption}, nil
		}
	}
	return consumeLookup{problem: ReferenceNotAuthorized}, nil
}

// CheckConsumeReference reports why a reference from consumer to another
// CR's resource does not resolve, or ReferenceResolved. A failed read of the
// producer is returned as an error.
func CheckConsumeReference(ctx context.Context, k8sClient client.Client, consumer *depsv1alpha1.AppDependencies, resourceType string, ref depsv1alpha1.ConsumeRef) (ReferenceProblem, error) {
	lookup, err := lookupConsume(ctx, k8sClient, consumer, resourceType, ref)
	return lookup.problem, err
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

// producerEntry is one resource a CR owns, as far as another CR's access to
// it is concerned.
type producerEntry struct {
	name       string
	sharedWith []depsv1alpha1.SharedWithEntry
	encryption *depsv1alpha1.EncryptionSpec
}

// producerEntries lists, per resource type, what the CR owns of that type.
var producerEntries = map[string]func(*depsv1alpha1.AppDependenciesSpec) []producerEntry{
	resourceTypeSQS: func(spec *depsv1alpha1.AppDependenciesSpec) []producerEntry {
		var entries []producerEntry
		if spec.SQS != nil {
			for _, q := range spec.SQS.Resources {
				entries = append(entries, producerEntry{q.Name, q.SharedWith, q.Encryption})
			}
		}
		return entries
	},
	resourceTypeSNS: func(spec *depsv1alpha1.AppDependenciesSpec) []producerEntry {
		var entries []producerEntry
		if spec.SNS != nil {
			for _, t := range spec.SNS.Resources {
				entries = append(entries, producerEntry{t.Name, t.SharedWith, t.Encryption})
			}
		}
		return entries
	},
	resourceTypeDynamoDB: func(spec *depsv1alpha1.AppDependenciesSpec) []producerEntry {
		var entries []producerEntry
		if spec.DynamoDB != nil {
			for _, tbl := range spec.DynamoDB.Resources {
				entries = append(entries, producerEntry{tbl.Name, tbl.SharedWith, tbl.Encryption})
			}
		}
		return entries
	},
	resourceTypeS3: func(spec *depsv1alpha1.AppDependenciesSpec) []producerEntry {
		var entries []producerEntry
		if spec.S3 != nil {
			for _, b := range spec.S3.Resources {
				entries = append(entries, producerEntry{b.Name, b.SharedWith, b.Encryption})
			}
		}
		return entries
	},
	// An instance's access is a network rule, not IAM, so it has no key
	// grant to derive.
	resourceTypeRDS: func(spec *depsv1alpha1.AppDependenciesSpec) []producerEntry {
		var entries []producerEntry
		if spec.RDS != nil {
			for _, db := range spec.RDS.Resources {
				entries = append(entries, producerEntry{db.Name, db.SharedWith, nil})
			}
		}
		return entries
	},
}

// producerResource returns the sharedWith list and encryption of one
// resource entry within the producer's given section, regardless of
// resource type.
func producerResource(producer *depsv1alpha1.AppDependencies, resourceType, resourceName string) ([]depsv1alpha1.SharedWithEntry, *depsv1alpha1.EncryptionSpec, bool) {
	entries, ok := producerEntries[resourceType]
	if !ok {
		return nil, nil, false
	}
	for _, e := range entries(&producer.Spec) {
		if e.name == resourceName {
			return e.sharedWith, e.encryption, true
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
