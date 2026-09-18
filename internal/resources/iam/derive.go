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

	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
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

var actionSets = map[string]actionSet{
	"sqs": {
		baseline:  []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ChangeMessageVisibility"},
		readWrite: []string{"sqs:SendMessage", "sqs:SendMessageBatch"},
	},
	"sns": {
		baseline:  []string{"sns:Subscribe", "sns:Unsubscribe", "sns:GetTopicAttributes"},
		readWrite: []string{"sns:Publish"},
	},
	"dynamodb": {
		baseline:  []string{"dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem"},
		readWrite: []string{"dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:BatchWriteItem"},
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
	"kms": {
		baseline:  []string{"kms:Decrypt"},
		readWrite: []string{"kms:GenerateDataKey"},
	},
}

// S3 is handled separately from actionSets: it needs two different ARN
// scopes per grant (the bare bucket ARN for ListBucket, the bucket ARN
// with a "/*" suffix for everything else), so one Resource per statement
// like every other type here doesn't fit.
var (
	s3BucketLevelActions = []string{"s3:ListBucket"}
	s3ObjectBaseline     = []string{"s3:GetObject"}
	s3ObjectReadWrite    = []string{"s3:PutObject", "s3:DeleteObject"}
)

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
	resourceType string // "sqs", "sns", "dynamodb", "s3"
	arn          string
	readWrite    bool
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
func collectGrants(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) (grants []grant, skipped []string) {
	if cr.Spec.SQS != nil {
		for _, q := range cr.Spec.SQS.Resources {
			grants, skipped = recordOwned(grants, skipped, "sqs", q.Name, findLedgerEntry(cr, "sqs", q.Name))
			if q.Encryption != nil && q.Encryption.Enabled {
				// The dedicated key is owned by this CR exactly as much as
				// the queue it protects - same "always full access to what
				// you own" rule, via the same recordOwned helper, keyed to
				// the same ledger name kms.EnsureDedicatedKey uses.
				grants, skipped = recordOwned(grants, skipped, "kms", q.Name+"-key", findLedgerEntry(cr, "kms", q.Name+"-key"))
			}
		}
		for _, ref := range cr.Spec.SQS.Consumes {
			if g, reason := resolveConsume(ctx, k8sClient, cr, "sqs", ref); reason != "" {
				skipped = append(skipped, reason)
			} else {
				grants = append(grants, g)
			}
		}
	}
	if cr.Spec.SNS != nil {
		for _, t := range cr.Spec.SNS.Resources {
			grants, skipped = recordOwned(grants, skipped, "sns", t.Name, findLedgerEntry(cr, "sns", t.Name))
			if t.Encryption != nil && t.Encryption.Enabled {
				grants, skipped = recordOwned(grants, skipped, "kms", t.Name+"-key", findLedgerEntry(cr, "kms", t.Name+"-key"))
			}
		}
		for _, ref := range cr.Spec.SNS.Consumes {
			if g, reason := resolveConsume(ctx, k8sClient, cr, "sns", ref); reason != "" {
				skipped = append(skipped, reason)
			} else {
				grants = append(grants, g)
			}
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, tbl := range cr.Spec.DynamoDB.Resources {
			grants, skipped = recordOwned(grants, skipped, "dynamodb", tbl.Name, findLedgerEntry(cr, "dynamodb", tbl.Name))
			if tbl.Encryption != nil && tbl.Encryption.Enabled {
				grants, skipped = recordOwned(grants, skipped, "kms", tbl.Name+"-key", findLedgerEntry(cr, "kms", tbl.Name+"-key"))
			}
		}
		for _, ref := range cr.Spec.DynamoDB.Consumes {
			if g, reason := resolveConsume(ctx, k8sClient, cr, "dynamodb", ref); reason != "" {
				skipped = append(skipped, reason)
			} else {
				grants = append(grants, g)
			}
		}
	}
	if cr.Spec.S3 != nil {
		for _, b := range cr.Spec.S3.Resources {
			grants, skipped = recordOwned(grants, skipped, "s3", b.Name, findLedgerEntry(cr, "s3", b.Name))
			if b.Encryption != nil && b.Encryption.Enabled {
				grants, skipped = recordOwned(grants, skipped, "kms", b.Name+"-key", findLedgerEntry(cr, "kms", b.Name+"-key"))
			}
		}
		for _, ref := range cr.Spec.S3.Consumes {
			if g, reason := resolveConsume(ctx, k8sClient, cr, "s3", ref); reason != "" {
				skipped = append(skipped, reason)
			} else {
				grants = append(grants, g)
			}
		}
	}
	if cr.Spec.KMS != nil {
		// A standalone kms.resources entry (as opposed to a dedicated key
		// EnsureDedicatedKey provisions for another section's own
		// resource) gets an owned grant exactly like every other resource
		// type - and its consumes entries resolve exactly the same way,
		// via the same sharedWith authorization every other cross-CR
		// reference already uses (see producerSharedWith's "kms" case).
		for _, k := range cr.Spec.KMS.Resources {
			grants, skipped = recordOwned(grants, skipped, "kms", k.Name, findLedgerEntry(cr, "kms", k.Name))
		}
		for _, ref := range cr.Spec.KMS.Consumes {
			if g, reason := resolveConsume(ctx, k8sClient, cr, "kms", ref); reason != "" {
				skipped = append(skipped, reason)
			} else {
				grants = append(grants, g)
			}
		}
	}

	return grants, skipped
}

// recordOwned appends a grant for an owned resource if it has a ledger
// entry (an ARN to grant access to), or a skip reason if it doesn't yet -
// e.g. a transient failure creating it, or simply not having run yet this
// reconcile. Kept symmetric with resolveConsume's own skip reasons: an
// owned resource silently missing from the derived policy with no
// explanation would be a real regression in visibility compared to a
// consumed one.
func recordOwned(grants []grant, skipped []string, resourceType, name string, entry *depsv1alpha1.ManagedResource) ([]grant, []string) {
	if entry != nil {
		return append(grants, grant{resourceType: resourceType, arn: entry.ARN, readWrite: true}), skipped
	}
	return grants, append(skipped, fmt.Sprintf("owned %s resource %q not reconciled yet", resourceType, name))
}

func findLedgerEntry(cr *depsv1alpha1.AppDependencies, resourceType, name string) *depsv1alpha1.ManagedResource {
	for i := range cr.Status.ManagedResources {
		e := &cr.Status.ManagedResources[i]
		if e.Type == resourceType && e.Name == name {
			return e
		}
	}
	return nil
}

// resolveConsume fetches the producer CR, finds the matching sharedWith
// grant naming this CR, and resolves the resource's ARN from the
// producer's own ledger. Returns a non-empty reason instead of an error
// for anything not yet resolvable - see collectGrants' doc comment.
func resolveConsume(ctx context.Context, k8sClient client.Client, consumer *depsv1alpha1.AppDependencies, resourceType string, ref depsv1alpha1.ConsumeRef) (grant, string) {
	prefix := fmt.Sprintf("%s/%s consumes %s/%s's %q", consumer.Namespace, consumer.Name, ref.Namespace, ref.Name, ref.ResourceName)

	var producer depsv1alpha1.AppDependencies
	key := client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}
	if err := k8sClient.Get(ctx, key, &producer); err != nil {
		return grant{}, prefix + ": producer CR not found yet"
	}

	sharedWith, ok := producerSharedWith(&producer, resourceType, ref.ResourceName)
	if !ok {
		return grant{}, prefix + ": resource not declared there yet"
	}

	var grantedTo *depsv1alpha1.SharedWithEntry
	for i := range sharedWith {
		if sharedWith[i].Namespace == consumer.Namespace && sharedWith[i].Name == consumer.Name {
			grantedTo = &sharedWith[i]
			break
		}
	}
	if grantedTo == nil {
		return grant{}, prefix + ": not authorized - owner hasn't granted this CR in sharedWith"
	}

	entry := findLedgerEntry(&producer, resourceType, ref.ResourceName)
	if entry == nil {
		return grant{}, prefix + ": not reconciled there yet"
	}

	return grant{
		resourceType: resourceType,
		arn:          entry.ARN,
		readWrite:    grantedTo.Access == depsv1alpha1.AccessLevelReadWrite,
	}, ""
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
func ResolveConsumeARN(ctx context.Context, k8sClient client.Client, consumer *depsv1alpha1.AppDependencies, resourceType string, ref depsv1alpha1.ConsumeRef) (arn string, ok bool) {
	g, reason := resolveConsume(ctx, k8sClient, consumer, resourceType, ref)
	if reason != "" {
		return "", false
	}
	return g.arn, true
}

// producerSharedWith returns the sharedWith list for one resource entry
// within the producer's given section, regardless of resource type.
func producerSharedWith(producer *depsv1alpha1.AppDependencies, resourceType, resourceName string) ([]depsv1alpha1.SharedWithEntry, bool) {
	switch resourceType {
	case "sqs":
		if producer.Spec.SQS == nil {
			return nil, false
		}
		for _, q := range producer.Spec.SQS.Resources {
			if q.Name == resourceName {
				return q.SharedWith, true
			}
		}
	case "sns":
		if producer.Spec.SNS == nil {
			return nil, false
		}
		for _, t := range producer.Spec.SNS.Resources {
			if t.Name == resourceName {
				return t.SharedWith, true
			}
		}
	case "dynamodb":
		if producer.Spec.DynamoDB == nil {
			return nil, false
		}
		for _, tbl := range producer.Spec.DynamoDB.Resources {
			if tbl.Name == resourceName {
				return tbl.SharedWith, true
			}
		}
	case "s3":
		if producer.Spec.S3 == nil {
			return nil, false
		}
		for _, b := range producer.Spec.S3.Resources {
			if b.Name == resourceName {
				return b.SharedWith, true
			}
		}
	case "kms":
		if producer.Spec.KMS == nil {
			return nil, false
		}
		for _, k := range producer.Spec.KMS.Resources {
			if k.Name == resourceName {
				return k.SharedWith, true
			}
		}
	}
	return nil, false
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
	doc := policyDocument{Version: "2012-10-17"}
	for i, g := range grants {
		if g.resourceType == "s3" {
			doc.Statement = append(doc.Statement,
				policyStatement{
					Sid:      fmt.Sprintf("s3Bucket%d", i),
					Effect:   "Allow",
					Action:   s3BucketLevelActions,
					Resource: []string{g.arn},
				},
				policyStatement{
					Sid:      fmt.Sprintf("s3Object%d", i),
					Effect:   "Allow",
					Action:   s3ObjectActions(g.readWrite),
					Resource: []string{g.arn + "/*"},
				},
			)
			continue
		}
		doc.Statement = append(doc.Statement, policyStatement{
			Sid:      fmt.Sprintf("%s%d", g.resourceType, i),
			Effect:   "Allow",
			Action:   actionsFor(g.resourceType, g.readWrite),
			Resource: []string{g.arn},
		})
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("encoding IAM policy document: %w", err)
	}
	return string(encoded), nil
}
