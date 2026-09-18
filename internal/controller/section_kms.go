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

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
)

// dedicatedKMSKeyNames collects the ledger names of dedicated KMS keys
// every other section's own encryption.enabled resources currently want,
// for kms.Cleanup's declared-vs-ledger diff — those keys are never
// declared in spec.KMS.Resources itself (see EnsureDedicatedKey's own
// doc comment), so without this, kmsSection's own Cleanup would see every
// dedicated key as unwanted and try to delete it on every reconcile,
// regardless of whether the resource that actually owns it still exists
// and still wants it.
//
// Checks every section that has an encryption field: sqs, sns, dynamodb,
// s3 - every resource type this operator manages now supports it.
func dedicatedKMSKeyNames(cr *depsv1alpha1.AppDependencies) []string {
	var names []string
	if cr.Spec.SQS != nil {
		for _, q := range cr.Spec.SQS.Resources {
			if q.Encryption != nil && q.Encryption.Enabled {
				names = append(names, q.Name+"-key")
			}
		}
	}
	if cr.Spec.SNS != nil {
		for _, t := range cr.Spec.SNS.Resources {
			if t.Encryption != nil && t.Encryption.Enabled {
				names = append(names, t.Name+"-key")
			}
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, tbl := range cr.Spec.DynamoDB.Resources {
			if tbl.Encryption != nil && tbl.Encryption.Enabled {
				names = append(names, tbl.Name+"-key")
			}
		}
	}
	if cr.Spec.S3 != nil {
		for _, b := range cr.Spec.S3.Resources {
			if b.Encryption != nil && b.Encryption.Enabled {
				names = append(names, b.Name+"-key")
			}
		}
	}
	return names
}

// kmsSection adapts the kms package's Ensure/Cleanup to the orchestrator's
// uniform section shape. Registered before iamSection but after every
// other resource section — nothing derives grants against a KMS key yet
// (that's future work, once other resource types can reference one via an
// encryption field), but keeping it ahead of iamSection now means a
// dedicated-key grant will already have somewhere to plug in without
// reordering sections later.
func kmsSection(awsClients *cloudctlaws.Clients) section {
	return section{
		name: "KMSReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			declared := 0
			if cr.Spec.KMS != nil {
				declared = len(cr.Spec.KMS.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, "kms", declared)
			defer cancel()

			ledger, ensureErr := kms.Ensure(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, cr.Status.ManagedResources,
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := kms.Cleanup(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, dedicatedKMSKeyNames(cr), cr.Status.ManagedResources, false,
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(cr, "KMSReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, error) {
			declared := 0
			if cr.Spec.KMS != nil {
				declared = len(cr.Spec.KMS.Resources)
			}
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, "kms", declared)
			defer cancel()

			// dedicatedKeysStillNeeded is irrelevant here — deleting is
			// true, so kms.Cleanup tears down every kms-type ledger entry
			// regardless, the same as every other resource type's finalize.
			ledger, results, err := kms.Cleanup(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, nil, cr.Status.ManagedResources, true,
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, err
			}
			for _, r := range results {
				if r.Reason == kms.CleanupReasonPendingDeletion {
					return false, nil
				}
			}
			return true, nil
		},
	}
}
