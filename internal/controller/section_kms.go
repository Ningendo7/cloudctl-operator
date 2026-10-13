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
	"fmt"
	"slices"
	"strings"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
)

// encryptedResource is one declared resource that can carry an encryption
// block, whichever section it comes from.
type encryptedResource struct {
	resourceType string
	name         string
	encryption   *depsv1alpha1.EncryptionSpec
}

// encryptedResources lists every resource in cr with an encryption block,
// across every section that supports one.
func encryptedResources(cr *depsv1alpha1.AppDependencies) []encryptedResource {
	var out []encryptedResource
	if cr.Spec.SQS != nil {
		for _, r := range cr.Spec.SQS.Resources {
			out = append(out, encryptedResource{resourceTypeSQS, r.Name, r.Encryption})
		}
	}
	if cr.Spec.SNS != nil {
		for _, r := range cr.Spec.SNS.Resources {
			out = append(out, encryptedResource{resourceTypeSNS, r.Name, r.Encryption})
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, r := range cr.Spec.DynamoDB.Resources {
			out = append(out, encryptedResource{resourceTypeDynamoDB, r.Name, r.Encryption})
		}
	}
	if cr.Spec.S3 != nil {
		for _, r := range cr.Spec.S3.Resources {
			out = append(out, encryptedResource{resourceTypeS3, r.Name, r.Encryption})
		}
	}
	if cr.Spec.RDS != nil {
		for _, r := range cr.Spec.RDS.Resources {
			out = append(out, encryptedResource{resourceTypeRDS, r.Name, r.Encryption})
		}
	}
	return out
}

// referencedKMSKeyNames returns the names, under kms.resources, of the keys
// this CR's resources reference through encryption.kmsKeyRef.
func referencedKMSKeyNames(cr *depsv1alpha1.AppDependencies) []string {
	var names []string
	for _, r := range encryptedResources(cr) {
		if r.encryption != nil && r.encryption.KMSKeyRef != "" && !slices.Contains(names, r.encryption.KMSKeyRef) {
			names = append(names, r.encryption.KMSKeyRef)
		}
	}
	return names
}

// neededKMSKeyNames collects the ledger names of keys this CR still needs
// but that kms.Cleanup wouldn't otherwise see as declared: dedicated keys
// of resources with encryption.enabled (never listed in spec.kms.resources,
// see EnsureDedicatedKey), and keys a resource still references through
// kmsKeyRef. Without it every such key would look unwanted on every pass.
func neededKMSKeyNames(cr *depsv1alpha1.AppDependencies) []string {
	names := referencedKMSKeyNames(cr)
	for _, r := range encryptedResources(cr) {
		if r.encryption != nil && r.encryption.Enabled {
			names = append(names, kms.DedicatedKeyLedgerName(r.resourceType, r.name))
		}
	}
	return names
}

// undeclaredKMSKeyRefs returns referenced key names missing from
// spec.kms.resources.
func undeclaredKMSKeyRefs(cr *depsv1alpha1.AppDependencies) []string {
	declared := map[string]bool{}
	if cr.Spec.KMS != nil {
		for _, k := range cr.Spec.KMS.Resources {
			declared[k.Name] = true
		}
	}
	var missing []string
	for _, name := range referencedKMSKeyNames(cr) {
		if !declared[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

// kmsSection adapts the kms package's Ensure/Cleanup to the orchestrator's
// uniform section shape. Runs first, so a key a resource references through
// kmsKeyRef already exists in the ledger when that resource reconciles.
func kmsSection(r *AppDependenciesReconciler, original *depsv1alpha1.AppDependencies) section {
	awsClients := r.AWSClients
	return section{
		name: conditionTypeKMSReady,
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			declared := 0
			if cr.Spec.KMS != nil {
				declared = len(cr.Spec.KMS.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, "kms", declared)
			defer cancel()

			ledger, ensureErr := kms.Ensure(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, cr.Status.ManagedResources, checkpointFor(r, cr, original), eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := kms.Cleanup(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, neededKMSKeyNames(cr), cr.Status.ManagedResources, false, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			// A kmsKeyRef to a key this CR never declared can't resolve no
			// matter how long the resource waits, so it gets its own
			// permanent reason rather than the resource's "not ready yet".
			conditionErr := err
			if conditionErr == nil {
				if missing := undeclaredKMSKeyRefs(cr); len(missing) > 0 {
					conditionErr = &cloudctlaws.ReconcileError{
						Err:    fmt.Errorf("encryption.kmsKeyRef names %s, not declared under kms.resources", strings.Join(missing, ", ")),
						Reason: "KMSKeyRefNotDeclared",
					}
				}
			}
			setSectionCondition(ctx, cr, conditionTypeKMSReady, conditionErr, eventRecorderFor(r, cr))
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, []string, error) {
			declared := 0
			if cr.Spec.KMS != nil {
				declared = len(cr.Spec.KMS.Resources)
			}
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, "kms", declared)
			defer cancel()

			// The needed-keys list is irrelevant here — deleting is true,
			// so kms.Cleanup tears down every kms-type ledger entry
			// regardless, the same as every other resource type's finalize.
			ledger, results, err := kms.Cleanup(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, nil, cr.Status.ManagedResources, true, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, nil, err
			}
			var blocked []string
			for _, res := range results {
				if res.Reason == kms.CleanupReasonPendingDeletion {
					blocked = append(blocked, fmt.Sprintf("kms %q: %s", res.Name, res.Reason))
				}
			}
			if len(blocked) > 0 {
				return false, blocked, nil
			}
			return true, nil, nil
		},
	}
}
