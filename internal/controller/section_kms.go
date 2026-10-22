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
	"github.com/Ningendo7/cloudctl-operator/internal/decisions"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const resourceTypeKMS = "kms"

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

// neededKMSKeyNames collects the ledger names of keys this CR's spec still
// needs but that kms.Cleanup wouldn't otherwise see as declared: dedicated
// keys of resources with encryption.enabled (never listed in
// spec.kms.resources, see EnsureDedicatedKey), and keys a resource still
// references through kmsKeyRef.
func neededKMSKeyNames(cr *depsv1alpha1.AppDependencies) []string {
	names := referencedKMSKeyNames(cr)
	for _, r := range encryptedResources(cr) {
		if r.encryption != nil && r.encryption.Enabled {
			names = append(names, kms.DedicatedKeyLedgerName(r.resourceType, r.name))
		}
	}
	return names
}

// kmsKeyLedgerName is the name, among this CR's kms ledger entries, of the
// key a resource's encryption block uses, or "" if it has none.
func kmsKeyLedgerName(r encryptedResource) string {
	switch {
	case r.encryption == nil:
		return ""
	case r.encryption.KMSKeyRef != "":
		return r.encryption.KMSKeyRef
	case r.encryption.Enabled:
		return kms.DedicatedKeyLedgerName(r.resourceType, r.name)
	default:
		return ""
	}
}

// recordKMSKeyARNs stamps each declared resource's ledger entry with the ARN
// of the key it is encrypted with. Once the resource leaves spec, this is
// the only record of which key its data needs. An SQS dead-letter queue
// uses its main queue's key.
func recordKMSKeyARNs(cr *depsv1alpha1.AppDependencies) {
	ledger := cr.Status.ManagedResources
	for _, r := range encryptedResources(cr) {
		keyName := kmsKeyLedgerName(r)
		if keyName == "" {
			continue
		}
		key, ok := status.FindManagedResource(ledger, resourceTypeKMS, keyName)
		if !ok || key.ARN == "" {
			continue
		}
		names := []string{r.name}
		if r.resourceType == resourceTypeSQS {
			names = append(names, cloudctlaws.DerivedKey(r.name, "dlq"))
		}
		for _, name := range names {
			if entry, found := status.FindManagedResource(ledger, r.resourceType, name); found && entry.KMSKeyARN != key.ARN {
				entry.KMSKeyARN = key.ARN
				status.UpsertManagedResource(&ledger, entry)
			}
		}
	}
	cr.Status.ManagedResources = ledger
}

// kmsKeysInUse returns every key of this CR that data still depends on:
// the keys the spec needs, plus the key of every resource still in the
// ledger, whether it is declared, draining towards deletion, retained, or
// an RDS final snapshot. A key whose dependents include a retained resource
// is marked Retained, since it has to outlive the CR along with that data.
//
// The key is found through the KMSKeyARN recorded on the entry. Entries
// recorded before that field existed fall back to the dedicated key derived
// from the entry's own type and name, which covers encryption.enabled.
func kmsKeysInUse(cr *depsv1alpha1.AppDependencies, includeSpec bool) map[string]kms.KeyUse {
	inUse := map[string]kms.KeyUse{}
	if includeSpec {
		for _, name := range neededKMSKeyNames(cr) {
			inUse[name] = kms.KeyUse{}
		}
	}

	for name, holders := range decisions.KeyHolders(cr.Status.ManagedResources) {
		for _, h := range holders {
			use := inUse[name]
			use.Retained = use.Retained || h.Retained
			inUse[name] = use
		}
	}
	return inUse
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

			recordKMSKeyARNs(cr)
			ledger, _, cleanupErr := kms.Cleanup(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, kmsKeysInUse(cr, true), cr.Status.ManagedResources, false, eventRecorderFor(r, cr),
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
						Reason: reasons.KMSKeyRefNotDeclared,
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

			// KMS finalizes last, so whatever other resources are still in the
			// ledger here are either draining or retained. Their keys wait for
			// the first and stay with the second.
			ledger, results, err := kms.Cleanup(
				ctx, awsClients.KMS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.KMS, kmsKeysInUse(cr, false), cr.Status.ManagedResources, true, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, nil, err
			}
			var blocked []string
			for _, res := range results {
				if res.Reason == kms.CleanupReasonPendingDeletion || res.Reason == kms.CleanupReasonInUse {
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

// refuseLegacyKMSKeyRef reports whether the CR still has the cross-CR form of
// encryption.kmsKeyRef that v0.3.0 removed, recording why on its status if so.
// It must run before any write to the CR: the finalizer Update would store
// the spec back without the removed field and decode that response over this
// object, leaving a CR that looks like it never asked for encryption.
func refuseLegacyKMSKeyRef(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	where := legacyKMSKeyRefPath(cr)
	if where == "" {
		return false, nil
	}
	setSectionCondition(ctx, cr, conditionTypeKMSReady, &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("%s uses the cross-CR form of encryption.kmsKeyRef that v0.3.0 removed; set it to the name of a key under this CR's kms.resources, "+
			"or use encryption.enabled for a dedicated key. Nothing is reconciled until it is fixed", where),
		Reason: reasons.LegacyKMSKeyRef,
	}, eventRecorderFor(r, cr))
	// A spec edit bumps the generation and triggers the next reconcile.
	return true, r.Status().Patch(ctx, cr, statusPatch(original, cr))
}

// legacyKMSKeyRefPath names the first resource whose encryption still has the
// pre-v0.3.0 kmsKeyRef object, or "" if there is none.
func legacyKMSKeyRefPath(cr *depsv1alpha1.AppDependencies) string {
	for _, r := range encryptedResources(cr) {
		if r.encryption != nil && r.encryption.LegacyKMSKeyRef {
			return fmt.Sprintf("%s resource %q", r.resourceType, r.name)
		}
	}
	return ""
}
