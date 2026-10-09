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

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
)

// rdsSection adapts the rds package's Ensure/EnsurePodNetworkIdentity/
// Cleanup to the orchestrator's uniform section shape. Takes the whole
// reconciler, like dynamodbSection, for the same reason: encryption's
// kmsKeyRef resolution and the subnet-group-grant/sharedWith checks all
// need r.Client for cross-CR lookups.
func rdsSection(r *AppDependenciesReconciler, original *depsv1alpha1.AppDependencies) section {
	return section{
		name: "RDSReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			declared := 0
			if cr.Spec.RDS != nil {
				declared = len(cr.Spec.RDS.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, resourceTypeRDS, declared)
			defer cancel()

			ledger, ensureErr := rds.Ensure(
				ctx, r.AWSClients.RDS, r.AWSClients.KMS, r.AWSClients.EC2, r.Client,
				cr.Namespace, cr.Name, string(cr.UID), r.AWSClients.Region, r.AWSClients.AccountID,
				cr.Spec.RDS, cr.Status.ManagedResources, checkpointFor(r, cr, original), eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			// Only ever relevant for a CR that consumes an RDS instance -
			// one that only owns instances of its own has no pods needing
			// network identity for this purpose.
			var networkErr error
			if cr.Spec.RDS != nil && len(cr.Spec.RDS.Consumes) > 0 {
				ledger, networkErr = rds.EnsurePodNetworkIdentity(
					ctx, r.AWSClients.RDS, r.AWSClients.EC2, r.Client,
					cr.Namespace, cr.Name, string(cr.UID), rdsNetworkServiceAccountName(cr),
					r.AWSClients.Region, r.AWSClients.AccountID, cr.Spec.RDS.Consumes, cr.Status.ManagedResources,
				)
				cr.Status.ManagedResources = ledger
			}

			ledger, _, cleanupErr := rds.Cleanup(
				ctx, r.AWSClients.RDS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.RDS, cr.Status.ManagedResources, false, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = networkErr
			}
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(ctx, cr, "RDSReady", err, eventRecorderFor(r, cr))
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, []string, error) {
			declared := 0
			if cr.Spec.RDS != nil {
				declared = len(cr.Spec.RDS.Resources)
			}
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, resourceTypeRDS, declared)
			defer cancel()

			ledger, results, err := rds.Cleanup(
				ctx, r.AWSClients.RDS, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.RDS, cr.Status.ManagedResources, true, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, nil, err
			}
			var blocked []string
			for _, res := range results {
				if res.Reason == rds.CleanupReasonPendingDeletion || res.Reason == rds.CleanupReasonStuckPendingDeletion {
					blocked = append(blocked, fmt.Sprintf("rds %q: %s", res.Name, res.Reason))
				}
			}
			if len(blocked) > 0 {
				return false, blocked, nil
			}
			return true, nil, nil
		},
	}
}

// rdsNetworkServiceAccountName resolves which ServiceAccount identifies
// this CR's own pods for RDS network-identity purposes: an explicit
// networkServiceAccountName override, falling back to the same
// serviceAccountName (and its own cr.Name default) IRSA already uses -
// see NetworkServiceAccountName's own field comment for why these two
// concerns default to the same ServiceAccount but can diverge.
func rdsNetworkServiceAccountName(cr *depsv1alpha1.AppDependencies) string {
	if cr.Spec.NetworkServiceAccountName != "" {
		return cr.Spec.NetworkServiceAccountName
	}
	if cr.Spec.ServiceAccountName != "" {
		return cr.Spec.ServiceAccountName
	}
	return cr.Name
}
