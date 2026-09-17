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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/serviceaccount"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const resourceTypeIAM = "iam"

// iamWorkloadCount approximates how much work deriving this CR's IAM
// policy actually involves, for sectionContext's timeout scaling. Unlike
// every other section, IAM never manages more than one ledger entry (the
// role itself) regardless of how many resources drive its policy, so
// ledger count alone (sectionContext's other input) would never reflect a
// CR with many owned/consumed resources needing a proportionally larger
// timeout. Counts owned resources (each contributes a policy statement)
// plus consumes entries (each requires a cross-namespace CR lookup to
// check sharedWith) across every implemented resource type.
func iamWorkloadCount(cr *depsv1alpha1.AppDependencies) int {
	count := 0
	if cr.Spec.SQS != nil {
		count += len(cr.Spec.SQS.Resources) + len(cr.Spec.SQS.Consumes)
	}
	if cr.Spec.SNS != nil {
		count += len(cr.Spec.SNS.Resources) + len(cr.Spec.SNS.Consumes)
	}
	if cr.Spec.DynamoDB != nil {
		count += len(cr.Spec.DynamoDB.Resources) + len(cr.Spec.DynamoDB.Consumes)
	}
	if cr.Spec.S3 != nil {
		count += len(cr.Spec.S3.Resources) + len(cr.Spec.S3.Consumes)
	}
	return count
}

// iamSection derives and reconciles this CR's IAM role, and the
// ServiceAccount annotation that lets a workload actually assume it via
// IRSA. Needs the whole reconciler (not just AWSClients like every other
// section) since policy derivation reads across every section's
// spec/ledger and needs the k8s client for cross-CR consumes lookups and
// for the ServiceAccount itself.
func iamSection(r *AppDependenciesReconciler) section {
	return section{
		name: "IAMReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, resourceTypeIAM, iamWorkloadCount(cr))
			defer cancel()

			ledger, roleARN, ensureErr := iam.Ensure(
				ctx, r.AWSClients.IAM, r.Client,
				r.OIDCProviderARN, r.OIDCProviderURL,
				cr, cr.Status.ManagedResources,
			)
			cr.Status.ManagedResources = ledger
			if roleARN != "" {
				cr.Status.IAMRoleARN = roleARN
			}

			// Runs every reconcile, not just on CR deletion: a CR that had
			// resources needing IAM and then had all of them removed from
			// spec must have its now-unneeded role cleaned up too, the
			// same "declared vs ledger diff" cleanup every other section
			// already does - not just at CR deletion time.
			ledger, cleanupErr := iam.Cleanup(ctx, r.AWSClients.IAM, r.Client, cr, cr.Status.ManagedResources, false)
			cr.Status.ManagedResources = ledger
			if status.FindManagedResource(ledger, resourceTypeIAM, "role") == nil {
				cr.Status.IAMRoleARN = ""
			}

			saErr := syncServiceAccount(ctx, r, cr)

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			if err == nil {
				err = saErr
			}
			setSectionCondition(cr, "IAMReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, error) {
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, resourceTypeIAM, iamWorkloadCount(cr))
			defer cancel()

			ledger, err := iam.Cleanup(ctx, r.AWSClients.IAM, r.Client, cr, cr.Status.ManagedResources, true)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, err
			}
			cr.Status.IAMRoleARN = ""
			if err := serviceaccount.Cleanup(ctx, r.Client, cr); err != nil {
				return false, err
			}
			cr.Status.ServiceAccountName = ""
			return true, nil
		},
	}
}

// syncServiceAccount keeps the ServiceAccount annotation in lockstep with
// the role: attach it once a role exists, strip it once one no longer does
// — the k8s-side half of IRSA, mirroring the ledger diff iamSection's
// reconcile already does for the AWS-side role.
func syncServiceAccount(ctx context.Context, r *AppDependenciesReconciler, cr *depsv1alpha1.AppDependencies) error {
	if cr.Status.IAMRoleARN != "" {
		saName, err := serviceaccount.Ensure(ctx, r.Client, cr, cr.Status.IAMRoleARN)
		if err != nil {
			return err
		}
		cr.Status.ServiceAccountName = saName
		return nil
	}
	if cr.Status.ServiceAccountName != "" {
		if err := serviceaccount.Cleanup(ctx, r.Client, cr); err != nil {
			return err
		}
		cr.Status.ServiceAccountName = ""
	}
	return nil
}
