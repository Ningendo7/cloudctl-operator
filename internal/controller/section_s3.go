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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/s3"
)

// s3Section adapts the s3 package's Ensure/Cleanup to the orchestrator's
// uniform section shape. Takes the whole reconciler (not just AWSClients,
// unlike before) because encryption.kmsKeyRef resolution needs r.Client to
// look up the producer CR a shared key belongs to.
func s3Section(r *AppDependenciesReconciler, original *depsv1alpha1.AppDependencies) section {
	return section{
		name: conditionTypeS3Ready,
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			declared := 0
			if cr.Spec.S3 != nil {
				declared = len(cr.Spec.S3.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, resourceTypeS3, declared)
			defer cancel()

			ledger, ensureErr := s3.Ensure(
				ctx, r.AWSClients.S3, r.AWSClients.KMS, r.Client, cr.Namespace, cr.Name, string(cr.UID),
				r.AWSClients.Region, r.AWSClients.AccountID,
				cr.Spec.S3, cr.Status.ManagedResources, checkpointFor(r, cr, original), eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := s3.Cleanup(
				ctx, r.AWSClients.S3, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.S3, cr.Status.ManagedResources, false, false, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(ctx, cr, conditionTypeS3Ready, err, eventRecorderFor(r, cr))
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, []string, error) {
			declared := 0
			if cr.Spec.S3 != nil {
				declared = len(cr.Spec.S3.Resources)
			}
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, resourceTypeS3, declared)
			defer cancel()

			forceDeleteAll := cr.Annotations[ForceDeleteAllAnnotation] != ""
			ledger, results, err := s3.Cleanup(
				ctx, r.AWSClients.S3, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.S3, cr.Status.ManagedResources, true, forceDeleteAll, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, nil, err
			}
			var blocked []string
			for _, res := range results {
				if res.Reason == s3.CleanupReasonPendingDeletion || res.Reason == s3.CleanupReasonStuckPendingDeletion {
					blocked = append(blocked, fmt.Sprintf("s3 %q: %s", res.Name, res.Reason))
				}
			}
			if len(blocked) > 0 {
				return false, blocked, nil
			}
			return true, nil, nil
		},
	}
}
