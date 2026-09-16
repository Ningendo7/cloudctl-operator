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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sqs"
)

// sqsSection adapts the sqs package's Ensure/Cleanup to the orchestrator's
// uniform section shape.
func sqsSection(awsClients *cloudctlaws.Clients) section {
	return section{
		name: "SQSReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			ledger, ensureErr := sqs.Ensure(
				ctx,
				awsClients.SQS,
				cr.Namespace,
				cr.Name,
				string(cr.UID),
				cr.Spec.SQS,
				cr.Status.ManagedResources,
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := sqs.Cleanup(
				ctx,
				awsClients.SQS,
				cr.Namespace,
				cr.Name,
				string(cr.UID),
				cr.Spec.SQS,
				cr.Status.ManagedResources,
				false,
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(cr, "SQSReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, error) {
			ledger, results, err := sqs.Cleanup(
				ctx,
				awsClients.SQS,
				cr.Namespace,
				cr.Name,
				string(cr.UID),
				cr.Spec.SQS,
				cr.Status.ManagedResources,
				true,
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, err
			}

			for _, r := range results {
				if r.Reason == sqs.CleanupReasonPendingDeletion || r.Reason == sqs.CleanupReasonStuckPendingDeletion {
					return false, nil
				}
			}
			return true, nil
		},
	}
}
