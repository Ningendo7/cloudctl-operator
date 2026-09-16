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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sns"
)

// snsSection adapts the sns package's Ensure/Cleanup to the orchestrator's
// uniform section shape.
func snsSection(awsClients *cloudctlaws.Clients) section {
	return section{
		name: "SNSReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			declared := 0
			if cr.Spec.SNS != nil {
				declared = len(cr.Spec.SNS.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, "sns", declared)
			defer cancel()

			ledger, ensureErr := sns.Ensure(
				ctx, 
				awsClients.SNS, 
				cr.Namespace, 
				cr.Name, 
				string(cr.UID), 
				awsClients.Region, 
				awsClients.AccountID, 
				cr.Spec.SNS, 
				cr.Status.ManagedResources,
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := sns.Cleanup(
				ctx, 
				awsClients.SNS, 
				cr.Namespace, 
				cr.Name, 
				string(cr.UID), 
				cr.Spec.SNS, 
				cr.Status.ManagedResources, 
				false,
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(cr, "SNSReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, error) {
			declared := 0
			if cr.Spec.SNS != nil {
				declared = len(cr.Spec.SNS.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, "sns", declared)
			defer cancel()

			ledger, results, err := sns.Cleanup(
				ctx, 
				awsClients.SNS, 
				cr.Namespace, 
				cr.Name, 
				string(cr.UID), 
				cr.Spec.SNS, 
				cr.Status.ManagedResources, 
				true,
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, err
			}

			for _, r := range results {
				if r.Reason == sns.CleanupReasonPendingDeletion || r.Reason == sns.CleanupReasonStuckPendingDeletion {
					return false, nil
				}
			}
			return true, nil
		},
	}
}

