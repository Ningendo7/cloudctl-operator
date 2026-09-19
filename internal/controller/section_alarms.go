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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/alarms"
)

// alarmEligibleResourceCount sums how many SQS/SNS/DynamoDB resources this
// CR declares — used only to scale alarmsSection's timeout budget, the same
// way every other section scales on its own declared count. Alarms carry
// no ledger entries of their own, so unlike every other section this is
// the only signal of how much work a reconcile could actually do; there's
// no ledger count to fall back on during finalize either, but Cleanup's
// unconditional delete-everything-under-our-prefix shape doesn't scale
// with declared resources anyway, so that's an acceptable gap.
func alarmEligibleResourceCount(cr *depsv1alpha1.AppDependencies) int {
	count := 0
	if cr.Spec.SQS != nil {
		count += len(cr.Spec.SQS.Resources)
	}
	if cr.Spec.SNS != nil {
		count += len(cr.Spec.SNS.Resources)
	}
	if cr.Spec.DynamoDB != nil {
		count += len(cr.Spec.DynamoDB.Resources)
	}
	return count
}

// alarmsSection adapts the alarms package's Ensure/Cleanup to the
// orchestrator's uniform section shape. Takes the whole reconciler (not
// just AWSClients) because alarms.snsTopicRef resolution needs r.Client to
// look up the producer CR the shared notification topic belongs to.
//
// Registered after every resource-producing section (SQS/SNS/DynamoDB/S3)
// so alarms always target resources this same reconcile pass already knows
// about, and before iamSection — alarms create no IAM grants of their own,
// so the ordering relative to IAM doesn't matter functionally, but keeping
// it grouped with KMS ahead of IAM matches this file's existing "resources
// first, IAM last" convention.
func alarmsSection(r *AppDependenciesReconciler) section {
	return section{
		name: "AlarmsReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, "alarm", alarmEligibleResourceCount(cr))
			defer cancel()

			err := alarms.Ensure(
				ctx, r.AWSClients.CloudWatch, r.Client,
				cr.Namespace, cr.Name, string(cr.UID),
				r.AWSClients.Region, r.AWSClients.AccountID,
				cr.Spec.Alarms, cr.Spec.SQS, cr.Spec.SNS, cr.Spec.DynamoDB,
			)
			setSectionCondition(cr, "AlarmsReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, error) {
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, "alarm", alarmEligibleResourceCount(cr))
			defer cancel()

			err := alarms.Cleanup(
				ctx, r.AWSClients.CloudWatch,
				cr.Namespace, cr.Name, string(cr.UID),
				r.AWSClients.Region, r.AWSClients.AccountID,
			)
			return err == nil, err
		},
	}
}
