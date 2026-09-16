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
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sqs"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// DriftDetectionInterval is how often a healthy CR is re-reconciled even
// without a spec change, to catch out-of-band AWS-side drift (e.g. someone
// deletes a queue via the console) that no Kubernetes watch can see.
const DriftDetectionInterval = 5 * time.Minute

// transientRequeueInterval is how soon a transient AWS error (throttling,
// eventual consistency) gets retried - short, since the SDK's own retryer
// has already been through its internal backoff before an error like this
// ever reached us.
const transientRequeueInterval = 30 * time.Second

// sectionTypes lists every per-section Ready condition type this CR can
// produce, used to compute the aggregate Ready condition. Grows as more
// resource packages (sns, s3, dynamodb, kms, iam, alarms) get wired in
// alongside sqs below.
var sectionTypes = []string{"SQSReady"}

// ensureDesiredState reconciles every declared section against AWS for a
// non-deleting CR, updating status conditions and the ownership ledger as
// it goes. Ensure and Cleanup both run regardless of each other's outcome —
// they cover independent concerns (currently-declared resources vs.
// resources removed from spec) — and the first error encountered is
// returned so the caller can decide on requeue behavior.
func ensureDesiredState(ctx context.Context, awsClients *cloudctlaws.Clients, cr *depsv1alpha1.AppDependencies) error {
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

	sqsErr := ensureErr
	if sqsErr == nil {
		sqsErr = cleanupErr
	}
	setSectionCondition(cr, "SQSReady", sqsErr)

	return sqsErr
}

// finalizeDesiredState runs cleanup for a CR that's being deleted, tearing
// down (or relinquishing, per each resource's deletionPolicy) everything in
// the ledger. done is false if at least one resource is still waiting to
// drain before it can actually be deleted (PendingDeletion /
// StuckPendingDeletion) — the caller must keep the finalizer in place in
// that case, or the resource would be silently abandoned once the CR
// disappears, with nothing left to ever check on it again.
func finalizeDesiredState(ctx context.Context, awsClients *cloudctlaws.Clients, cr *depsv1alpha1.AppDependencies) (done bool, err error) {
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
}

func setSectionCondition(cr *depsv1alpha1.AppDependencies, conditionType string, err error) {
	if err == nil {
		status.SetSectionCondition(
			&cr.Status.Conditions,
			conditionType,
			metav1.ConditionTrue,
			"Reconciled", "reconciled successfully",
			cr.Generation,
			sectionTypes,
		)
		return
	}

	reason := "Error"
	switch {
	case cloudctlaws.IsPermissionDenied(err):
		reason = "PermissionDenied"
	case isRetryable(err):
		reason = "TransientError"
	}

	status.SetSectionCondition(
		&cr.Status.Conditions,
		conditionType,
		metav1.ConditionFalse,
		reason,
		err.Error(),
		cr.Generation,
		sectionTypes,
	)
}

func isRetryable(err error) bool {
	var reconcileErr *cloudctlaws.ReconcileError
	return errors.As(err, &reconcileErr) && reconcileErr.Retryable
}
