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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/configmap"
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
// produce, used to compute the aggregate Ready condition. Kept in sync
// with allSections above — each entry here should have a matching
// section constructor registered there.
var sectionTypes = []string{"SQSReady", "SNSReady", "DynamoDBReady", "S3Ready", "KMSReady", "IAMReady", "ConnectionInfoReady"}

type section struct {
	name      string
	reconcile func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error
	finalize  func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (done bool, err error)
}

// allSections lists every resource-type section this CR reconciles, in
// dependency order — iamSection must run last, since deriving this CR's
// IAM policy reads the ARNs the other sections just wrote into this same
// reconcile pass's ledger. Adding a new resource type means adding one file
// (section_<type>.go) with its own constructor, and one line here — this
// file's size doesn't grow with the number of resource types.
func allSections(r *AppDependenciesReconciler) []section {
	return []section{
		sqsSection(r),
		snsSection(r),
		dynamodbSection(r),
		s3Section(r),
		kmsSection(r.AWSClients),
		iamSection(r),
	}
}

func ensureDesiredState(ctx context.Context, r *AppDependenciesReconciler, cr *depsv1alpha1.AppDependencies) error {
	var firstErr error
	for _, s := range allSections(r) {
		if err := s.reconcile(ctx, cr); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	checkSharedWithReferences(ctx, r.Client, cr)

	// Runs last, after every section has written this pass's ARNs into the
	// ledger: the ConfigMap it generates is only as fresh as that ledger
	// data, so anything reconciled earlier in this same pass is already
	// reflected in it, not lagging a full reconcile behind.
	connErr := configmap.Ensure(ctx, r.Client, r.AWSClients.Region, r.AWSClients.AccountID, cr)
	setSectionCondition(cr, "ConnectionInfoReady", connErr)
	if firstErr == nil {
		firstErr = connErr
	}
	return firstErr
}

// finalizeDesiredState runs cleanup for a CR that's being deleted, tearing
// down (or relinquishing, per each resource's deletionPolicy) everything in
// the ledger. done is false if at least one resource is still waiting to
// drain before it can actually be deleted (PendingDeletion /
// StuckPendingDeletion) — the caller must keep the finalizer in place in
// that case, or the resource would be silently abandoned once the CR
// disappears, with nothing left to ever check on it again.
func finalizeDesiredState(ctx context.Context, r *AppDependenciesReconciler, cr *depsv1alpha1.AppDependencies) (done bool, err error) {
	allDone := true
	var firstErr error
	for _, s := range allSections(r) {
		done, err := s.finalize(ctx, cr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			allDone = false
			continue
		}
		if !done {
			allDone = false
		}
	}
	return allDone, firstErr
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
