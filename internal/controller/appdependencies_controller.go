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
	"time"

	apierror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/controller/predicates"
)

// AppDependenciesReconciler reconciles a AppDependencies object
type AppDependenciesReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	AWSClients *cloudctlaws.Clients
	Recorder   events.EventRecorder

	// OIDCProviderARN and OIDCProviderURL identify this cluster's IAM OIDC
	// identity provider, needed to build the IRSA trust policy on every IAM
	// role this operator derives. Left empty is valid for a deployment that
	// never uses sharedWith/consumes — the IAM section only ever needs
	// these when it actually has to create or update a role, and reports a
	// clear error at that point rather than refusing to start without them.
	OIDCProviderARN string
	OIDCProviderURL string

	// MaxConcurrentReconciles bounds how many AppDependencies CRs this
	// manager reconciles in parallel. Left at zero (the Go zero value, not
	// a deliberate "disable" setting) falls back to defaultMaxConcurrentReconciles
	MaxConcurrentReconciles int
}

const defaultMaxConcurrentReconciles = 5

// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=appdependencies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=appdependencies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=appdependencies/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *AppDependenciesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cr depsv1alpha1.AppDependencies
	if err := r.Get(ctx, req.NamespacedName, &cr); err != nil {
		if apierror.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !cr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &cr)
	}
	return r.reconcileNormal(ctx, &cr)
}

func (r *AppDependenciesReconciler) reconcileNormal(ctx context.Context, cr *depsv1alpha1.AppDependencies) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	start := time.Now()
	log.Info("Starting reconcile")
	original := cr.DeepCopy()

	if err := ensureFinalizer(ctx, r.Client, cr); err != nil {
		log.Error(err, "Reconcile failed", "duration", time.Since(start))
		return ctrl.Result{}, err
	}
	// Adding a finalizer only touches metadata, not spec, so it won't bump
	// generation and re-trigger our predicate on its own - keep going in
	// this same pass (rather than returning) or a freshly created CR would
	// never actually get reconciled until some later spec change.

	err := ensureDesiredState(ctx, r, cr, original)

	if statusErr := r.Status().Patch(ctx, cr, client.MergeFrom(original)); statusErr != nil {
		log.Error(statusErr, "failed to update status")
		if err == nil {
			err = statusErr
		}
	}

	if err != nil {
		if isRetryable(err) {
			log.Info("Reconcile hit a transient error, requeuing", "error", err.Error(), "requeueAfter", transientRequeueInterval, "duration", time.Since(start))
			return ctrl.Result{RequeueAfter: transientRequeueInterval}, nil
		}
		log.Error(err, "Reconcile failed", "duration", time.Since(start))
		return ctrl.Result{}, err
	}

	log.Info("Reconcile succeeded", "requeueAfter", DriftDetectionInterval, "duration", time.Since(start))
	return ctrl.Result{RequeueAfter: DriftDetectionInterval}, nil
}

func (r *AppDependenciesReconciler) reconcileDelete(ctx context.Context, cr *depsv1alpha1.AppDependencies) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !hasFinalizer(cr) {
		return ctrl.Result{}, nil
	}
	start := time.Now()
	log.Info("Starting deletion reconcile")
	original := cr.DeepCopy()

	done, err := finalizeDesiredState(ctx, r, cr)

	if statusErr := r.Status().Patch(ctx, cr, client.MergeFrom(original)); statusErr != nil {
		log.Error(statusErr, "failed to update status during deletion")
	}

	if err != nil {
		if isRetryable(err) {
			log.Info("Deletion reconcile hit a transient error, requeuing", "error", err.Error(), "requeueAfter", transientRequeueInterval, "duration", time.Since(start))
			return ctrl.Result{RequeueAfter: transientRequeueInterval}, nil
		}
		log.Error(err, "Deletion reconcile failed", "duration", time.Since(start))
		return ctrl.Result{}, err
	}

	if !done {
		log.Info("Deletion reconcile still waiting on resources to drain", "requeueAfter", DriftDetectionInterval, "duration", time.Since(start))
		return ctrl.Result{RequeueAfter: DriftDetectionInterval}, nil
	}

	if err := removeFinalizer(ctx, r.Client, cr); err != nil {
		log.Error(err, "Failed to remove finalizer", "duration", time.Since(start))
		return ctrl.Result{}, err
	}
	log.Info("Deletion reconcile succeeded, finalizer removed", "duration", time.Since(start))
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AppDependenciesReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentReconciles
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&depsv1alpha1.AppDependencies{}).
		WithEventFilter(predicates.AppDependenciesPredicate()).
		// Self-referential watch: a producer's sharedWith change (or any
		// spec change generation-changed already lets through) also
		// reconciles every CR that consumes from it, so a revocation or
		// new grant takes effect immediately instead of waiting for the
		// periodic drift-detection interval. See mapProducerToConsumers.
		Watches(
			&depsv1alpha1.AppDependencies{},
			handler.EnqueueRequestsFromMapFunc(r.mapProducerToConsumers),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrent}).
		Named("appdependencies").
		Complete(r)
}
