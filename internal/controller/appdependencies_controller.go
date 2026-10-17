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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/controller/predicates"
	"github.com/Ningendo7/cloudctl-operator/internal/controller/watches"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/serviceaccount"
)

// AppDependenciesReconciler reconciles a AppDependencies object
type AppDependenciesReconciler struct {
	client.Client
	APIReader  client.Reader
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

	// PodBaseSecurityGroupIDs are the security groups the pods of a CR using
	// rds.consumes keep next to the database group.
	PodBaseSecurityGroupIDs []string

	// MaxConcurrentReconciles bounds how many AppDependencies CRs this
	// manager reconciles in parallel. Left at zero (the Go zero value, not
	// a deliberate "disable" setting) falls back to defaultMaxConcurrentReconciles
	MaxConcurrentReconciles int

	// transientBackoff counts each CR's consecutive transient failures; see
	// transientDelay. Created on first use, so a zero-valued reconciler works.
	transientBackoff     workqueue.TypedRateLimiter[reconcile.Request]
	transientBackoffOnce sync.Once
}

const defaultMaxConcurrentReconciles = 5

// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=appdependencies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=appdependencies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=appdependencies/finalizers,verbs=update
// +kubebuilder:rbac:groups=deps.cloudctl.io,resources=rdssubnetgroupgrants,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vpcresources.k8s.aws,resources=securitygrouppolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *AppDependenciesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	ctx, endSpan := startReconcileSpan(ctx, req)
	defer func() { endSpan(err) }()

	var cr depsv1alpha1.AppDependencies
	if err := r.Get(ctx, req.NamespacedName, &cr); err != nil {
		if apierror.IsNotFound(err) {
			r.resetTransientBackoff(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// A CR told which previous UID to take resources over from (a restore,
	// a new cluster) carries it to every section through the context.
	ctx = cloudctlaws.WithAdoptFromUID(ctx, cr.Annotations[cloudctlaws.AdoptFromUIDAnnotation])

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

	if refused, err := refuseLegacyKMSKeyRef(ctx, r, cr, original); refused || err != nil {
		return ctrl.Result{}, err
	}

	finalizerCtx, endFinalizer := startSpan(ctx, "ensure finalizer")
	finalizerErr := ensureFinalizer(finalizerCtx, r.Client, cr)
	endFinalizer(finalizerErr)
	if err := finalizerErr; err != nil {
		log.Error(err, "Reconcile failed", "duration", time.Since(start))
		return ctrl.Result{}, err
	}
	// Adding a finalizer only touches metadata, not spec, so it won't bump
	// generation and re-trigger our predicate on its own - keep going in
	// this same pass (rather than returning) or a freshly created CR would
	// never actually get reconciled until some later spec change.

	err := ensureDesiredState(ctx, r, cr, original)

	patchCtx, endPatch := startSpan(ctx, "patch status")
	patchCtx, cancelPatch := statusWriteContext(patchCtx)
	statusErr := r.Status().Patch(patchCtx, cr, statusPatch(original, cr))
	cancelPatch()
	endPatch(statusErr)
	if statusErr != nil {
		log.Error(statusErr, "failed to update status")
		if err == nil {
			err = statusErr
		}
	}

	key := client.ObjectKeyFromObject(cr)
	if err != nil {
		if isRetryable(err) {
			delay := r.transientDelay(key)
			log.Info("Reconcile hit a transient error, requeuing", "error", err.Error(), "requeueAfter", delay, "failures", r.transientBackoffLimiter().NumRequeues(reconcile.Request{NamespacedName: key}), "duration", time.Since(start))
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		r.resetTransientBackoff(key)
		log.Error(err, "Reconcile failed", "duration", time.Since(start))
		return ctrl.Result{}, err
	}
	r.resetTransientBackoff(key)

	requeueAfter := jitteredDriftRequeue()
	log.Info("Reconcile succeeded", "requeueAfter", requeueAfter, "duration", time.Since(start))
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
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

	patchCtx, cancelPatch := statusWriteContext(ctx)
	defer cancelPatch()
	if statusErr := r.Status().Patch(patchCtx, cr, client.MergeFrom(original)); statusErr != nil {
		log.Error(statusErr, "failed to update status during deletion")
	}

	key := client.ObjectKeyFromObject(cr)
	if err != nil {
		if isRetryable(err) {
			delay := r.transientDelay(key)
			log.Info("Deletion reconcile hit a transient error, requeuing", "error", err.Error(), "requeueAfter", delay, "duration", time.Since(start))
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		r.resetTransientBackoff(key)
		log.Error(err, "Deletion reconcile failed", "duration", time.Since(start))
		return ctrl.Result{}, err
	}
	r.resetTransientBackoff(key)

	if !done {
		requeueAfter := jitteredDriftRequeue()
		log.Info("Deletion reconcile still waiting on resources to drain", "requeueAfter", requeueAfter, "duration", time.Since(start))
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
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
	if err := watches.RegisterIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&depsv1alpha1.AppDependencies{}, builder.WithPredicates(predicates.AppDependenciesPredicate())).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Owns(
			&corev1.ServiceAccount{},
			builder.WithPredicates(predicates.AnnotationValueChangedPredicate(serviceaccount.RoleARNAnnotation)),
		).
		Watches(
			&depsv1alpha1.AppDependencies{},
			handler.EnqueueRequestsFromMapFunc(watches.ProducerToConsumers(r.Client)),
			builder.WithPredicates(predicates.ProducerChangedPredicate()),
		).
		Watches(
			&depsv1alpha1.RDSSubnetGroupGrant{},
			handler.EnqueueRequestsFromMapFunc(watches.SubnetGroupGrantToAffectedCRs(r.Client)),
		).
		Watches(
			&depsv1alpha1.AppDependencies{},
			handler.EnqueueRequestsFromMapFunc(watches.ConsumerPodIdentityToRDSProducers(r.Client)),
			builder.WithPredicates(predicates.PodNetworkIdentityPublishedPredicate()),
		).
		WithOptions(controllerOptions(maxConcurrent)).
		Named("appdependencies").
		Complete(r)
}

// ManagedObjectCache limits the manager's Secret and ConfigMap cache to
// the objects this operator created, so it doesn't hold every Secret in the
// cluster in memory. An object without the label is invisible to cached
// reads, which is why the code that must notice such an object (a foreign
// one sitting at a name the operator wants) falls back to the API reader.
// Shared by main and the envtest managers so the tests run the production
// configuration.
func ManagedObjectCache() map[client.Object]cache.ByObject {
	keepManagedFields := func(in any) (any, error) { return in, nil }
	managed := cache.ByObject{
		Label: labels.SelectorFromSet(labels.Set{
			depsv1alpha1.ManagedByLabelKey: depsv1alpha1.ManagedByLabelValue,
		}),
		Transform: keepManagedFields,
	}
	return map[client.Object]cache.ByObject{
		&corev1.Secret{}:         managed,
		&corev1.ConfigMap{}:      managed,
		&corev1.ServiceAccount{}: {Transform: keepManagedFields},
	}
}

// ManagedCacheDefaultTransform drops managedFields, which are large, from
// cached objects. ManagedObjectCache keeps them on the three types whose
// unchanged applies are skipped.
func ManagedCacheDefaultTransform() toolscache.TransformFunc {
	return cache.TransformStripManagedFields()
}
