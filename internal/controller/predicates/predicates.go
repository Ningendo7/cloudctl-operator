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

package predicates

import (
	"maps"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
)

// ForceReconcileAnnotation lets a human trigger a reconcile without a real
// spec change, e.g. after fixing an external AWS-side condition the
// controller was waiting on.
const ForceReconcileAnnotation = "cloudctl.io/force-reconcile"

// ForceDeleteAllAnnotation lets a human override a Delete-policy resource's
// non-empty guard to unstick a CR's deletion. Setting it wakes the
// reconciler: the CR is already in deletion, so no spec change will.
const ForceDeleteAllAnnotation = "cloudctl.io/force-delete-all"

// AbandonOtherRegionAnnotation lets a human release a CR in deletion whose
// resources are in another region than this operator's, which otherwise can
// never be deleted from here. Setting it wakes the reconciler for the same
// reason ForceDeleteAllAnnotation does.
const AbandonOtherRegionAnnotation = "cloudctl.io/abandon-other-region"

// AppDependenciesPredicate reconciles on spec changes (the standard
// generation-changed check) or when specifically the force-reconcile
// annotation's value changes, or the force-delete-all annotation is set,
// filtering out everything else — status-only updates, unrelated
// label/annotation churn.
func AppDependenciesPredicate() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldAnn, newAnn := e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations()
				if oldAnn[ForceReconcileAnnotation] != newAnn[ForceReconcileAnnotation] {
					return true
				}
				// Taking either of these off changes nothing, so only setting
				// or changing them reconciles.
				return setOrChanged(oldAnn, newAnn, cloudctlaws.AdoptFromUIDAnnotation) ||
					setOrChanged(oldAnn, newAnn, ForceDeleteAllAnnotation) ||
					setOrChanged(oldAnn, newAnn, AbandonOtherRegionAnnotation)
			},
		},
	)
}

func setOrChanged(oldAnn, newAnn map[string]string, key string) bool {
	v := newAnn[key]
	return v != "" && v != oldAnn[key]
}

// ProducerChangedPredicate is AppDependenciesPredicate plus updates where a
// resource's ARN appeared, changed or disappeared in status.managedResources.
// Consumers resolve producers through those ARNs, which only change in
// status, so a spec-only filter would leave them stale until their own
// next requeue. Other ledger churn (timestamps, state) is ignored.
func ProducerChangedPredicate() predicate.Predicate {
	return predicate.Or(
		AppDependenciesPredicate(),
		predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return false },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldCR, ok := e.ObjectOld.(*depsv1alpha1.AppDependencies)
				if !ok {
					return false
				}
				newCR, ok := e.ObjectNew.(*depsv1alpha1.AppDependencies)
				if !ok {
					return false
				}
				return !maps.Equal(ledgerARNs(oldCR), ledgerARNs(newCR))
			},
		},
	)
}

func ledgerARNs(cr *depsv1alpha1.AppDependencies) map[string]string {
	arns := make(map[string]string, len(cr.Status.ManagedResources))
	for _, r := range cr.Status.ManagedResources {
		arns[r.Type+"/"+r.Name] = r.ARN
	}
	return arns
}

// SpecOrDeletionChangedPredicate passes creates, deletes, spec changes and a
// CR starting to be deleted, and nothing else. For watches that read a CR's
// spec to decide what to wake, and must also hear that it is going away.
func SpecOrDeletionChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
				e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
	}
}

// AnnotationValueChangedPredicate reconciles on Create/Delete/Generic
// events, or an Update where the given annotation's value actually
// changed — filtering out updates to any other field. Meant for watches on
// objects this operator only partially owns (so GenerationChangedPredicate
// doesn't apply, and the whole object isn't ours to react to every change
// on) but where exactly one annotation is actually ours to care about.
func AnnotationValueChangedPredicate(key string) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetAnnotations()[key] != e.ObjectNew.GetAnnotations()[key]
		},
	}
}

// PodNetworkIdentityPublishedPredicate reconciles only when a CR's own
// published pod-network-identity ARN actually changes, when it starts
// being deleted, or when its rds.consumes list changes - lets the RDS
// producer-side ingress watch react immediately to a consumer publishing
// (or losing) its identity, or no longer being entitled to the database,
// without firing on every unrelated status write. Delete always passes
// through. Without the last two, the consumer's own security group cannot
// be deleted until the producer happens to revoke its rule.
func PodNetworkIdentityPublishedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldCR, ok := e.ObjectOld.(*depsv1alpha1.AppDependencies)
			if !ok {
				return false
			}
			newCR, ok := e.ObjectNew.(*depsv1alpha1.AppDependencies)
			if !ok {
				return false
			}
			oldARN, _ := rds.PodNetworkIdentityARN(oldCR)
			newARN, _ := rds.PodNetworkIdentityARN(newCR)
			return oldARN != newARN ||
				oldCR.DeletionTimestamp.IsZero() != newCR.DeletionTimestamp.IsZero() ||
				!slices.Equal(rdsConsumes(oldCR), rdsConsumes(newCR))
		},
	}
}

func rdsConsumes(cr *depsv1alpha1.AppDependencies) []depsv1alpha1.ConsumeRef {
	if cr.Spec.RDS == nil {
		return nil
	}
	return cr.Spec.RDS.Consumes
}
