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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// ForceReconcileAnnotation lets a human trigger a reconcile without a real
// spec change, e.g. after fixing an external AWS-side condition the
// controller was waiting on.
const ForceReconcileAnnotation = "cloudctl.io/force-reconcile"

// AppDependenciesPredicate reconciles on spec changes (the standard
// generation-changed check) or when specifically the force-reconcile
// annotation's value changes, filtering out everything else — status-only
// updates, unrelated label/annotation churn.
func AppDependenciesPredicate() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldVal := e.ObjectOld.GetAnnotations()[ForceReconcileAnnotation]
				newVal := e.ObjectNew.GetAnnotations()[ForceReconcileAnnotation]
				return oldVal != newVal
			},
		},
	)
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
