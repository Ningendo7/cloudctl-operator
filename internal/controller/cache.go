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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// ManagedObjectCache limits the manager's Secret and ConfigMap cache to
// the objects this operator created, so it doesn't hold every Secret in the
// cluster in memory. An object without the label is invisible to cached
// reads, which is why the code that must notice such an object (a foreign
// one sitting at a name the operator wants) falls back to the API reader.
// Shared by main and the envtest managers so the tests run the production
// configuration.
func ManagedObjectCache() map[client.Object]cache.ByObject {
	keepManagedFields := keepOperatorsManagedFields
	managed := cache.ByObject{
		Label: labels.SelectorFromSet(labels.Set{
			depsv1alpha1.ManagedLabelKey: depsv1alpha1.ManagedLabelValue,
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

// operatorManagerPrefix starts the name of every field manager this operator
// applies under.
const operatorManagerPrefix = "cloudctl-"

// keepOperatorsManagedFields drops every managedFields entry but the
// operator's own, the only ones the skipped applies read. A ServiceAccount is
// written by many tools, so this is where it saves memory.
func keepOperatorsManagedFields(in any) (any, error) {
	obj, err := meta.Accessor(in)
	if err != nil || len(obj.GetManagedFields()) == 0 {
		return in, nil
	}
	var kept []metav1.ManagedFieldsEntry
	for _, entry := range obj.GetManagedFields() {
		if strings.HasPrefix(entry.Manager, operatorManagerPrefix) {
			kept = append(kept, entry)
		}
	}
	obj.SetManagedFields(kept)
	return in, nil
}
