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

package status

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AppliedBy reports whether manager has applied obj before. Only then is
// skipping an unchanged apply safe: a later apply can only drop fields it owns.
func AppliedBy(obj metav1.Object, manager string) bool {
	for _, f := range obj.GetManagedFields() {
		if f.Manager == manager && f.Operation == metav1.ManagedFieldsOperationApply {
			return true
		}
	}
	return false
}
