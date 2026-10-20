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

package v1alpha1

// ManagedLabelKey/Value mark the ConfigMaps and Secrets this operator
// generates; the cache only watches objects with it. It is the operator's own
// key because policies rewrite the standard managed-by label, which is set
// alongside for people and never relied on.
const (
	ManagedLabelKey   = "cloudctl.io/managed"
	ManagedLabelValue = "true"

	ManagedByLabelKey   = "app.kubernetes.io/managed-by"
	ManagedByLabelValue = "cloudctl-operator"
)
