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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RDSSubnetGroupGrantSpec authorizes specific namespaces to place an RDS
// instance inside a given DB subnet group. Cluster-scoped and created by a
// platform admin, not an AppDependencies CR - no CR owns a raw AWS subnet
// group the way one owns a queue, so there's no "owner" to carry the
// sharedWith-style opt-in the way every other cross-CR grant in this
// project does. Checked at reconcile time rather than via an admission
// webhook, and surfaced as a SubnetGroupNotAuthorized status condition on
// any CR whose dbSubnetGroupName isn't covered by a matching grant.
type RDSSubnetGroupGrantSpec struct {
	// dbSubnetGroupName is the real AWS DBSubnetGroup this grant covers.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=255
	DBSubnetGroupName string `json:"dbSubnetGroupName"`

	// allowedNamespaces lists every Kubernetes namespace permitted to
	// reference dbSubnetGroupName from an AppDependencies CR's
	// rds.resources[].dbSubnetGroupName field.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=100
	// +listType=set
	AllowedNamespaces []string `json:"allowedNamespaces"`
}

// RDSSubnetGroupGrantStatus is intentionally empty for now - this object
// has no Reconcile() of its own; it's read-only input consumed by the AppDependencies
// controller's RDS section, not something that reports its own state.
type RDSSubnetGroupGrantStatus struct{}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// RDSSubnetGroupGrant is the Schema for the rdssubnetgroupgrants API
type RDSSubnetGroupGrant struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RDSSubnetGroupGrant
	// +required
	Spec RDSSubnetGroupGrantSpec `json:"spec"`

	// status defines the observed state of RDSSubnetGroupGrant
	// +optional
	Status RDSSubnetGroupGrantStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RDSSubnetGroupGrantList contains a list of RDSSubnetGroupGrant
type RDSSubnetGroupGrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RDSSubnetGroupGrant `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RDSSubnetGroupGrant{}, &RDSSubnetGroupGrantList{})
		return nil
	})
}
