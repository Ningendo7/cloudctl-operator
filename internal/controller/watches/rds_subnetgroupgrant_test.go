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

package watches

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func TestSubnetGroupGrantToAffectedCRs_MapsMatchingCRsOnly(t *testing.T) {
	grant := &depsv1alpha1.RDSSubnetGroupGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-grant"},
		Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "prod-private-data-tier", AllowedNamespaces: []string{"checkout"}},
	}
	matching := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "checkout", Name: "orders-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db", DBSubnetGroupName: "prod-private-data-tier"},
		}}},
	}
	differentSubnetGroup := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "checkout", Name: "other-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "other-db", DBSubnetGroupName: "staging-tier"},
		}}},
	}
	noRDS := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "checkout", Name: "no-rds-service"}}

	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant, matching, differentSubnetGroup, noRDS).Build()

	requests := SubnetGroupGrantToAffectedCRs(k8sClient)(context.Background(), grant)
	if !containsRequest(requests, "checkout", "orders-service") {
		t.Errorf("expected a reconcile request for the matching CR, got %v", requestedNames(requests))
	}
	if len(requests) != 1 {
		t.Errorf("expected exactly 1 request, got %d: %v", len(requests), requestedNames(requests))
	}
}

func TestSubnetGroupGrantToAffectedCRs_MultipleResourcesSameCR_MapsOnce(t *testing.T) {
	grant := &depsv1alpha1.RDSSubnetGroupGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-grant"},
		Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "prod-private-data-tier"},
	}
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "checkout", Name: "multi-db-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db", DBSubnetGroupName: "prod-private-data-tier"},
			{Name: "invoices-db", DBSubnetGroupName: "prod-private-data-tier"},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant, cr).Build()

	requests := SubnetGroupGrantToAffectedCRs(k8sClient)(context.Background(), grant)
	count := 0
	for _, r := range requests {
		if r.Namespace == "checkout" && r.Name == "multi-db-service" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one request for a CR with two matching resources, got %d (%v)", count, requestedNames(requests))
	}
}

func TestSubnetGroupGrantToAffectedCRs_WrongObjectType_ReturnsNil(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(cr).Build()

	// The watch is registered against RDSSubnetGroupGrant, so this should
	// never actually happen in practice - exercised anyway, since silently
	// mis-handling an unexpected type (e.g. panicking on the type assertion)
	// would be a far worse failure mode than returning no requests.
	requests := SubnetGroupGrantToAffectedCRs(k8sClient)(context.Background(), cr)
	if requests != nil {
		t.Errorf("expected nil for a non-grant object, got %v", requests)
	}
}

func TestSubnetGroupGrantToAffectedCRs_NoMatchingCRs(t *testing.T) {
	grant := &depsv1alpha1.RDSSubnetGroupGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-grant"},
		Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "prod-private-data-tier"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()

	requests := SubnetGroupGrantToAffectedCRs(k8sClient)(context.Background(), grant)
	if len(requests) != 0 {
		t.Errorf("expected no requests, got %v", requestedNames(requests))
	}
}
