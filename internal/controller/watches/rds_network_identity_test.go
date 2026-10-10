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

func TestConsumerPodIdentityToRDSProducers_MapsProducerThatSharesWithConsumer(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
			Name: "orders-db", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}},
		}}}},
	}
	unrelatedProducer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "billing-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
			Name: "invoices-db", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "analytics", Name: "analytics-service"}},
		}}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer, producer, unrelatedProducer).Build()

	requests := ConsumerPodIdentityToRDSProducers(k8sClient)(context.Background(), consumer)

	if !containsRequest(requests, "default", "checkout-service") {
		t.Errorf("expected a reconcile request for the sharing producer, got %v", requestedNames(requests))
	}
	if containsRequest(requests, "default", "billing-service") {
		t.Errorf("expected no request for an unrelated producer, got %v", requestedNames(requests))
	}
	if len(requests) != 1 {
		t.Errorf("expected exactly 1 request, got %d: %v", len(requests), requestedNames(requests))
	}
}

func TestConsumerPodIdentityToRDSProducers_MapsOnlyOncePerProducerWithMultipleResources(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}},
			{Name: "invoices-db", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "fulfillment", Name: "fulfillment-service"}}},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer, producer).Build()

	requests := ConsumerPodIdentityToRDSProducers(k8sClient)(context.Background(), consumer)

	count := 0
	for _, r := range requests {
		if r.Namespace == "default" && r.Name == "checkout-service" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one request for a producer sharing two resources with the same consumer, got %d (%v)", count, requestedNames(requests))
	}
}

func TestConsumerPodIdentityToRDSProducers_NeverMapsConsumerToItself(t *testing.T) {
	// A CR can own an RDS instance and share it with itself in a degenerate
	// spec - the map function must not turn that into a self-reconcile
	// loop via this watch specifically (the main watch already handles
	// a CR's own spec changes).
	self := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
			Name: "orders-db", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "default", Name: "checkout-service"}},
		}}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(self).Build()

	requests := ConsumerPodIdentityToRDSProducers(k8sClient)(context.Background(), self)
	if len(requests) != 0 {
		t.Errorf("expected no self-referential request, got %v", requestedNames(requests))
	}
}

func TestConsumerPodIdentityToRDSProducers_NoSharingProducers(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer).Build()

	requests := ConsumerPodIdentityToRDSProducers(k8sClient)(context.Background(), consumer)
	if len(requests) != 0 {
		t.Errorf("expected no requests, got %v", requestedNames(requests))
	}
}

func TestConsumerPodIdentityToRDSProducers_IgnoresProducerWithNoRDSSpec(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	noRDS := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "no-rds-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(consumer, noRDS).Build()

	requests := ConsumerPodIdentityToRDSProducers(k8sClient)(context.Background(), consumer)
	if len(requests) != 0 {
		t.Errorf("expected no requests, got %v", requestedNames(requests))
	}
}

func TestConsumerPodIdentityToRDSProducers_ListFailure_ReturnsNilNotPanic(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service"}}
	k8sClient := fake.NewClientBuilder().Build() // unregistered scheme makes List fail

	requests := ConsumerPodIdentityToRDSProducers(k8sClient)(context.Background(), consumer)
	if requests != nil {
		t.Errorf("expected nil requests on a List failure, got %v", requests)
	}
}
