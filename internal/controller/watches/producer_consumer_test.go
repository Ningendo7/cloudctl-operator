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
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func TestProducerToConsumers_MapsEveryConsumingResourceType(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"}}
	sqsConsumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "sqs-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
		}}},
	}
	snsConsumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "sns-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{SNS: &depsv1alpha1.SNSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
		}}},
	}
	dynamodbConsumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "dynamodb-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{DynamoDB: &depsv1alpha1.DynamoDBSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
		}}},
	}
	s3Consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "s3-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{S3: &depsv1alpha1.S3Spec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
		}}},
	}
	rdsConsumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "rds-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		}}},
	}
	unrelated := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "unrelated"}}

	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(producer, sqsConsumer, snsConsumer, dynamodbConsumer, s3Consumer, rdsConsumer, unrelated).Build()

	requests := ProducerToConsumers(k8sClient)(context.Background(), producer)

	for _, want := range []struct{ namespace, name string }{
		{"fulfillment", "sqs-consumer"},
		{"fulfillment", "sns-consumer"},
		{"fulfillment", "dynamodb-consumer"},
		{"fulfillment", "s3-consumer"},
		{"fulfillment", "rds-consumer"},
	} {
		if !containsRequest(requests, want.namespace, want.name) {
			t.Errorf("expected a reconcile request for %s/%s, got %v", want.namespace, want.name, requestedNames(requests))
		}
	}
	if containsRequest(requests, "fulfillment", "unrelated") {
		t.Errorf("expected no reconcile request for an unrelated CR, got %v", requestedNames(requests))
	}
	if len(requests) != 5 {
		t.Errorf("expected exactly 5 requests, got %d: %v", len(requests), requestedNames(requests))
	}
}

func TestProducerToConsumers_NeverMapsProducerToItself(t *testing.T) {
	// A CR consuming from itself would be a self-referential loop if ever
	// mapped - not something any valid spec should express, but the map
	// function itself must not be the thing that lets it reconcile forever.
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()

	requests := ProducerToConsumers(k8sClient)(context.Background(), producer)
	if len(requests) != 0 {
		t.Errorf("expected no self-referential request, got %v", requestedNames(requests))
	}
}

func TestProducerToConsumers_IgnoresConsumerInDifferentNamespaceWithSameName(t *testing.T) {
	// A ConsumeRef names both namespace and name - a consumer referencing a
	// different producer that merely shares this one's name must not match.
	producer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"}}
	decoy := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "decoy-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "staging", Name: "checkout-service", ResourceName: "orders"},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer, decoy).Build()

	requests := ProducerToConsumers(k8sClient)(context.Background(), producer)
	if len(requests) != 0 {
		t.Errorf("expected no request for a same-name producer in a different namespace, got %v", requestedNames(requests))
	}
}

func TestProducerToConsumers_NoConsumersReturnsNil(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"}}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()

	requests := ProducerToConsumers(k8sClient)(context.Background(), producer)
	if len(requests) != 0 {
		t.Errorf("expected no requests, got %v", requestedNames(requests))
	}
}

func TestProducerToConsumers_ListFailure_ReturnsNilNotPanic(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"}}
	// An empty, unregistered scheme makes List fail deterministically -
	// exercising the "can't even ask" path without needing a dedicated
	// error-injecting fake client just for this package.
	k8sClient := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()

	requests := ProducerToConsumers(k8sClient)(context.Background(), producer)
	if requests != nil {
		t.Errorf("expected nil requests on a List failure, got %v", requests)
	}
}
