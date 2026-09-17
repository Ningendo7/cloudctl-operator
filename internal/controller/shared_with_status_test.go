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
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func newSharedWithScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

func TestCheckSharedWithReferences_AllResolve_ReportsTrue(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "fulfillment-service"},
	}
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).WithObjects(consumer).Build()

	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", Generation: 1},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "team-b", Name: "fulfillment-service"},
				}},
			}},
		},
	}

	checkSharedWithReferences(context.Background(), c, producer)

	cond := apimeta.FindStatusCondition(producer.Status.Conditions, conditionTypeSharedWithReferences)
	if cond == nil {
		t.Fatal("expected a SharedWithReferencesValid condition to be set")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("expected ConditionTrue, got %v (%s: %s)", cond.Status, cond.Reason, cond.Message)
	}
}

func TestCheckSharedWithReferences_MissingConsumer_ReportsFalseWithDetail(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).Build()

	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", Generation: 1},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "team-b", Name: "fulfillment-service"},
				}},
			}},
		},
	}

	checkSharedWithReferences(context.Background(), c, producer)

	cond := apimeta.FindStatusCondition(producer.Status.Conditions, conditionTypeSharedWithReferences)
	if cond == nil {
		t.Fatal("expected a SharedWithReferencesValid condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected ConditionFalse, got %v", cond.Status)
	}
	if cond.Reason != "StaleReferencesFound" {
		t.Errorf("reason = %q, want StaleReferencesFound", cond.Reason)
	}
	wantSubstr := `sqs "orders" shares with team-b/fulfillment-service, which no longer exists`
	if cond.Message != wantSubstr {
		t.Errorf("message = %q, want %q", cond.Message, wantSubstr)
	}
}

func TestCheckSharedWithReferences_DoesNotAffectAggregateReady(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).Build()

	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", Generation: 1},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "team-b", Name: "gone"},
				}},
			}},
		},
	}
	// Simulate every real section already having reported Ready, the way
	// ensureDesiredState would leave things after a fully successful pass.
	for _, t := range sectionTypes {
		apimeta.SetStatusCondition(&producer.Status.Conditions, metav1.Condition{
			Type: t, Status: metav1.ConditionTrue, Reason: "Reconciled", Message: "ok", ObservedGeneration: 1,
		})
	}

	checkSharedWithReferences(context.Background(), c, producer)

	ready := apimeta.FindStatusCondition(producer.Status.Conditions, "Ready")
	if ready == nil {
		t.Fatal("expected a Ready condition")
	}
	if ready.Status != metav1.ConditionTrue {
		t.Errorf("expected a stale sharedWith reference to leave the aggregate Ready condition True, got %v (%s: %s)", ready.Status, ready.Reason, ready.Message)
	}
}
