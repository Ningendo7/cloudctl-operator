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
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func consumerWithDanglingRef() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service", Generation: 1},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "nonexistent-producer", ResourceName: "orders"},
			}},
		},
	}
}

func TestCheckConsumeReferences_ProducerExists_ReportsTrue(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "nonexistent-producer"},
	}
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).WithObjects(producer).Build()

	consumer := consumerWithDanglingRef()
	checkConsumeReferences(context.Background(), c, consumer)

	cond := apimeta.FindStatusCondition(consumer.Status.Conditions, conditionTypeConsumeReferences)
	if cond == nil {
		t.Fatal("expected a ConsumeReferencesValid condition to be set")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("expected ConditionTrue, got %v (%s: %s)", cond.Status, cond.Reason, cond.Message)
	}
}

func TestCheckConsumeReferences_MissingProducer_ReportsFalseWithoutImplyingMisconfiguration(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).Build()

	consumer := consumerWithDanglingRef()
	checkConsumeReferences(context.Background(), c, consumer)

	cond := apimeta.FindStatusCondition(consumer.Status.Conditions, conditionTypeConsumeReferences)
	if cond == nil {
		t.Fatal("expected a ConsumeReferencesValid condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected ConditionFalse, got %v", cond.Status)
	}
	if cond.Reason != reasonProducerNotFoundYet {
		t.Errorf("reason = %q, want ProducerNotFoundYet on first sighting", cond.Reason)
	}
	if strings.Contains(cond.Message, "misconfiguration") {
		t.Errorf("message = %q, should not yet claim misconfiguration on first sighting", cond.Message)
	}
}

func TestCheckConsumeReferences_StillMissingPastThreshold_EscalatesReason(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).Build()

	consumer := consumerWithDanglingRef()
	// Simulate the condition already having been False for longer than the
	// escalation threshold, rather than sleeping in the test.
	consumer.Status.Conditions = []metav1.Condition{{
		Type:               conditionTypeConsumeReferences,
		Status:             metav1.ConditionFalse,
		Reason:             reasonProducerNotFoundYet,
		Message:            "stale",
		LastTransitionTime: metav1.NewTime(time.Now().Add(-consumeReferenceDanglingThreshold - time.Minute)),
		ObservedGeneration: consumer.Generation,
	}}

	checkConsumeReferences(context.Background(), c, consumer)

	cond := apimeta.FindStatusCondition(consumer.Status.Conditions, conditionTypeConsumeReferences)
	if cond == nil {
		t.Fatal("expected a ConsumeReferencesValid condition to be set")
	}
	if cond.Reason != reasonProducerLikelyMisconfigured {
		t.Errorf("reason = %q, want ProducerLikelyMisconfigured once past the dangling threshold", cond.Reason)
	}
	if !strings.Contains(cond.Message, "misconfiguration") {
		t.Errorf("message = %q, want it to flag likely misconfiguration once past the threshold", cond.Message)
	}
}
