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

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// newStuckDeletionCR builds a CR mid-deletion, owning one sqs resource
// whose ledger entry is Delete-policy, force:false, and no longer declared
// in spec (removed, the common real-world path into this state) - the
// minimal fixture for F9: a non-empty, force:false resource with nothing
// left in spec to ever flip force back to true.
func newStuckDeletionCR() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "checkout-service",
			UID:               "uid-1",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSectionsReady", Message: "all declared sections are ready", ObservedGeneration: 1},
			},
			ManagedResources: []depsv1alpha1.ManagedResource{
				{
					Type:           "sqs",
					Name:           "orders",
					ARN:            "arn:aws:sqs:us-east-1:123456789012:orders",
					State:          depsv1alpha1.ManagedResourceStateVerified,
					DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
					Force:          false,
				},
			},
		},
	}
}

// seedOwnedNonEmptyQueue makes f.queues["orders"] exist, tagged as owned by
// cr, and non-empty - the real-AWS-side half of the stuck-deletion fixture.
func seedOwnedNonEmptyQueue(f *fakeSQSClient, cr *depsv1alpha1.AppDependencies) {
	f.queues["orders"] = &fakeQueue{
		url: "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
		arn: "arn:aws:sqs:us-east-1:123456789012:orders",
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(cr.Namespace, cr.Name),
			cloudctlaws.OwnerUIDTagKey: string(cr.UID),
		},
		approxMessages:        "5",
		approxMessagesHidden:  "0",
		approxMessagesDelayed: "0",
	}
}

func TestReconcileDelete_BlockedByNonEmptyGuard_SetsDeletionBlockedAndClearsReady(t *testing.T) {
	cr := newStuckDeletionCR()
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	seedOwnedNonEmptyQueue(r.AWSClients.SQS.(*fakeSQSClient), cr)

	result, err := r.reconcileDelete(context.Background(), cr)
	if err != nil {
		t.Fatalf("reconcileDelete() error = %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a RequeueAfter while deletion is blocked, got zero")
	}
	if !hasFinalizer(cr) {
		t.Error("expected the finalizer to remain in place while a resource is still blocked")
	}

	blocked := apimeta.FindStatusCondition(cr.Status.Conditions, "DeletionBlocked")
	if blocked == nil {
		t.Fatal("expected a DeletionBlocked condition, got none - this is the regression F9 guards against: " +
			"a stuck deletion must be visible in status, not silent")
	}
	if blocked.Status != metav1.ConditionTrue {
		t.Errorf("DeletionBlocked status = %v, want True", blocked.Status)
	}
	if !strings.Contains(blocked.Message, "orders") {
		t.Errorf("expected DeletionBlocked message to name the blocking resource, got %q", blocked.Message)
	}

	ready := apimeta.FindStatusCondition(cr.Status.Conditions, "Ready")
	if ready == nil {
		t.Fatal("expected a Ready condition")
	}
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready status = %v, want False - a CR stuck mid-deletion must not keep reporting "+
			"whatever Ready said before deletion started", ready.Status)
	}
	if ready.Reason != "DeletionBlocked" {
		t.Errorf("Ready reason = %q, want DeletionBlocked", ready.Reason)
	}
}

func TestReconcileDelete_ForceDeleteAllAnnotation_UnsticksBlockedDeletion(t *testing.T) {
	cr := newStuckDeletionCR()
	cr.Annotations = map[string]string{ForceDeleteAllAnnotation: "true"}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	fakeSQS := r.AWSClients.SQS.(*fakeSQSClient)
	seedOwnedNonEmptyQueue(fakeSQS, cr)

	result, err := r.reconcileDelete(context.Background(), cr)
	if err != nil {
		t.Fatalf("reconcileDelete() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected deletion to complete (no requeue) with the override set, got RequeueAfter = %v", result.RequeueAfter)
	}
	if hasFinalizer(cr) {
		t.Error("expected the finalizer to be removed once the override forced the blocked resource through")
	}
	if _, stillExists := fakeSQS.queues["orders"]; stillExists {
		t.Error("expected the non-empty queue to actually be deleted under the override, not just marked done internally")
	}

	if blocked := apimeta.FindStatusCondition(cr.Status.Conditions, "DeletionBlocked"); blocked != nil {
		t.Errorf("expected no DeletionBlocked condition once the override let deletion complete, got %+v", blocked)
	}
}

func TestReconcileDelete_WithoutForceDeleteAllAnnotation_QueueSurvives(t *testing.T) {
	// Companion to the override test above: confirms the default (no
	// annotation) behavior really is "leave it alone", not a fluke of the
	// fixture - the same non-empty queue must still exist afterward.
	cr := newStuckDeletionCR()
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	fakeSQS := r.AWSClients.SQS.(*fakeSQSClient)
	seedOwnedNonEmptyQueue(fakeSQS, cr)

	if _, err := r.reconcileDelete(context.Background(), cr); err != nil {
		t.Fatalf("reconcileDelete() error = %v", err)
	}

	if _, stillExists := fakeSQS.queues["orders"]; !stillExists {
		t.Error("expected the non-empty queue to survive without the override annotation")
	}
}
