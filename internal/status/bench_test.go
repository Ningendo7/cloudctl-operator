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
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func benchLedger(n int) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	ledger := make([]depsv1alpha1.ManagedResource, 0, n)
	for i := range n {
		ledger = append(ledger, depsv1alpha1.ManagedResource{
			Type: "sqs", Name: fmt.Sprintf("queue-%d", i),
			ARN:            fmt.Sprintf("arn:aws:sqs:us-east-1:111122223333:queue-%d", i),
			State:          depsv1alpha1.ManagedResourceStateVerified,
			DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
			CreatedAt:      now, LastVerifiedAt: &now,
		})
	}
	return ledger
}

// The last entry is the slowest to find.
func BenchmarkFindManagedResource_LastOfFifty(b *testing.B) {
	ledger := benchLedger(50)
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := FindManagedResource(ledger, "sqs", "queue-49"); !ok {
			b.Fatal("not found")
		}
	}
}

// The check that decides whether a pass writes status at all.
func BenchmarkStatusUnchangedComparison(b *testing.B) {
	now := metav1.Now()
	conds := make([]metav1.Condition, 0, 10)
	for i := range 10 {
		conds = append(conds, metav1.Condition{Type: fmt.Sprintf("Section%dReady", i), Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: now})
	}
	original := depsv1alpha1.AppDependenciesStatus{Conditions: conds, ManagedResources: benchLedger(30)}
	current := *original.DeepCopy()
	b.ReportAllocs()
	for b.Loop() {
		if !equality.Semantic.DeepEqual(original, current) {
			b.Fatal("the statuses should be equal")
		}
	}
}
