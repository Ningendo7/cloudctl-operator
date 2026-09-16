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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func TestSetSectionCondition_AggregatesReady(t *testing.T) {
	var conditions []metav1.Condition
	sections := []string{"SQSReady", "S3Ready"}

	SetSectionCondition(&conditions, "SQSReady", metav1.ConditionTrue, "Reconciled", "ok", 1, sections)
	if ready := findCondition(conditions, ConditionTypeReady); ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready=False while S3Ready is missing, got %+v", ready)
	}

	SetSectionCondition(&conditions, "S3Ready", metav1.ConditionTrue, "Reconciled", "ok", 1, sections)
	if ready := findCondition(conditions, ConditionTypeReady); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True once all sections are ready, got %+v", ready)
	}

	SetSectionCondition(&conditions, "S3Ready", metav1.ConditionFalse, "Error", "bucket create failed", 2, sections)
	if ready := findCondition(conditions, ConditionTypeReady); ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready=False after S3Ready regresses, got %+v", ready)
	}
}
