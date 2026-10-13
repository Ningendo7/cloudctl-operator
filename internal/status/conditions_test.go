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

// TestSetSectionCondition_StaleSectionFromOlderGenerationDoesNotSatisfyReady
// covers a checkpointing hazard: ensureDesiredState checkpoints status
// after every section within one reconcile pass, so mid-pass, sections
// later in the list still carry their condition from the CR's *previous*
// generation. recomputeReady must check each condition's own
// observedGeneration, not just Status == True - otherwise a stale-but-True
// DynamoDBReady (e.g. a vacuous "nothing declared" success from the
// generation before dynamodb was ever added to spec) would satisfy the
// aggregate check just as well as a fresh one, flipping Ready True after
// only 2 of 7 sections had actually been reprocessed.
func TestSetSectionCondition_StaleSectionFromOlderGenerationDoesNotSatisfyReady(t *testing.T) {
	var conditions []metav1.Condition
	sections := []string{"SQSReady", "DynamoDBReady"}

	// Generation 1: only SQS declared. DynamoDBReady is a vacuous success
	// (section always runs, nothing to do when spec.DynamoDB is nil).
	SetSectionCondition(&conditions, "SQSReady", metav1.ConditionTrue, "Reconciled", "ok", 1, sections)
	SetSectionCondition(&conditions, "DynamoDBReady", metav1.ConditionTrue, "Reconciled", "ok", 1, sections)
	if ready := findCondition(conditions, ConditionTypeReady); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True at generation 1, got %+v", ready)
	}

	// Generation 2: dynamodb added to spec. SQSReady gets reprocessed first
	// (still True, now at generation 2) - DynamoDBReady hasn't run yet this
	// pass and is still sitting at its generation-1 value.
	SetSectionCondition(&conditions, "SQSReady", metav1.ConditionTrue, "Reconciled", "ok", 2, sections)
	if ready := findCondition(conditions, ConditionTypeReady); ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready=False: DynamoDBReady's stale generation-1 True must not satisfy generation 2, got %+v", ready)
	}
	if dr := findCondition(conditions, "DynamoDBReady"); dr.ObservedGeneration != 1 {
		t.Fatalf("expected DynamoDBReady to still show its stale observedGeneration=1, got %d", dr.ObservedGeneration)
	}

	// Now DynamoDBReady actually gets reprocessed for generation 2 - only
	// now should Ready legitimately go True.
	SetSectionCondition(&conditions, "DynamoDBReady", metav1.ConditionTrue, "Reconciled", "ok", 2, sections)
	ready := findCondition(conditions, ConditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True once DynamoDBReady is actually reprocessed for generation 2, got %+v", ready)
	}
	if ready.ObservedGeneration != 2 {
		t.Fatalf("expected Ready's observedGeneration=2, got %d", ready.ObservedGeneration)
	}
}
