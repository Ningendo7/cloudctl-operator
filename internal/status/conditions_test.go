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
	"strings"
	"testing"
	"unicode/utf8"

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

func TestCapMessage(t *testing.T) {
	short := "queue is fine"
	if got := CapMessage(short); got != short {
		t.Errorf("a short message changed: %q", got)
	}
	long := strings.Repeat("é", MaxMessageLength) // two bytes each, so a naive cut could split one
	got := CapMessage(long)
	if len(got) > MaxMessageLength {
		t.Errorf("capped message is %d bytes, want at most %d", len(got), MaxMessageLength)
	}
	if !utf8.ValidString(got) || !strings.HasSuffix(got, truncatedMarker) {
		t.Errorf("capped message must stay valid UTF-8 and say it was cut: %q", got[len(got)-20:])
	}
}

func TestJoinCapped(t *testing.T) {
	if got := JoinCapped([]string{"a", "b", "c"}, "; "); got != "a; b; c" {
		t.Errorf("JoinCapped() = %q, want %q", got, "a; b; c")
	}
	items := make([]string, 1000)
	for i := range items {
		items[i] = fmt.Sprintf("stale reference %04d to team-a/some-consumer", i)
	}
	got := JoinCapped(items, "; ")
	if len(got) > MaxMessageLength {
		t.Fatalf("joined message is %d bytes, want at most %d", len(got), MaxMessageLength)
	}
	var shown, more int
	if _, err := fmt.Sscanf(got[strings.LastIndex(got, "… and ")+len("… and "):], "%d more", &more); err != nil {
		t.Fatalf("expected an \"… and N more\" ending: %q", got[len(got)-40:])
	}
	shown = strings.Count(got, "stale reference")
	if shown+more != len(items) || more == 0 {
		t.Errorf("shown %d + more %d should account for all %d items", shown, more, len(items))
	}
}

// A message over the API server's limit gets the whole status patch
// rejected, ledger checkpoints included.
func TestConditionsNeverExceedTheMessageLimit(t *testing.T) {
	var conditions []metav1.Condition
	SetSectionCondition(&conditions, "SQSReady", metav1.ConditionFalse, "Error", strings.Repeat("x", 100000), 1, []string{"SQSReady"})
	reasons := make([]string, 2000)
	for i := range reasons {
		reasons[i] = fmt.Sprintf("sqs %q: PendingDeletion", fmt.Sprint("queue-", i))
	}
	SetDeletionBlocked(&conditions, 1, reasons)

	for _, c := range conditions {
		if len(c.Message) > MaxMessageLength {
			t.Errorf("condition %s message is %d bytes, want at most %d", c.Type, len(c.Message), MaxMessageLength)
		}
	}
}

func TestStripRequestIDs(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{
			"SQS",
			"operation error SQS: GetQueueUrl, https response error StatusCode: 403, RequestID: 8a5e0c66-1f3e-5c6b-9b0d-2d1f1c3a9e11, api error AccessDenied: not authorized",
			"operation error SQS: GetQueueUrl, https response error StatusCode: 403, api error AccessDenied: not authorized",
		},
		{
			"S3 adds a HostID",
			"operation error S3: HeadBucket, https response error StatusCode: 403, RequestID: 7XYZ123, HostID: abc+/DEF=, api error Forbidden: Forbidden",
			"operation error S3: HeadBucket, https response error StatusCode: 403, api error Forbidden: Forbidden",
		},
		{
			"after retries",
			"exceeded maximum number of attempts, 3, https response error StatusCode: 503, RequestID: 1234, api error ServiceUnavailable: try later",
			"exceeded maximum number of attempts, 3, https response error StatusCode: 503, api error ServiceUnavailable: try later",
		},
		{"at the end", "operation error EC2: DescribeSecurityGroups, StatusCode: 500, RequestID: abc-123", "operation error EC2: DescribeSecurityGroups, StatusCode: 500"},
		{"first", "RequestID: abc, then more", "then more"},
		{"nothing to strip", "queue \"orders\" is already owned by a different CR", "queue \"orders\" is already owned by a different CR"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := StripRequestIDs(tc.in); got != tc.want {
				t.Errorf("StripRequestIDs() = %q, want %q", got, tc.want)
			}
		})
	}
}
