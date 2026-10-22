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

package decisions

import (
	"fmt"
	"slices"
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func entry(entryType, name string, policy depsv1alpha1.DeletionPolicy) depsv1alpha1.ManagedResource {
	return depsv1alpha1.ManagedResource{Type: entryType, Name: name, DeletionPolicy: policy}
}

func TestOnDelete(t *testing.T) {
	del, retain := depsv1alpha1.DeletionPolicyDelete, depsv1alpha1.DeletionPolicyRetain
	forced := func(e depsv1alpha1.ManagedResource) depsv1alpha1.ManagedResource { e.Force = true; return e }

	tests := []struct {
		name string
		in   depsv1alpha1.ManagedResource
		want depsv1alpha1.OnDeleteOutcome
	}{
		{"a retained queue", entry(typeSQS, "q", retain), depsv1alpha1.OnDeleteRetain},
		{"a queue to delete", entry(typeSQS, "q", del), depsv1alpha1.OnDeleteDelete},
		{"a queue to delete, forced", forced(entry(typeSQS, "q", del)), depsv1alpha1.OnDeleteEvenIfNotEmpty},
		{"a bucket to delete, forced", forced(entry(typeS3, "b", del)), depsv1alpha1.OnDeleteEvenIfNotEmpty},
		{"a retained table, forced", forced(entry(typeDynamoDB, "t", retain)), depsv1alpha1.OnDeleteRetain},
		{"a key to delete, force has no meaning", forced(entry("kms", "k", del)), depsv1alpha1.OnDeleteDelete},
		{"a retained key", entry("kms", "k", retain), depsv1alpha1.OnDeleteRetain},
		{"an instance to delete is never deleted", entry("rds", "db", del), depsv1alpha1.OnDeleteManual},
		{"a retained instance", entry("rds", "db", retain), depsv1alpha1.OnDeleteRetain},
		{"a final snapshot is kept", entry("rds-snapshot", "db", retain), depsv1alpha1.OnDeleteRetain},
		{"a role is deleted", entry("iam", "role", del), depsv1alpha1.OnDeleteDelete},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := OnDelete(tc.in); got != tc.want {
				t.Errorf("OnDelete() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestKeyHolders(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/shared"
	del, retain := depsv1alpha1.DeletionPolicyDelete, depsv1alpha1.DeletionPolicyRetain
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "kms", Name: "shared", ARN: arn},
		{Type: "kms", Name: cloudctlaws.DedicatedKeyLedgerName(typeS3, "receipts")},
		{Type: "kms", Name: cloudctlaws.DedicatedKeyLedgerName("rds", "db")},
		{Type: typeSQS, Name: "orders", DeletionPolicy: del, KMSKeyARN: arn},
		{Type: typeS3, Name: "receipts", DeletionPolicy: retain},
		{Type: "rds-snapshot", Name: "db", DeletionPolicy: retain},
		{Type: typeSNS, Name: "events", DeletionPolicy: del},
		{Type: "iam", Name: "role", DeletionPolicy: del, KMSKeyARN: arn},
	}

	got := KeyHolders(ledger)

	if h := got["shared"]; len(h) != 1 || h[0].Name != "orders" || h[0].Retained {
		t.Errorf("shared key holders = %+v, want the queue, not retained", h)
	}
	if h := got[cloudctlaws.DedicatedKeyLedgerName(typeS3, "receipts")]; len(h) != 1 || !h[0].Retained {
		t.Errorf("a dedicated key is found from its owner's type and name, and a retained owner marks it retained, got %+v", h)
	}
	if h := got[cloudctlaws.DedicatedKeyLedgerName("rds", "db")]; len(h) != 1 || h[0].Type != "rds-snapshot" {
		t.Errorf("a final snapshot holds its instance's key, got %+v", h)
	}
	if len(got) != 3 {
		t.Errorf("a resource with no key and an IAM role hold nothing, got %+v", got)
	}
}

func TestStampSetsDecisionsAndIsIdempotent(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/shared"
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "kms", Name: "shared", ARN: arn, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
		{Type: typeSQS, Name: "b", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, KMSKeyARN: arn},
		{Type: typeSQS, Name: "a", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, KMSKeyARN: arn},
	}

	Stamp(ledger)
	first := slices.Clone(ledger)
	Stamp(ledger)

	if ledger[1].OnDelete != depsv1alpha1.OnDeleteRetain || ledger[2].OnDelete != depsv1alpha1.OnDeleteDelete {
		t.Errorf("onDelete not stamped: %+v", ledger)
	}
	if !slices.Equal(ledger[0].HeldBy, []string{"sqs/a", "sqs/b"}) {
		t.Errorf("heldBy = %v, want the holders in order", ledger[0].HeldBy)
	}
	if ledger[1].HeldBy != nil {
		t.Errorf("only key entries carry heldBy, got %v", ledger[1].HeldBy)
	}
	for i := range ledger {
		if ledger[i].OnDelete != first[i].OnDelete || !slices.Equal(ledger[i].HeldBy, first[i].HeldBy) {
			t.Errorf("a second stamp changed entry %d", i)
		}
	}
}

func TestStampListsAtMostTwentyHolders(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/shared"
	ledger := make([]depsv1alpha1.ManagedResource, 0, 26)
	ledger = append(ledger, depsv1alpha1.ManagedResource{Type: "kms", Name: "shared", ARN: arn})
	for i := range 25 {
		ledger = append(ledger, depsv1alpha1.ManagedResource{Type: typeSQS, Name: fmt.Sprintf("q%02d", i), KMSKeyARN: arn})
	}

	Stamp(ledger)

	held := ledger[0].HeldBy
	if len(held) != 21 || held[20] != "and 5 more" {
		t.Errorf("heldBy = %v, want 20 holders then \"and 5 more\"", held)
	}
}

func TestStampClearsHeldByWhenNothingHoldsTheKeyAnyMore(t *testing.T) {
	ledger := []depsv1alpha1.ManagedResource{{Type: "kms", Name: "shared", ARN: "arn:k", HeldBy: []string{"sqs/gone"}}}

	Stamp(ledger)

	if ledger[0].HeldBy != nil {
		t.Errorf("heldBy = %v, want it cleared", ledger[0].HeldBy)
	}
}
