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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func TestUpsertAndFindManagedResource(t *testing.T) {
	var ledger []depsv1alpha1.ManagedResource

	UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: "arn:1"})
	if got := FindManagedResource(ledger, "sqs", "orders"); got == nil || got.ARN != "arn:1" {
		t.Fatalf("expected to find inserted entry, got %+v", got)
	}

	UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: "arn:2"})
	if len(ledger) != 1 {
		t.Fatalf("expected upsert to update in place, not append; got %d entries", len(ledger))
	}
	if got := FindManagedResource(ledger, "sqs", "orders"); got.ARN != "arn:2" {
		t.Fatalf("expected updated ARN, got %+v", got)
	}
}

func TestRemoveManagedResource(t *testing.T) {
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "orders"},
		{Type: "sqs", Name: "receipts"},
	}

	RemoveManagedResource(&ledger, "sqs", "orders")
	if len(ledger) != 1 || ledger[0].Name != "receipts" {
		t.Fatalf("expected only receipts to remain, got %+v", ledger)
	}

	// Removing something absent is a no-op, not an error.
	RemoveManagedResource(&ledger, "sqs", "does-not-exist")
	if len(ledger) != 1 {
		t.Fatalf("expected no change removing an absent entry, got %+v", ledger)
	}
}

func TestNeedsRevalidation(t *testing.T) {
	now := time.Now()
	stale := metav1.NewTime(now.Add(-2 * TrustWindow))
	fresh := metav1.NewTime(now.Add(-1 * time.Minute))

	cases := []struct {
		name  string
		entry depsv1alpha1.ManagedResource
		want  bool
	}{
		{"never verified", depsv1alpha1.ManagedResource{State: depsv1alpha1.ManagedResourceStateVerified}, true},
		{"tag pending is always revalidated", depsv1alpha1.ManagedResource{State: depsv1alpha1.ManagedResourceStateTagPending, LastVerifiedAt: &fresh}, true},
		{"verified and fresh", depsv1alpha1.ManagedResource{State: depsv1alpha1.ManagedResourceStateVerified, LastVerifiedAt: &fresh}, false},
		{"verified but stale", depsv1alpha1.ManagedResource{State: depsv1alpha1.ManagedResourceStateVerified, LastVerifiedAt: &stale}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NeedsRevalidation(tc.entry); got != tc.want {
				t.Errorf("NeedsRevalidation() = %v, want %v", got, tc.want)
			}
		})
	}
}
