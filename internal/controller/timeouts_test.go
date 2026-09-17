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
	"time"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func timeoutOf(ctx context.Context, t *testing.T) time.Duration {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a deadline on the returned context")
	}
	return time.Until(deadline)
}

func TestSectionContext_UsesBaseTimeoutWithNothingDeclaredOrInLedger(t *testing.T) {
	ctx, cancel := sectionContext(context.Background(), nil, "sqs", 0)
	defer cancel()

	got := timeoutOf(ctx, t)
	if got > baseSectionTimeout || got < baseSectionTimeout-time.Second {
		t.Errorf("expected roughly baseSectionTimeout (%v), got %v", baseSectionTimeout, got)
	}
}

func TestSectionContext_ScalesWithDeclaredCount(t *testing.T) {
	ctx, cancel := sectionContext(context.Background(), nil, "sqs", 10)
	defer cancel()

	want := baseSectionTimeout + 10*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v for 10 declared resources, got %v", want, got)
	}
}

func TestSectionContext_UsesLedgerCountWhenLargerThanDeclared(t *testing.T) {
	// Mirrors the finalize path: nothing is "declared" during deletion, so
	// the ledger count - not the (irrelevant) declared count - must drive
	// the timeout, or a CR with many resources still in the ledger would
	// get a timeout sized as if it had none.
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "a"},
		{Type: "sqs", Name: "b"},
		{Type: "sqs", Name: "c"},
	}
	ctx, cancel := sectionContext(context.Background(), ledger, "sqs", 0)
	defer cancel()

	want := baseSectionTimeout + 3*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v driven by ledger count, got %v", want, got)
	}
}

func TestSectionContext_OnlyCountsLedgerEntriesOfTheGivenResourceType(t *testing.T) {
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "a"},
		{Type: "sns", Name: "b"},
		{Type: "sns", Name: "c"},
		{Type: "dynamodb", Name: "d"},
	}
	ctx, cancel := sectionContext(context.Background(), ledger, "sns", 0)
	defer cancel()

	want := baseSectionTimeout + 2*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v counting only sns entries, got %v", want, got)
	}
}

func TestSectionContext_UsesDeclaredCountWhenLargerThanLedger(t *testing.T) {
	// The normal reconcile path: a CR that just added several new
	// resources has more declared than are in the ledger yet.
	ledger := []depsv1alpha1.ManagedResource{{Type: "dynamodb", Name: "a"}}
	ctx, cancel := sectionContext(context.Background(), ledger, "dynamodb", 5)
	defer cancel()

	want := baseSectionTimeout + 5*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v driven by declared count, got %v", want, got)
	}
}

func TestSectionDeletionContext_UsesTheLargerDeletionBudget(t *testing.T) {
	// Deletion does strictly more work per resource than creation (S3's
	// per-object-version cleanup being the extreme case) - its budget must
	// never be reused from, or smaller than, provisioning's.
	if baseSectionDeletionTimeout <= baseSectionTimeout {
		t.Errorf("baseSectionDeletionTimeout (%v) must be greater than baseSectionTimeout (%v)", baseSectionDeletionTimeout, baseSectionTimeout)
	}
	if perResourceDeletionTimeout <= perResourceTimeout {
		t.Errorf("perResourceDeletionTimeout (%v) must be greater than perResourceTimeout (%v)", perResourceDeletionTimeout, perResourceTimeout)
	}

	ledger := []depsv1alpha1.ManagedResource{
		{Type: "s3", Name: "a"},
		{Type: "s3", Name: "b"},
	}
	ctx, cancel := sectionDeletionContext(context.Background(), ledger, "s3", 0)
	defer cancel()

	want := baseSectionDeletionTimeout + 2*perResourceDeletionTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v driven by ledger count under the deletion budget, got %v", want, got)
	}

	provisioningWant := baseSectionTimeout + 2*perResourceTimeout
	if got <= provisioningWant {
		t.Errorf("expected the deletion timeout (%v) to exceed the equivalent provisioning timeout (%v)", got, provisioningWant)
	}
}
