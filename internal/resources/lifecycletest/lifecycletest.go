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

// Package lifecycletest is a shared test harness, not production code: it
// runs one common set of ownership-lifecycle scenarios (create, adopt,
// foreign-resource refusal, retain/relinquish, delete-safety,
// pending-deletion cancellation) against every resource package's own real
// Ensure/Cleanup, through a small per-package adapter implementing Subject.
// It exists to guarantee the five resource packages' test *coverage* stays
// equivalent for the lifecycle they all share - it never replaces or
// duplicates each package's own resource-specific tests (S3's atomic
// tagging, SQS's RedrivePolicy clearing, DynamoDB's async CREATING/ACTIVE
// split, and so on), and it must never be imported by production code.
package lifecycletest

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// ErrUnsupported is returned by SetNonEmpty for a resource type with no
// "still in use" concept at all (KMS: deletion is gated by AWS's own
// ScheduleKeyDeletion wait, not an emptiness check).
var ErrUnsupported = errors.New("lifecycletest: not supported by this resource type")

// preTestQuietWindow safely clears every resource type's 10-minute
// deletion quiet window while staying well under the shared 7-day
// PendingDeletionGracePeriod, so the same value works for every scenario
// below regardless of which resource type is under test.
const preTestQuietWindow = 15 * time.Minute

// OwnerIdentity is the (namespace, crName, crUID) triple every resource
// type's ownership tag is keyed on.
type OwnerIdentity struct {
	Namespace, CRName, CRUID string
}

// EnsureOpts is the minimal spec surface every resource type's Ensure
// needs to vary across these scenarios - nothing resource-specific.
type EnsureOpts struct {
	DeletionPolicy depsv1alpha1.DeletionPolicy
	Adopt          bool
}

// CleanupReason mirrors each resource package's own CleanupReason enum,
// normalized to one shared type the scenarios below can compare against.
type CleanupReason string

const (
	CleanupReasonRetained             CleanupReason = "Retained"
	CleanupReasonPendingDeletion      CleanupReason = "PendingDeletion"
	CleanupReasonStuckPendingDeletion CleanupReason = "StuckPendingDeletion"
)

// CleanupResult mirrors each resource package's own CleanupResult struct.
type CleanupResult struct {
	Name   string
	Reason CleanupReason
}

// Subject adapts one resource package's real Ensure/Cleanup to a shape
// every scenario below can drive identically, regardless of that
// package's actual signatures, spec types, or extra dependencies
// (region/accountID, a KMS client for dedicated-key encryption, ...).
type Subject interface {
	// ResourceType is the ledger's Type discriminator for this resource
	// package (e.g. "sqs"), so scenarios can look up entries generically
	// via status.FindManagedResource.
	ResourceType() string

	// EnsureOne declares exactly one resource named name with opts and
	// reconciles it once, against a fixed owner identity the adapter was
	// built with.
	EnsureOne(ctx context.Context, ledger []depsv1alpha1.ManagedResource, name string, opts EnsureOpts) ([]depsv1alpha1.ManagedResource, error)

	// Cleanup runs this resource type's declared-vs-ledger diff. declared
	// lists which previously-EnsureOne'd names are still in spec.
	Cleanup(ctx context.Context, ledger []depsv1alpha1.ManagedResource, declared []string, deleting bool) ([]depsv1alpha1.ManagedResource, []CleanupResult, error)

	// SeedForeign creates name directly against the fake AWS backend,
	// bypassing Ensure entirely. A nil owner leaves it untagged (genuinely
	// foreign); a non-nil owner tags it for that identity instead of this
	// subject's own.
	SeedForeign(name string, owner *OwnerIdentity) error

	// SetNonEmpty flips this resource type's own "still in use" signal
	// (message count, subscriptions, bucket objects, table rows), or
	// returns ErrUnsupported if this resource type has no such concept.
	SetNonEmpty(name string, nonEmpty bool) error
}

func findResult(results []CleanupResult, name string) *CleanupResult {
	for i := range results {
		if results[i].Name == name {
			return &results[i]
		}
	}
	return nil
}

func markPastQuietWindow(ledger []depsv1alpha1.ManagedResource, resourceType, name string) []depsv1alpha1.ManagedResource {
	entry := status.FindManagedResource(ledger, resourceType, name)
	if entry == nil {
		return ledger
	}
	past := metav1.NewTime(time.Now().Add(-preTestQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)
	return ledger
}

// Run drives the shared lifecycle scenario list against subject, one
// t.Run per scenario. Each scenario uses its own resource name so they
// never interfere with each other even though they share one subject.
func Run(t *testing.T, newSubject func(t *testing.T) Subject) {
	t.Helper()

	t.Run("Create", func(t *testing.T) {
		s := newSubject(t)
		ledger, err := s.EnsureOne(context.Background(), nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		entry := status.FindManagedResource(ledger, s.ResourceType(), "widget")
		if entry == nil {
			t.Fatal("expected a ledger entry after EnsureOne")
		}
		if entry.ARN == "" {
			t.Error("expected a non-empty ARN")
		}
	})

	t.Run("AdoptsForeignResourceWhenAdoptTrue", func(t *testing.T) {
		s := newSubject(t)
		if err := s.SeedForeign("widget", nil); err != nil {
			t.Fatalf("SeedForeign() error = %v", err)
		}
		ledger, err := s.EnsureOne(context.Background(), nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true})
		if err != nil {
			t.Fatalf("expected adopt:true to succeed against a foreign resource, got %v", err)
		}
		if entry := status.FindManagedResource(ledger, s.ResourceType(), "widget"); entry == nil {
			t.Error("expected a ledger entry after adopting")
		}
	})

	t.Run("RefusesForeignResourceWithoutAdopt", func(t *testing.T) {
		s := newSubject(t)
		if err := s.SeedForeign("widget", nil); err != nil {
			t.Fatalf("SeedForeign() error = %v", err)
		}
		_, err := s.EnsureOne(context.Background(), nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err == nil {
			t.Error("expected EnsureOne to refuse a foreign resource without adopt:true")
		}
	})

	t.Run("RefusesResourceOwnedByADifferentCR", func(t *testing.T) {
		s := newSubject(t)
		if err := s.SeedForeign("widget", &OwnerIdentity{Namespace: "other-ns", CRName: "someone-else", CRUID: "other-uid"}); err != nil {
			t.Fatalf("SeedForeign() error = %v", err)
		}
		_, err := s.EnsureOne(context.Background(), nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true})
		if err == nil {
			t.Error("expected EnsureOne to refuse adopting a resource already owned by a different CR")
		}
	})

	t.Run("RetainRelinquishesOwnershipOnRemoval", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		_, results, err := s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		if r := findResult(results, "widget"); r == nil || r.Reason != CleanupReasonRetained {
			t.Errorf("expected a Retained result once removed from spec, got %+v", results)
		}
	})

	t.Run("DeleteBlockedWhenNonEmpty", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyDelete})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if err := s.SetNonEmpty("widget", true); errors.Is(err, ErrUnsupported) {
			t.Skip("this resource type has no emptiness concept")
		} else if err != nil {
			t.Fatalf("SetNonEmpty() error = %v", err)
		}

		ledger, _, err = s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("first Cleanup() error = %v", err)
		}
		ledger = markPastQuietWindow(ledger, s.ResourceType(), "widget")

		_, results, err := s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("second Cleanup() error = %v", err)
		}
		if r := findResult(results, "widget"); r == nil || r.Reason != CleanupReasonPendingDeletion {
			t.Errorf("expected a non-empty resource to stay PendingDeletion, got %+v", results)
		}
	})

	t.Run("DeleteRemovesWhenEmpty", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyDelete})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if err := s.SetNonEmpty("widget", false); err != nil && !errors.Is(err, ErrUnsupported) {
			t.Fatalf("SetNonEmpty() error = %v", err)
		}

		ledger, _, err = s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("first Cleanup() error = %v", err)
		}
		ledger = markPastQuietWindow(ledger, s.ResourceType(), "widget")

		updated, _, err := s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("second Cleanup() error = %v", err)
		}
		if entry := status.FindManagedResource(updated, s.ResourceType(), "widget"); entry != nil {
			t.Errorf("expected the ledger entry removed once actually deleted, got %+v", entry)
		}
	})

	t.Run("CancelsPendingDeletionOnReappearance", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyDelete})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if err := s.SetNonEmpty("widget", true); err != nil && !errors.Is(err, ErrUnsupported) {
			t.Fatalf("SetNonEmpty() error = %v", err)
		}

		ledger, _, err = s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("first Cleanup() error = %v", err)
		}
		if entry := status.FindManagedResource(ledger, s.ResourceType(), "widget"); entry == nil || entry.PendingDeletionSince == nil {
			t.Fatal("test setup broken: expected the resource marked pending deletion before reappearance")
		}

		updated, _, err := s.Cleanup(ctx, ledger, []string{"widget"}, false)
		if err != nil {
			t.Fatalf("Cleanup (reappeared) error = %v", err)
		}
		entry := status.FindManagedResource(updated, s.ResourceType(), "widget")
		if entry == nil {
			t.Fatal("expected the ledger entry to survive reappearance")
		}
		if entry.PendingDeletionSince != nil {
			t.Error("expected PendingDeletionSince cleared on reappearance")
		}
	})
}
