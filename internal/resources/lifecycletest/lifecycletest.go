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
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/decisions"
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

	// DeleteOutOfBand removes the resource from the fake AWS backend, as if
	// someone had deleted it by hand.
	DeleteOutOfBand(name string) error

	// Tags returns the tags on the named resource in the fake AWS backend.
	Tags(name string) map[string]string

	// PutTag sets one tag on the named resource, as someone else's tooling
	// would.
	PutTag(name, key, value string) error

	// KeepsRelinquishedEntries is true for a resource type whose ledger entry
	// stays after the resource is let go even when it pins no key (KMS keys
	// themselves); the others drop it.
	KeepsRelinquishedEntries() bool
}

// pinned stamps the named entry with a key, as the controller does for an
// encrypted resource, so it stays in the ledger after being relinquished.
func pinned(ledger []depsv1alpha1.ManagedResource, resourceType, name string) []depsv1alpha1.ManagedResource {
	entry, found := status.FindManagedResource(ledger, resourceType, name)
	if !found {
		return ledger
	}
	entry.KMSKeyARN = "arn:aws:kms:us-east-1:123456789012:key/pinned-by-the-scenario"
	status.UpsertManagedResource(&ledger, entry)
	return ledger
}

func findResult(results []CleanupResult, name string) *CleanupResult {
	for i := range results {
		if results[i].Name == name {
			return &results[i]
		}
	}
	return nil
}

func markPastQuietWindow(ledger []depsv1alpha1.ManagedResource, resourceType string) []depsv1alpha1.ManagedResource {
	entry, entryFound := status.FindManagedResource(ledger, resourceType, "widget")
	if !entryFound {
		return ledger
	}
	past := metav1.NewTime(time.Now().Add(-preTestQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, entry)
	return ledger
}

// Run drives the shared lifecycle scenario list against subject, one
// t.Run per scenario. Each scenario uses its own resource name so they
// never interfere with each other even though they share one subject.
//
//nolint:gocyclo // one t.Run block per shared scenario; splitting would scatter closely-related assertions across multiple functions for no readability gain
func Run(t *testing.T, newSubject func(t *testing.T) Subject) {
	t.Helper()

	t.Run("Create", func(t *testing.T) {
		s := newSubject(t)
		ledger, err := s.EnsureOne(context.Background(), nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		entry, entryFound := status.FindManagedResource(ledger, s.ResourceType(), "widget")
		if !entryFound {
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
		if !status.HasManagedResource(ledger, s.ResourceType(), "widget") {
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

	t.Run("RetainedEntryThatPinsNoKeyLeavesTheLedgerButStillNeedsAdopt", func(t *testing.T) {
		s := newSubject(t)
		if s.KeepsRelinquishedEntries() {
			t.Skip("this resource type keeps its entry after it is let go")
		}
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "unpinned", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		var results []CleanupResult
		if ledger, results, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		if r := findResult(results, "unpinned"); r == nil || r.Reason != CleanupReasonRetained {
			t.Fatalf("expected a Retained result, got %+v", results)
		}
		if status.HasManagedResource(ledger, s.ResourceType(), "unpinned") {
			t.Fatal("expected an entry that pins no key to leave the ledger once the resource is let go")
		}

		// With no entry the resource is found untagged, which needs adopt, as
		// a relinquished entry did.
		if _, err := s.EnsureOne(ctx, ledger, "unpinned", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}); err == nil {
			t.Fatal("expected re-declaring a retained resource without adopt to be refused")
		}
		ledger, err = s.EnsureOne(ctx, ledger, "unpinned", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true})
		if err != nil {
			t.Fatalf("EnsureOne() with adopt error = %v", err)
		}
		if entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "unpinned"); entry.State != depsv1alpha1.ManagedResourceStateVerified {
			t.Errorf("expected the adopted entry to be Verified, got %s", entry.State)
		}

		t.Run("an entry relinquished by an earlier version goes without a read", func(t *testing.T) {
			ledger, err := s.EnsureOne(ctx, nil, "older", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
			if err != nil {
				t.Fatalf("EnsureOne() error = %v", err)
			}
			entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "older")
			status.UpsertManagedResource(&ledger, status.Relinquished(entry))
			if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
			if status.HasManagedResource(ledger, s.ResourceType(), "older") {
				t.Error("expected the old relinquished entry to be dropped")
			}
		})
	})

	t.Run("RelinquishedEntryIsKeptAndNeedsAdoptToComeBack", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "kept", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		ledger = pinned(ledger, s.ResourceType(), "kept") // as an encrypted resource is
		for range 2 {
			var results []CleanupResult
			if ledger, results, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
			if r := findResult(results, "kept"); r == nil || r.Reason != CleanupReasonRetained {
				t.Fatalf("expected a Retained result, got %+v", results)
			}
		}
		// The entry is what keeps the key its data is encrypted with.
		entry, found := status.FindManagedResource(ledger, s.ResourceType(), "kept")
		if !found || entry.State != depsv1alpha1.ManagedResourceStateRelinquished {
			t.Fatalf("expected the entry kept as Relinquished, got %+v (found=%v)", entry, found)
		}

		// Its last verification is recent, but its tags are gone: declaring
		// it again must not trust it.
		if _, err := s.EnsureOne(ctx, ledger, "kept", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}); err == nil {
			t.Fatal("expected re-declaring a relinquished resource without adopt to be refused")
		}
		ledger, err = s.EnsureOne(ctx, ledger, "kept", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true})
		if err != nil {
			t.Fatalf("EnsureOne() with adopt error = %v", err)
		}
		if entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "kept"); entry.State != depsv1alpha1.ManagedResourceStateVerified {
			t.Errorf("expected the adopted entry to be Verified, got %s", entry.State)
		}
	})

	t.Run("RelinquishedEntryIsDroppedOnceItsResourceIsGone", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "vanishing", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		ledger = pinned(ledger, s.ResourceType(), "vanishing") // as an encrypted resource is
		cleanup := func() {
			t.Helper()
			if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
		}
		backdate := func() {
			t.Helper()
			entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "vanishing")
			old := metav1.NewTime(time.Now().Add(-status.RelinquishedRecheckInterval - time.Hour))
			entry.LastVerifiedAt = &old
			status.UpsertManagedResource(&ledger, entry)
		}
		tracked := func() bool { return status.HasManagedResource(ledger, s.ResourceType(), "vanishing") }

		cleanup() // relinquishes
		if err := s.DeleteOutOfBand("vanishing"); err != nil {
			t.Fatalf("DeleteOutOfBand() error = %v", err)
		}
		cleanup()
		if !tracked() {
			t.Fatal("a relinquished entry must not be read again within the recheck interval")
		}
		backdate()
		cleanup()
		if tracked() {
			t.Error("expected the entry dropped once its resource is gone and the recheck is due")
		}
	})

	t.Run("RelinquishedEntryStaysWhileItsResourceExists", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "kept", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		ledger = pinned(ledger, s.ResourceType(), "kept") // as an encrypted resource is
		if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "kept")
		old := metav1.NewTime(time.Now().Add(-status.RelinquishedRecheckInterval - time.Hour))
		entry.LastVerifiedAt = &old
		status.UpsertManagedResource(&ledger, entry)

		if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		entry, found := status.FindManagedResource(ledger, s.ResourceType(), "kept")
		if !found || entry.State != depsv1alpha1.ManagedResourceStateRelinquished {
			t.Fatalf("expected the entry kept as Relinquished, got %+v (found=%v)", entry, found)
		}
		if time.Since(entry.LastVerifiedAt.Time) > time.Minute {
			t.Error("expected the recheck to restart the interval")
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
		ledger = markPastQuietWindow(ledger, s.ResourceType())

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
		ledger = markPastQuietWindow(ledger, s.ResourceType())

		updated, _, err := s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("second Cleanup() error = %v", err)
		}
		if entry, entryFound := status.FindManagedResource(updated, s.ResourceType(), "widget"); entryFound {
			t.Errorf("expected the ledger entry removed once actually deleted, got %+v", entry)
		}
	})

	t.Run("LettingGoLeavesProvenanceAndKeepsOtherTags", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if err := s.PutTag("widget", "team", "payments"); err != nil {
			t.Fatalf("PutTag() error = %v", err)
		}
		if _, _, err := s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}

		tags := s.Tags("widget")
		if tags[cloudctlaws.ReleasedFromTagKey] != "default/lifecycle-test" {
			t.Errorf("released-from = %q, want the CR it came from", tags[cloudctlaws.ReleasedFromTagKey])
		}
		if tags[cloudctlaws.ReleasedAtTagKey] != time.Now().UTC().Format(time.DateOnly) {
			t.Errorf("released-at = %q, want today's UTC date", tags[cloudctlaws.ReleasedAtTagKey])
		}
		if _, owned := tags[cloudctlaws.OwnerTagKey]; owned {
			t.Error("the ownership tag should be gone")
		}
		if tags["team"] != "payments" {
			t.Errorf("another tool's tag was lost: %v", tags)
		}
	})

	t.Run("LettingGoLeavesNoProvenanceWhenTurnedOff", func(t *testing.T) {
		s := newSubject(t)
		ctx := cloudctlaws.WithReleaseTags(context.Background(), false)
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if _, _, err := s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		if cloudctlaws.HasReleaseTags(s.Tags("widget")) {
			t.Errorf("no released tags expected when turned off, got %v", s.Tags("widget"))
		}
	})

	t.Run("AdoptingAgainRemovesOnlyTheReleasedTags", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if err := s.PutTag("widget", "team", "payments"); err != nil {
			t.Fatalf("PutTag() error = %v", err)
		}
		if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		}
		if !cloudctlaws.HasReleaseTags(s.Tags("widget")) {
			t.Fatal("test setup broken: expected the released tags after letting go")
		}
		// A relinquished entry that stays in the ledger (KMS, or a pinned key)
		// re-adopts through the same path as one that left it.
		if _, err = s.EnsureOne(ctx, ledger, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true}); err != nil {
			t.Fatalf("EnsureOne() with adopt error = %v", err)
		}

		tags := s.Tags("widget")
		if cloudctlaws.HasReleaseTags(tags) {
			t.Errorf("the released tags should go when the resource is owned again, got %v", tags)
		}
		if tags["team"] != "payments" {
			t.Errorf("another tool's tag was lost: %v", tags)
		}
		if tags[cloudctlaws.OwnerTagKey] != "default/lifecycle-test" {
			t.Errorf("the ownership tag should be back, got %v", tags)
		}
	})

	// What status publishes as onDelete must be what Cleanup does.
	t.Run("OnDeleteRetainMatchesCleanup", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyRetain})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "widget")
		if got := decisions.OnDelete(entry); got != depsv1alpha1.OnDeleteRetain {
			t.Fatalf("OnDelete() = %s, want Retain", got)
		}
		if _, results, err := s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("Cleanup() error = %v", err)
		} else if r := findResult(results, "widget"); r == nil || r.Reason != CleanupReasonRetained {
			t.Errorf("onDelete says Retain but Cleanup did %+v", results)
		}
	})

	t.Run("OnDeleteDeleteMatchesCleanup", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyDelete})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "widget")
		if got := decisions.OnDelete(entry); got != depsv1alpha1.OnDeleteDelete {
			t.Fatalf("OnDelete() = %s, want Delete", got)
		}
		if err := s.SetNonEmpty("widget", false); err != nil && !errors.Is(err, ErrUnsupported) {
			t.Fatalf("SetNonEmpty() error = %v", err)
		}
		ledger, _, err = s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("first Cleanup() error = %v", err)
		}
		ledger = markPastQuietWindow(ledger, s.ResourceType())
		if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("second Cleanup() error = %v", err)
		}
		if status.HasManagedResource(ledger, s.ResourceType(), "widget") {
			t.Error("onDelete says Delete but the resource was not deleted")
		}
	})

	t.Run("OnDeleteForcedMatchesCleanup", func(t *testing.T) {
		s := newSubject(t)
		ctx := context.Background()
		ledger, err := s.EnsureOne(ctx, nil, "widget", EnsureOpts{DeletionPolicy: depsv1alpha1.DeletionPolicyDelete})
		if err != nil {
			t.Fatalf("EnsureOne() error = %v", err)
		}
		if err := s.SetNonEmpty("widget", true); errors.Is(err, ErrUnsupported) {
			t.Skip("this resource type has no emptiness check for force to bypass")
		} else if err != nil {
			t.Fatalf("SetNonEmpty() error = %v", err)
		}
		entry, _ := status.FindManagedResource(ledger, s.ResourceType(), "widget")
		entry.Force = true
		status.UpsertManagedResource(&ledger, entry)
		if got := decisions.OnDelete(entry); got != depsv1alpha1.OnDeleteEvenIfNotEmpty {
			t.Fatalf("OnDelete() = %s, want DeleteEvenIfNotEmpty", got)
		}
		ledger, _, err = s.Cleanup(ctx, ledger, nil, false)
		if err != nil {
			t.Fatalf("first Cleanup() error = %v", err)
		}
		ledger = markPastQuietWindow(ledger, s.ResourceType())
		if ledger, _, err = s.Cleanup(ctx, ledger, nil, false); err != nil {
			t.Fatalf("second Cleanup() error = %v", err)
		}
		if status.HasManagedResource(ledger, s.ResourceType(), "widget") {
			t.Error("onDelete says DeleteEvenIfNotEmpty but a non-empty resource was kept")
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
		if entry, entryFound := status.FindManagedResource(ledger, s.ResourceType(), "widget"); !entryFound || entry.PendingDeletionSince == nil {
			t.Fatal("test setup broken: expected the resource marked pending deletion before reappearance")
		}

		updated, _, err := s.Cleanup(ctx, ledger, []string{"widget"}, false)
		if err != nil {
			t.Fatalf("Cleanup (reappeared) error = %v", err)
		}
		entry, entryFound := status.FindManagedResource(updated, s.ResourceType(), "widget")
		if !entryFound {
			t.Fatal("expected the ledger entry to survive reappearance")
		}
		if entry.PendingDeletionSince != nil {
			t.Error("expected PendingDeletionSince cleared on reappearance")
		}
	})
}
