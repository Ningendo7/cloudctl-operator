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

package sns

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func setupTopic(t *testing.T, client *fakeSNS, namespace, crName, name string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	t.Helper()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: name, DeletionPolicy: deletionPolicy, Force: force},
	}}
	ledger, err := Ensure(context.Background(), client, namespace, crName, "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}
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

func TestCleanup_RetainsByDefaultWhenRemovedFromSpec(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyRetain, false)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected events to be reported Retained, got %+v", results)
	}
	if status.FindManagedResource(updated, "sns", "events") == nil {
		t.Error("expected retained entry to stay in the ledger")
	}
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Error("expected Retain policy to leave the AWS topic in place")
	}
}

func TestCleanup_TreatsUnsetDeletionPolicyAsRetain(t *testing.T) {
	// Defensive-coding verification: Cleanup checks "!= Delete", not
	// "== Retain", specifically so a DeletionPolicy that's somehow truly
	// unset (Go's zero value, bypassing the CRD's own default) still falls
	// to the safe side rather than an undefined one. Every other test
	// exercises an explicitly-set DeletionPolicyRetain, which never
	// actually proves this - this one constructs the ledger entry directly
	// with the field left at its zero value.
	client := newFakeSNS()
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn] = &fakeTopic{
		arn: topicArn,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
	ledger := []depsv1alpha1.ManagedResource{
		{
			Type: resourceType,
			Name: "events",
			ARN:  topicArn,
			// DeletionPolicy deliberately left unset (zero value).
		},
	}

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r == nil || r.Reason != CleanupReasonRetained {
		t.Errorf("expected an unset DeletionPolicy to be treated as Retained, got %+v", results)
	}
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Error("expected the topic to survive - an unset DeletionPolicy must never be treated as Delete")
	}
	if status.FindManagedResource(updated, "sns", "events") == nil {
		t.Error("expected the retained entry to stay in the ledger")
	}
}

func TestCleanup_RelinquishesOwnershipTagForRetainedResource(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyRetain, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))

	if !cloudctlaws.IsOwnedBy(client.topics[topicArn].tags, "default", "checkout-service", "uid-1") {
		t.Fatal("test setup broken: expected the topic to start out owned by us")
	}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if cloudctlaws.IsOwnedBy(client.topics[topicArn].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the ownership tag to be relinquished once a Retain resource is no longer declared")
	}
}

func TestCleanup_HoldsNewlyEligibleTopicForQuietWindowBeforeDeleting(t *testing.T) {
	// Regression-shaped test for the ListSubscriptionsByTopic consistency
	// gap: a topic that looks empty on the very first pass it comes up for
	// deletion must NOT be deleted immediately - only denied and held. Only
	// once the quiet window has elapsed AND it's still confirmed empty
	// should it actually delete.
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected events to be held PendingDeletion on first encounter, got %+v", results)
	}
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Fatal("expected the topic to survive the first pass even though it's empty right now")
	}
	entry := status.FindManagedResource(updated, "sns", "events")
	if entry == nil || entry.PendingDeletionSince == nil {
		t.Fatal("expected PendingDeletionSince to be recorded on first encounter")
	}

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, updated, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Fatalf("expected events to still be PendingDeletion while inside the quiet window, got %+v", results)
	}
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Fatal("expected the topic to still survive while inside the quiet window")
	}

	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry = status.FindManagedResource(updated, "sns", "events")
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&updated, *entry)

	updated, results, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, updated, false)
	if err != nil {
		t.Fatalf("third Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r != nil {
		t.Errorf("expected no pending result once actually deleted, got %+v", results)
	}
	if _, stillExists := client.topics[topicArn]; stillExists {
		t.Error("expected the topic to be deleted once the quiet window elapsed and it's confirmed empty")
	}
	if status.FindManagedResource(updated, "sns", "events") != nil {
		t.Error("expected the ledger entry to be removed after deletion")
	}
}

func TestCleanup_BlocksDeletingTopicWithActiveSubscriptions(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn].subscriptions = 1

	// First pass only enters the mandatory quiet window - subscriptions
	// aren't evaluated yet on first encounter.
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}

	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry := status.FindManagedResource(ledger, "sns", "events")
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)

	updated, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r == nil || r.Reason != CleanupReasonPendingDeletion {
		t.Errorf("expected events to be PendingDeletion, got %+v", results)
	}
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Error("expected the topic with active subscriptions to NOT be deleted")
	}
	if entry := status.FindManagedResource(updated, "sns", "events"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected PendingDeletionSince to remain recorded")
	}
}

func TestCleanup_AddsDenyPolicyWhenMarkingPendingDeletion(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn].subscriptions = 1

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	policy := client.topics[topicArn].policy
	if !strings.Contains(policy, pendingDeletionDenySid) {
		t.Errorf("expected a deny policy to be attached once pending deletion, got policy=%s", policy)
	}
	if !strings.Contains(policy, "sns:Publish") || !strings.Contains(policy, "sns:Subscribe") {
		t.Errorf("expected the pending-deletion deny to block both Publish and Subscribe so nothing new can attach while held, got policy=%s", policy)
	}
}

func TestCleanup_RemovesDenyPolicyWhenTopicReturnsToSpec(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn].subscriptions = 1

	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if !strings.Contains(client.topics[topicArn].policy, pendingDeletionDenySid) {
		t.Fatal("test setup broken: expected the deny policy to be attached after the first pass")
	}

	backInSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete}}}
	ledger, _, err = Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", backInSpec, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}

	if strings.Contains(client.topics[topicArn].policy, pendingDeletionDenySid) {
		t.Errorf("expected the deny policy to be removed once the topic is declared again, got policy=%s", client.topics[topicArn].policy)
	}
	if entry := status.FindManagedResource(ledger, "sns", "events"); entry == nil || entry.PendingDeletionSince != nil {
		t.Errorf("expected PendingDeletionSince to be cleared once the topic is declared again, got %+v", entry)
	}
}

func TestCleanup_EscalatesToStuckAfterGracePeriod(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn].subscriptions = 1

	longAgo := metav1.NewTime(time.Now().Add(-2 * PendingDeletionGracePeriod))
	entry := status.FindManagedResource(ledger, "sns", "events")
	entry.PendingDeletionSince = &longAgo
	status.UpsertManagedResource(&ledger, *entry)

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if r := findResult(results, "events"); r == nil || r.Reason != CleanupReasonStuckPendingDeletion {
		t.Errorf("expected events to escalate to StuckPendingDeletion, got %+v", results)
	}
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Error("expected the stuck topic to still NOT be force-deleted automatically")
	}
}

func TestCleanup_ForceDeletesTopicWithActiveSubscriptions(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, true)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn].subscriptions = 1

	_, results, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected force to override the active-subscriptions guard, got %v", results)
	}
	if _, stillExists := client.topics[topicArn]; stillExists {
		t.Error("expected force:true to delete the topic despite active subscriptions")
	}
}

func TestCleanup_RefusesDeletingUnverifiedOwnership(t *testing.T) {
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))
	client.topics[topicArn].tags = map[string]string{"team": "someone-else"}

	_, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected Cleanup to refuse deleting a topic whose ownership tags no longer verify")
	}
	if _, stillExists := client.topics[topicArn]; !stillExists {
		t.Error("expected the topic to NOT be deleted when ownership can't be re-verified")
	}
}

func TestCleanup_TreatsAlreadyDeletedTopicAsSuccess(t *testing.T) {
	// Regression-shaped test carried over from a real bug found reviewing a
	// sibling project's S3 cleanup: without treating "already gone" as
	// success, retrying cleanup on a topic a previous attempt had already
	// deleted (e.g. after a transient failure removing the finalizer
	// itself) would wrongly report a failure for a topic that was, in
	// fact, correctly cleaned up already.
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	topicArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "events"))

	// First pass only enters the mandatory quiet window.
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry := status.FindManagedResource(ledger, "sns", "events")
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)

	// Simulate the topic already having been deleted out from under us.
	delete(client.topics, topicArn)

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v — expected an already-gone topic to be treated as already cleaned up, not a failure", err)
	}
	if status.FindManagedResource(updated, "sns", "events") != nil {
		t.Error("expected the ledger entry to be removed for an already-gone topic")
	}
}

func TestCleanup_DoesNotForgetTopicOnTransientLookupError(t *testing.T) {
	// Regression test mirroring the equivalent sqs fix: ListTagsForResource
	// failing for any reason other than genuinely-not-found (throttling, a
	// permission gap, a network blip) must be reported as an error, not
	// silently treated as "the topic is gone" - that would drop a topic
	// that still exists from the ledger, abandoning it without ever
	// actually deleting or retaining it per policy.
	client := newFakeSNS()
	ledger := setupTopic(t, client, "default", "checkout-service", "events", depsv1alpha1.DeletionPolicyDelete, false)
	ledger, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry := status.FindManagedResource(ledger, "sns", "events")
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)

	client.listTagsForResourceErr = &fakeAWSError{code: "ThrottlingException", fault: smithy.FaultServer}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected a transient ListTagsForResource failure to be reported as an error, not silently swallowed")
	}
	if status.FindManagedResource(updated, "sns", "events") == nil {
		t.Error("expected the ledger entry to survive a transient lookup failure, not be silently forgotten")
	}
}

func TestHasSubscriptions_ContinuesPastAnEmptyPageWithNextToken(t *testing.T) {
	// Regression-shaped test: the fake's first page is deliberately empty
	// but carries a NextToken, and the real subscription only appears on
	// the second page. A loop that stops at "this page had nothing" without
	// checking NextToken would wrongly report no subscriptions.
	client := newFakeSNS()
	arn := "arn:aws:sns:us-east-1:123456789012:events"
	client.topics[arn] = &fakeTopic{arn: arn, subscriptions: 1}

	has, err := hasSubscriptions(context.Background(), client, arn)
	if err != nil {
		t.Fatalf("hasSubscriptions() error = %v", err)
	}
	if !has {
		t.Error("expected hasSubscriptions to find the subscription on the second page, not stop at the empty first page")
	}
}

func TestHasSubscriptions_FalseWhenNoneExist(t *testing.T) {
	client := newFakeSNS()
	arn := "arn:aws:sns:us-east-1:123456789012:events"
	client.topics[arn] = &fakeTopic{arn: arn}

	has, err := hasSubscriptions(context.Background(), client, arn)
	if err != nil {
		t.Fatalf("hasSubscriptions() error = %v", err)
	}
	if has {
		t.Error("expected hasSubscriptions to report false when there are none")
	}
}

func TestCleanup_ContinuesToOtherResourcesAfterOneFails(t *testing.T) {
	// Regression test: a failure cleaning up one ledger entry must not
	// prevent an unrelated entry from being processed in the same pass.
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "orders-events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Name: "user-events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil)
	if err != nil {
		t.Fatalf("setup Ensure() error = %v", err)
	}

	// Corrupt orders-events' ownership out-of-band so its cleanup fails.
	badArn := cloudctlaws.TopicARN(testRegion, testAccountID, cloudctlaws.ResourceName("default", "checkout-service", "orders-events"))
	client.topics[badArn].tags = map[string]string{"team": "someone-else"}

	updated, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false)
	if err == nil {
		t.Fatal("expected an error reported for the corrupted-ownership topic")
	}

	// user-events is a fresh delete candidate, so this first pass only
	// enters its mandatory quiet window rather than deleting it outright -
	// what matters for this regression test is that it was processed at all
	// (i.e. orders-events failing didn't stop the loop before reaching it).
	if entry := status.FindManagedResource(updated, "sns", "user-events"); entry == nil || entry.PendingDeletionSince == nil {
		t.Error("expected user-events to still be processed into its own pending-deletion window despite orders-events failing")
	}
	if status.FindManagedResource(updated, "sns", "orders-events") == nil {
		t.Error("expected the failed orders-events entry to remain in the ledger for retry")
	}
}
