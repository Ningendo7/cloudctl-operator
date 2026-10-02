//go:build live

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

// Live tests run against a real AWS account - no LocalStack, no
// Kubernetes cluster. The integration tier (sqs_integration_test.go)
// catches a wrong belief about SQS's API shape against LocalStack's own
// simulation of it; this tier catches the case where LocalStack's
// simulation itself diverges from the real thing. Skipped entirely unless
// real credentials resolve via the standard AWS credential chain, so
// `go test ./...` and CI never need them, and this can never run by
// accident. Every test creates its own uniquely-named real queue and
// cleans it up via t.Cleanup, which runs even if the test body fails
// partway through. Run explicitly with whatever already authenticates
// your AWS CLI:
//
//	go test -tags=live ./internal/resources/sqs/... -v
package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with a
// genuine, harmless STS call (GetCallerIdentity needs no permissions
// beyond being a valid, authenticated principal).
func skipUnlessLiveAWSCredentials(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}); err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}
	return cfg
}

func newLiveClient(t *testing.T) *sqs.Client {
	t.Helper()
	return sqs.NewFromConfig(skipUnlessLiveAWSCredentials(t))
}

// liveUniqueSuffix keeps each test's queue name distinct so repeated runs
// against the same real account never collide, and never orphan a
// same-named queue from a previous failed run's cleanup.
func liveUniqueSuffix(t *testing.T) string {
	return "live-" + t.Name()[len("TestLive_"):] + "-" + time.Now().UTC().Format("150405")
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

func deleteQueueIfExists(t *testing.T, client *sqs.Client, namespace, crName, resourceKey string) {
	t.Helper()
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", resourceKey, 80)
	urlOut, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		return
	}
	_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl})
}

func TestLive_Ensure_CreatesRealQueueWithAttributesAndTags(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{
			Name:           "orders",
			DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
			Force:          true,
			Overrides:      &depsv1alpha1.SQSOverrides{VisibilityTimeoutSeconds: aws.Int32(45)},
		},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := findEntry(ledger, "orders")
	if entry == nil {
		t.Fatal("expected a ledger entry for orders")
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v — queue wasn't actually created", err)
	}

	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout, types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if got := attrsOut.Attributes["VisibilityTimeout"]; got != "45" {
		t.Errorf("real VisibilityTimeout = %q, want 45", got)
	}
	if got := attrsOut.Attributes[string(types.QueueAttributeNameQueueArn)]; got != entry.ARN {
		t.Errorf("ledger ARN %q doesn't match the real queue's ARN %q", entry.ARN, got)
	}

	tagsOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: urlOut.QueueUrl})
	if err != nil {
		t.Fatalf("real ListQueueTags() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, "uid-1") {
		t.Errorf("real queue tags don't satisfy IsOwnedBy: %+v", tagsOut.Tags)
	}
}

func TestLive_Ensure_AdoptsRealUntaggedQueue(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	if _, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName}); err != nil {
		t.Fatalf("setting up pre-existing real queue: %v", err)
	}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	tagsOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: urlOut.QueueUrl})
	if err != nil {
		t.Fatalf("real ListQueueTags() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, "uid-1") {
		t.Errorf("expected adopt:true to tag the pre-existing real queue as owned, got tags %+v", tagsOut.Tags)
	}
}

func TestLive_Cleanup_DeletesRealQueueImmediatelyWhenForced(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", spec, ledger, true, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger entry to be removed, got %+v", ledger)
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName}); err == nil {
		t.Error("expected the real queue to be gone after Cleanup, but GetQueueUrl succeeded")
	}
}

// TestLive_Ensure_RedrivePolicyClearsWhenDLQRemoved guards against
// RedrivePolicy silently surviving on the real queue after its DLQ is
// removed from spec.
func TestLive_Ensure_RedrivePolicyClearsWhenDLQRemoved(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	dlqName := cloudctlaws.DerivedResourceName(namespace, crName, "sqs", 80, "orders", "dlq")
	t.Cleanup(func() {
		deleteQueueIfExists(t, client, namespace, crName, "orders")
		// deleteQueueIfExists can't be reused here - it computes a plain
		// ResourceName, not the DerivedResourceName-based name the DLQ was
		// actually created under (see naming.go: the two hash different
		// tuples even when DerivedKey's '#'-joined string looks similar).
		urlOut, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &dlqName})
		if err == nil {
			_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl})
		}
	})

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if attrsOut.Attributes["RedrivePolicy"] == "" {
		t.Fatal("expected a RedrivePolicy pointing at the real DLQ, got none")
	}

	// Remove the DLQ from spec - RedrivePolicy on the real main queue must
	// actually clear, not just get left pointing at a now-deleted DLQ ARN.
	// The main queue's own trust window has to be expired first: removing a
	// DLQ is the one spec change reconcileAttributes' own fast path doesn't
	// treat as needing a round trip on its own (see sqs.go's
	// reconcileAttributes doc comment) - a known, accepted gap, not
	// something this test is trying to disprove.
	mainEntry := status.FindManagedResource(ledger, "sqs", "orders")
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	mainEntry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, *mainEntry)

	spec.Resources[0].DLQ = false
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", spec, ledger, false, false, nil); err != nil {
		t.Fatalf("Cleanup() (removing the now-undeclared DLQ) error = %v", err)
	}

	attrsOut, err = client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if got := attrsOut.Attributes["RedrivePolicy"]; got != "" {
		t.Errorf("expected RedrivePolicy cleared on the real queue after removing the DLQ, still got %q", got)
	}

	if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &dlqName}); err == nil {
		t.Error("expected the real DLQ to actually be deleted once undeclared")
	}
}

func TestLive_Ensure_FIFOQueueGetsRealFifoSuffixAndAttribute(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", FIFO: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	// deleteQueueIfExists alone can't find this queue - a FIFO queue's real
	// name carries a ".fifo" suffix and a correspondingly shorter hash
	// budget, neither of which deleteQueueIfExists's plain-name computation
	// accounts for, so it would silently no-op and leak this queue.
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80-len(".fifo")) + ".fifo"
	t.Cleanup(func() {
		urlOut, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &queueName})
		if err != nil {
			return
		}
		_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl})
	})

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := findEntry(ledger, "orders")
	if entry == nil {
		t.Fatal("expected a ledger entry for the FIFO queue")
	}

	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() for the .fifo-suffixed name error = %v — real AWS didn't accept/return the name the way Ensure assumes", err)
	}
	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameFifoQueue},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if attrsOut.Attributes["FifoQueue"] != "true" {
		t.Errorf("expected real FifoQueue attribute true, got %q", attrsOut.Attributes["FifoQueue"])
	}
}

// TestLive_Cleanup_BlocksDeletingQueueWithARealMessage exercises the actual
// approximate-count counters SQS reports, not a fake's hand-set field —
// AWS documents these as eventually consistent, so this also confirms
// that consistency window in practice is short enough for the existing
// quiet-window/backoff design to be sound.
func TestLive_Cleanup_BlocksDeletingQueueWithARealMessage(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: false},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	body := "keep-this-queue-non-empty"
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: urlOut.QueueUrl, MessageBody: &body}); err != nil {
		t.Fatalf("real SendMessage() error = %v", err)
	}
	waitForApproxMessageCount(t, client, *urlOut.QueueUrl, "1", 90*time.Second)

	// First pass only marks pending (quiet window); backdate it so the
	// second pass evaluates the real emptiness check instead of waiting.
	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	entry := status.FindManagedResource(ledger, "sqs", "orders")
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, *entry)

	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName}); err != nil {
		t.Error("expected the real queue with a genuine message still in it to survive Cleanup, but it's gone")
	}
}

// TestLive_Ensure_RefusesQueueOwnedByADifferentRealCR confirms the
// tag-based ownership check against real, round-tripped AWS tags, not a
// fake's in-memory map.
func TestLive_Ensure_RefusesQueueOwnedByADifferentRealCR(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ownerSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "owner-uid", ownerSpec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (establishing the real owner) error = %v", err)
	}

	intruderSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "intruder-uid", intruderSpec, nil, nil, nil); err == nil {
		t.Error("expected Ensure to refuse a real queue already owned by a different CR's UID, even with adopt:true")
	}
}

// TestLive_Ensure_DedicatedKMSKeyEncryptsRealQueue crosses two real
// services - SQS and KMS - which LocalStack's own KMS support is
// explicitly not trusted enough to validate here (see docs/testing.md).
// The created key can't be deleted immediately by design (KMS enforces a
// minimum 7-day pending-deletion window); t.Cleanup schedules deletion via
// the real Cleanup path rather than leaving it dangling, accepting that
// small, unavoidable real-AWS cost.
func TestLive_Ensure_DedicatedKMSKeyEncryptsRealQueue(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sqs.NewFromConfig(cfg)
	kmsClient := kms.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}
	t.Cleanup(func() {
		if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, nil, true, false, nil); err != nil {
			t.Logf("cleanup warning: %v", err)
		}
		deleteQueueIfExists(t, client, namespace, crName, "orders")
	})

	ledger, err := Ensure(ctx, client, kmsClient, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	t.Cleanup(func() {
		if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, true, false, nil); err != nil {
			t.Logf("key cleanup warning: %v", err)
		}
		// sqs.Cleanup only tears down "sqs"-type ledger entries, and the
		// kms package's own Cleanup won't schedule deletion within a single
		// pass (it only starts a quiet window) - delete the real key
		// directly instead of leaving it dangling.
		if keyEntry := status.FindManagedResource(ledger, "kms", cloudctlaws.DedicatedKeyLedgerName(resourceType, "orders")); keyEntry != nil {
			windowDays := int32(7)
			if _, err := kmsClient.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: &keyEntry.ARN, PendingWindowInDays: &windowDays}); err != nil {
				t.Logf("key cleanup warning: %v", err)
			}
		}
	})

	keyEntry := status.FindManagedResource(ledger, "kms", cloudctlaws.DedicatedKeyLedgerName(resourceType, "orders"))
	if keyEntry == nil {
		t.Fatal("expected a dedicated KMS key ledger entry for the encrypted queue")
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameKmsMasterKeyId},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	if attrsOut.Attributes["KmsMasterKeyId"] != keyEntry.ARN {
		t.Errorf("real queue's KmsMasterKeyId = %q, want the dedicated key's real ARN %q", attrsOut.Attributes["KmsMasterKeyId"], keyEntry.ARN)
	}

	if _, err := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: &keyEntry.ARN}); err != nil {
		t.Errorf("expected the dedicated key to genuinely exist in real KMS, DescribeKey error = %v", err)
	}
}

// TestLive_Ensure_RecreatesQueueDeletedExternally guards against the real
// AWS QueueDoesNotExist exception failing to deserialize into the exact
// typed error the recreate path matches via errors.As - a fake client
// constructing that same error by hand can never catch a mismatch here.
func TestLive_Ensure_RecreatesQueueDeletedExternally(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl}); err != nil {
		t.Fatalf("simulating external deletion: real DeleteQueue() error = %v", err)
	}

	// Bypass the trust window so Ensure actually re-verifies against real
	// AWS on this pass, instead of trusting the (now-stale) cached entry.
	entry := status.FindManagedResource(ledger, "sqs", "orders")
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	entry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, *entry)

	// AWS enforces a ~60s cooldown before a deleted queue's name can be
	// reused (QueueDeletedRecently) - retry through it rather than sleep a
	// fixed amount, so this doesn't hardcode AWS's own undocumented-exact
	// cooldown length.
	var updated []depsv1alpha1.ManagedResource
	deadline := time.Now().Add(2 * time.Minute)
	for {
		updated, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
		if err == nil {
			break
		}
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable || time.Now().After(deadline) {
			t.Fatalf("recovery Ensure() error = %v", err)
		}
		time.Sleep(5 * time.Second)
	}
	// SQS ARNs are purely name-derived (region+account+queue name), not a
	// unique creation-time identifier, so the recreated queue legitimately
	// gets the exact same ARN as before - the meaningful check is that the
	// queue genuinely exists again, not that its ARN changed.
	if status.FindManagedResource(updated, "sqs", "orders") == nil {
		t.Fatal("expected a ledger entry for the recreated queue")
	}
	if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName}); err != nil {
		t.Errorf("expected the queue to exist again after recovery, real GetQueueUrl() error = %v", err)
	}
}

// TestLive_Ensure_ClassifiesRecentlyDeletedQueueAsRetryable guards against
// AWS's real QueueDeletedRecently cooldown (recreating under the same name
// too soon after deletion) failing to deserialize into the exact typed
// error the retryable-classification path matches via errors.As.
func TestLive_Ensure_ClassifiesRecentlyDeletedQueueAsRetryable(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real CreateQueue() (setup) error = %v", err)
	}
	if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: createOut.QueueUrl}); err != nil {
		t.Fatalf("real DeleteQueue() (setup) error = %v", err)
	}

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	_, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err == nil {
		t.Skip("real AWS didn't enforce the post-deletion cooldown fast enough for this test to observe it")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Errorf("expected a retryable ReconcileError for a queue deleted too recently to recreate, got %v", err)
	}
}

// waitForApproxMessageCount polls GetQueueAttributes until
// ApproximateNumberOfMessages matches want or timeout elapses - SQS's own
// docs describe this counter as eventually consistent, typically settling
// within a minute of a send.
func waitForApproxMessageCount(t *testing.T, client *sqs.Client, queueURL, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
			QueueUrl:       &queueURL,
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
		})
		if err == nil && out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] == want {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("real ApproximateNumberOfMessages never reached %q within %s", want, timeout)
}
