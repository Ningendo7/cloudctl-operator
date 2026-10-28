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
	"fmt"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/livetest"
	"github.com/Ningendo7/cloudctl-operator/internal/policycheck"
	kmspkg "github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	snspkg "github.com/Ningendo7/cloudctl-operator/internal/resources/sns"
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

	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
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
	if _, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil); err != nil {
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

	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	ledger, _, err = Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, true, false, nil)
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
	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
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
	mainEntry, _ := status.FindManagedResource(ledger, "sqs", "orders")
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	mainEntry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, mainEntry)

	spec.Resources[0].DLQ = false
	if _, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if _, _, err := Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, false, false, nil); err != nil {
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

	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
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

	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
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
	ledger, _, err = Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	entry, _ := status.FindManagedResource(ledger, "sqs", "orders")
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, entry)

	if _, _, err := Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil); err != nil {
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
	if _, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "owner-uid", ownerSpec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (establishing the real owner) error = %v", err)
	}

	intruderSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "intruder-uid", intruderSpec, nil, nil, nil); err == nil {
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
		if _, _, err := Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, nil, true, false, nil); err != nil {
			t.Logf("cleanup warning: %v", err)
		}
		deleteQueueIfExists(t, client, namespace, crName, "orders")
	})

	ledger, err := Ensure(ctx, client, kmsClient, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	t.Cleanup(func() {
		if _, _, err := Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, true, false, nil); err != nil {
			t.Logf("key cleanup warning: %v", err)
		}
		// sqs.Cleanup only tears down "sqs"-type ledger entries, and the
		// kms package's own Cleanup won't schedule deletion within a single
		// pass (it only starts a quiet window) - delete the real key
		// directly instead of leaving it dangling.
		if keyEntry, keyEntryFound := status.FindManagedResource(ledger, "kms", cloudctlaws.DedicatedKeyLedgerName(resourceType, "orders")); keyEntryFound {
			windowDays := int32(7)
			if _, err := kmsClient.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: &keyEntry.ARN, PendingWindowInDays: &windowDays}); err != nil {
				t.Logf("key cleanup warning: %v", err)
			}
		}
	})

	keyEntry, keyEntryFound := status.FindManagedResource(ledger, "kms", cloudctlaws.DedicatedKeyLedgerName(resourceType, "orders"))
	if !keyEntryFound {
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

	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
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
	entry, _ := status.FindManagedResource(ledger, "sqs", "orders")
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	entry.LastVerifiedAt = &stale
	status.UpsertManagedResource(&ledger, entry)

	// AWS enforces a ~60s cooldown before a deleted queue's name can be
	// reused (QueueDeletedRecently) - retry through it rather than sleep a
	// fixed amount, so this doesn't hardcode AWS's own undocumented-exact
	// cooldown length.
	var updated []depsv1alpha1.ManagedResource
	deadline := time.Now().Add(2 * time.Minute)
	for {
		updated, err = Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
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
	if !status.HasManagedResource(updated, "sqs", "orders") {
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
	_, err = Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
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

// TestLive_Cleanup_RetainedQueueIsLetGoAndAMissingOneIsGone checks against
// real SQS that letting go removes the ownership tags and drops the ledger
// entry, and that a queue that doesn't exist is recognized as gone.
func TestLive_Cleanup_RetainedQueueIsLetGoAndAMissingOneIsGone(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}}}
	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	ledger, results, err := Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 1 || results[0].Reason != CleanupReasonRetained {
		t.Errorf("expected one Retained result, got %+v", results)
	}
	if status.HasManagedResource(ledger, resourceType, "orders") {
		t.Error("expected the retained queue to leave the ledger")
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("the retained queue should still exist: %v", err)
	}
	tagsOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: urlOut.QueueUrl})
	if err != nil {
		t.Fatalf("real ListQueueTags() error = %v", err)
	}
	if _, owned := tagsOut.Tags[cloudctlaws.OwnerTagKey]; owned || tagsOut.Tags[cloudctlaws.ReleasedFromTagKey] != namespace+"/"+crName {
		t.Errorf("expected the ownership tag gone and released-from left, tags: %+v", tagsOut.Tags)
	}

	missing := depsv1alpha1.ManagedResource{Type: resourceType, Name: "never-created", ARN: "arn:aws:sqs:us-east-1:123456789012:" + cloudctlaws.ResourceName(namespace, crName, "sqs", "never-created", 80)}
	_, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, "uid-1", missing, nil)
	if err != nil || !gone {
		t.Errorf("relinquishIfStillTagged() on a missing queue gone=%v err=%v, want gone", gone, err)
	}
}

// TestLive_Cleanup_RetainedEncryptedQueueKeepsItsEntryUntilDeletedByHand
// checks that a retained queue pinning a key stays as Relinquished until it
// is deleted by hand and the daily recheck runs.
func TestLive_Cleanup_RetainedEncryptedQueueKeepsItsEntryUntilDeletedByHand(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sqs.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	key := livetest.SharedKey(t, cfg)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: livetest.SharedKeyName}},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })
	ledger, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, []depsv1alpha1.ManagedResource{key}, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	// The controller stamps the key on the entry of a resource using it.
	queue, _ := status.FindManagedResource(ledger, resourceType, "orders")
	queue.KMSKeyARN = key.ARN
	status.UpsertManagedResource(&ledger, queue)

	ledger, _, err = Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() (retain) error = %v", err)
	}
	entry, found := status.FindManagedResource(ledger, resourceType, "orders")
	if !found || entry.State != depsv1alpha1.ManagedResourceStateRelinquished {
		t.Fatalf("expected a Relinquished entry for the queue that pins a key, got %+v (found=%v)", entry, found)
	}

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("GetQueueUrl() error = %v", err)
	}
	if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl}); err != nil {
		t.Fatalf("DeleteQueue() error = %v", err)
	}
	// SQS keeps answering for a queue for up to a minute after deleting it.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var notFound *types.QueueDoesNotExist
		if _, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName}); errors.As(err, &notFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the deleted queue never stopped being found")
		}
		time.Sleep(5 * time.Second)
	}

	due := metav1.NewTime(time.Now().Add(-(status.RelinquishedRecheckInterval + time.Hour)))
	entry.LastVerifiedAt = &due
	status.UpsertManagedResource(&ledger, entry)
	ledger, _, err = Cleanup(ctx, client, nil, nil, namespace, crName, "uid-1", &depsv1alpha1.SQSSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() (recheck) error = %v", err)
	}
	if status.HasManagedResource(ledger, resourceType, "orders") {
		t.Error("expected the entry of the deleted queue to be dropped on the recheck")
	}
}

// TestLive_Ensure_TakesOverAQueueFromAPreviousUID checks a restored CR is
// refused a queue tagged with its old UID, and takes it over when told which.
func TestLive_Ensure_TakesOverAQueueFromAPreviousUID(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	t.Cleanup(func() { deleteQueueIfExists(t, client, namespace, crName, "orders") })
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true}}}

	if _, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}
	if _, err := Ensure(ctx, client, nil, nil, nil, namespace, crName, "uid-2", spec, nil, nil, nil); err == nil {
		t.Fatal("expected a new UID to be refused without adopt-from-uid")
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, "uid-1")
	if _, err := Ensure(takeover, client, nil, nil, nil, namespace, crName, "uid-2", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("GetQueueUrl() error = %v", err)
	}
	tagsOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: urlOut.QueueUrl})
	if err != nil {
		t.Fatalf("ListQueueTags() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsOut.Tags, namespace, crName, "uid-2") {
		t.Errorf("expected the queue to be tagged with the new UID, got %+v", tagsOut.Tags)
	}
}

func waitUntil(within time.Duration, check func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// countingSQS counts the queue policy writes Ensure makes.
type countingSQS struct {
	sqsAPI
	policyWrites int
}

func (c *countingSQS) SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	if _, ok := in.Attributes[string(types.QueueAttributeNamePolicy)]; ok {
		c.policyWrites++
	}
	return c.sqsAPI.SetQueueAttributes(ctx, in, optFns...)
}

// queuePolicyText reads the policy as SQS hands it back, empty when there is none.
func queuePolicyText(t *testing.T, client *sqs.Client, queueURL string) string {
	t.Helper()
	out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: &queueURL, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes(Policy) error = %v", err)
	}
	return out.Attributes[string(types.QueueAttributeNamePolicy)]
}

// TestLive_Ensure_QueuesCarryTheTransportStatementAndItIsNotRewritten confirms
// SQS accepts the transport statement on a queue and its dead-letter queue and
// Access Analyzer finds nothing wrong with it, that HTTPS still works, that a
// second pass writes nothing (SQS hands the policy back reformatted, which
// must still compare equal), and that the pending-deletion statement joins it
// and leaves it when removed. Whether a plain-HTTP request is refused cannot be
// shown here: SQS now refuses one on every queue ("All requests to this queue
// must use HTTPS and SigV4"), policy or not, so the statement is there for the
// compliance checks that look for it.
func TestLive_Ensure_QueuesCarryTheTransportStatementAndItIsNotRewritten(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sqs.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	dlqName := cloudctlaws.DerivedResourceName(namespace, crName, "sqs", 80, "orders", "dlq")
	t.Cleanup(func() {
		deleteQueueIfExists(t, client, namespace, crName, "orders")
		if out, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &dlqName}); err == nil {
			_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
		}
	})

	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DLQ: true, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	recordEvent, events := newEventCollector()
	counted := &countingSQS{sqsAPI: client}
	ledger, err := Ensure(ctx, counted, nil, nil, nil, namespace, crName, "uid-1", spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if countEvents(*events, "TransportSecurityEnforced") != 2 {
		t.Errorf("expected the statement on the queue and its dead-letter queue, got events %v", *events)
	}

	urls := map[string]string{}
	for _, name := range []string{queueName, dlqName} {
		out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &name})
		if err != nil {
			t.Fatalf("real GetQueueUrl(%s) error = %v", name, err)
		}
		urls[name] = *out.QueueUrl
		policy := queuePolicyText(t, client, *out.QueueUrl)
		if got := strings.Count(policy, awspolicy.InsecureTransportSid); got != 1 {
			t.Fatalf("expected the transport statement once on %s, found %d in %s", name, got, policy)
		}
		policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)
	}

	url := urls[queueName]
	send := func(c *sqs.Client) error {
		_, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &url, MessageBody: aws.String("hello")})
		return err
	}
	if err := send(client); err != nil {
		t.Fatalf("real SendMessage over HTTPS error = %v: the statement must not refuse TLS", err)
	}
	writes := counted.policyWrites
	if _, err := Ensure(ctx, counted, nil, nil, nil, namespace, crName, "uid-1", spec, withTrustExpired(ledger), nil, recordEvent); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if counted.policyWrites != writes {
		t.Errorf("the second pass wrote a queue policy %d more time(s): the stored policy reads as different from the desired one", counted.policyWrites-writes)
	}

	entry := findEntry(ledger, "orders")
	if entry == nil {
		t.Fatal("expected a ledger entry for orders")
	}
	if err := addPendingDeletionDeny(ctx, client, url, entry.ARN); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	policy := queuePolicyText(t, client, url)
	if !strings.Contains(policy, pendingDeletionDenySid) || !strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("expected both statements after the pending-deletion deny, got %s", policy)
	}
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)
	if err := removePendingDeletionDeny(ctx, client, url); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}
	policy = queuePolicyText(t, client, url)
	if strings.Contains(policy, pendingDeletionDenySid) || !strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("expected only the transport statement left, got %s", policy)
	}
}

// TestLive_QueuePolicyAcceptsTwentyStatementsAndRefusesMore checks the limit
// behind the cap on subscriptions per queue: every subscription takes one
// statement of the queue policy, next to the operator's two.
func TestLive_QueuePolicyAcceptsTwentyStatementsAndRefusesMore(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sqs.NewFromConfig(cfg)
	ctx := context.Background()
	queueName := shortLiveName(t, "limit")
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real CreateQueue() (setup) error = %v", err)
	}
	t.Cleanup(func() { _, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl}) })
	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: out.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() (setup) error = %v", err)
	}
	queueARN := attrs.Attributes[string(types.QueueAttributeNameQueueArn)]
	topicPrefix := strings.Replace(queueARN[:strings.LastIndex(queueARN, ":")], ":sqs:", ":sns:", 1)

	policyOf := func(n int) string {
		statements := make([]string, n)
		for i := range statements {
			statements[i] = fmt.Sprintf(`{"Sid":"s%d","Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":"sqs:SendMessage","Resource":%q,"Condition":{"ArnEquals":{"aws:SourceArn":"%s:topic-%d"}}}`,
				i, queueARN, topicPrefix, i)
		}
		return `{"Version":"2012-10-17","Statement":[` + strings.Join(statements, ",") + `]}`
	}
	set := func(n int) error {
		_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl: out.QueueUrl, Attributes: map[string]string{string(types.QueueAttributeNamePolicy): policyOf(n)},
		})
		return err
	}
	if err := set(20); err != nil {
		t.Errorf("a queue policy with 20 statements was refused: %v", err)
	}
	if err := set(21); err == nil {
		t.Error("a queue policy with 21 statements was accepted: the documented limit of 20 does not hold, so the cap on subscriptions could be raised")
	} else {
		t.Logf("21 statements are refused with: %v", err)
	}
}

func newLiveSNSClient(t *testing.T) *sns.Client {
	t.Helper()
	return sns.NewFromConfig(skipUnlessLiveAWSCredentials(t))
}

// liveSubscriptionFixture mirrors subscriptionFixture from the integration
// tier, but against real AWS: a real queue, a real topic, and an
// authorized producer CR resolved through a fake controller-runtime
// client (that resolution logic has no AWS dependency to verify here
// either).
type liveSubscriptionFixture struct {
	sqsClient                     *sqs.Client
	snsClient                     *sns.Client
	k8sClient                     client.Client
	queueName, queueURL, queueARN string
	topicARN                      string
	ref                           depsv1alpha1.ConsumeRef
}

// shortLiveName fits SQS's 80 character limit, which the test name alone can
// exceed.
func shortLiveName(t *testing.T, kind string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(t.Name()))
	return fmt.Sprintf("live-%08x-%s-%s", h.Sum32(), time.Now().UTC().Format("150405"), kind)
}

func setupLiveSubscriptionFixture(t *testing.T) liveSubscriptionFixture {
	t.Helper()
	ctx := context.Background()
	sqsClient := newLiveClient(t)
	snsClient := newLiveSNSClient(t)

	queueName := shortLiveName(t, "orders")
	createQueueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real CreateQueue() (setup) error = %v", err)
	}
	queueURL := *createQueueOut.QueueUrl
	t.Cleanup(func() { _, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: &queueURL}) })

	attrsOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() (setup) error = %v", err)
	}
	queueARN := attrsOut.Attributes[string(types.QueueAttributeNameQueueArn)]

	topicName := shortLiveName(t, "events")
	createTopicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicName})
	if err != nil {
		t.Fatalf("real CreateTopic() (setup) error = %v", err)
	}
	topicARN := *createTopicOut.TopicArn
	t.Cleanup(func() { _, _ = snsClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: &topicARN}) })

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events", topicARN, "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()

	return liveSubscriptionFixture{
		sqsClient: sqsClient,
		snsClient: snsClient,
		k8sClient: k8sClient,
		queueName: queueName,
		queueURL:  queueURL,
		queueARN:  queueARN,
		topicARN:  topicARN,
		ref:       ref,
	}
}

func TestLive_EnsureSubscriptions_RealSubscribeAndQueuePolicyGrant(t *testing.T) {
	ctx := context.Background()
	f := setupLiveSubscriptionFixture(t)

	ledger, err := EnsureSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL, f.queueARN,
		[]depsv1alpha1.ConsumeRef{f.ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}
	ledgerName := subscriptionLedgerName(f.queueName, f.ref)
	entry, entryFound := status.FindManagedResource(ledger, subscriptionResourceType, ledgerName)
	if !entryFound {
		t.Fatal("expected a ledger entry for the new subscription")
	}

	subsOut, err := f.snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &f.topicARN})
	if err != nil {
		t.Fatalf("real ListSubscriptionsByTopic() error = %v", err)
	}
	if len(subsOut.Subscriptions) != 1 {
		t.Fatalf("expected exactly one real subscription on the topic, got %d", len(subsOut.Subscriptions))
	}
	if *subsOut.Subscriptions[0].SubscriptionArn != entry.ARN {
		t.Errorf("ledger ARN %q doesn't match the real subscription's ARN %q", entry.ARN, *subsOut.Subscriptions[0].SubscriptionArn)
	}

	policyOut, err := f.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &f.queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes(Policy) error = %v", err)
	}
	policy := policyOut.Attributes[string(types.QueueAttributeNamePolicy)]
	if policy == "" {
		t.Fatal("expected a real queue policy granting the topic send access, got none")
	}
	if !strings.Contains(policy, f.topicARN) {
		t.Errorf("expected the queue policy to scope the grant to topic ARN %q, got: %s", f.topicARN, policy)
	}
}

// SNS isn't an IAM identity in the account, so delivery into a queue
// encrypted with an operator-managed key works only if the key's own policy
// lets SNS use it. Only real AWS enforces key policies.
func TestLive_EnsureSubscriptions_DeliversIntoAQueueEncryptedWithTheSharedKey(t *testing.T) {
	ctx := context.Background()
	f := setupLiveSubscriptionFixture(t)
	cfg := skipUnlessLiveAWSCredentials(t)
	key := livetest.SharedKey(t, cfg)
	// What the key policy sync grants: SNS may use the key for this topic only.
	livetest.GrantServices(t, cfg, key, kmspkg.ServiceGrants{SNSTopicARNs: []string{f.topicARN}})
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: livetest.SharedKeyName}},
	}}

	_, err := Ensure(ctx, f.sqsClient, nil, nil, nil, namespace, crName, "uid-1", spec, []depsv1alpha1.ManagedResource{key}, nil, nil)
	t.Cleanup(func() { deleteQueueIfExists(t, f.sqsClient, namespace, crName, "orders") })
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := f.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}
	attrsOut, err := f.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: urlOut.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes() error = %v", err)
	}
	queueARN := attrsOut.Attributes[string(types.QueueAttributeNameQueueArn)]

	if _, err := EnsureSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", "orders", *urlOut.QueueUrl, queueARN,
		[]depsv1alpha1.ConsumeRef{f.ref}, nil, nil); err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}

	const message = "cloudctl-live-encrypted-delivery"
	if _, err := f.snsClient.Publish(ctx, &sns.PublishInput{TopicArn: &f.topicARN, Message: aws.String(message)}); err != nil {
		t.Fatalf("real Publish() error = %v", err)
	}
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		out, err := f.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: urlOut.QueueUrl, WaitTimeSeconds: 10, MaxNumberOfMessages: 10,
		})
		if err != nil {
			t.Fatalf("real ReceiveMessage() error = %v", err)
		}
		for _, m := range out.Messages {
			if strings.Contains(aws.ToString(m.Body), message) {
				return
			}
		}
	}
	t.Fatal("the message published to the topic never reached the encrypted queue: SNS could not use its key")
}

func TestLive_CleanupSubscriptions_RealUnsubscribeAndPolicyRevoke(t *testing.T) {
	ctx := context.Background()
	f := setupLiveSubscriptionFixture(t)

	ledger, err := EnsureSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL, f.queueARN,
		[]depsv1alpha1.ConsumeRef{f.ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() (setup) error = %v", err)
	}

	// subscribesTo is now empty - the ref was removed from spec.
	ledger, err = CleanupSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL,
		nil, ledger, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() error = %v", err)
	}

	ledgerName := subscriptionLedgerName(f.queueName, f.ref)
	if status.HasManagedResource(ledger, subscriptionResourceType, ledgerName) {
		t.Error("expected the ledger entry to be removed")
	}

	// Real SNS unsubscribe is asynchronous - poll briefly rather than
	// asserting immediately, the same pattern used elsewhere in this
	// tier for other eventually-consistent AWS behavior.
	var subsOut *sns.ListSubscriptionsByTopicOutput
	deadline := time.Now().Add(30 * time.Second)
	for {
		var listErr error
		subsOut, listErr = f.snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &f.topicARN})
		if listErr != nil {
			t.Fatalf("real ListSubscriptionsByTopic() error = %v", listErr)
		}
		if len(subsOut.Subscriptions) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if len(subsOut.Subscriptions) != 0 {
		t.Errorf("expected the real subscription to be gone, got %d still on the topic", len(subsOut.Subscriptions))
	}

	policyOut, err := f.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &f.queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
	})
	if err != nil {
		t.Fatalf("real GetQueueAttributes(Policy) error = %v", err)
	}
	if policy := policyOut.Attributes[string(types.QueueAttributeNamePolicy)]; policy != "" && strings.Contains(policy, f.topicARN) {
		t.Errorf("expected the queue policy grant for %q to be revoked, got: %s", f.topicARN, policy)
	}
}

// TestLive_EnsureSubscriptions_DeliversThroughATopicAndQueueThatRefuseHTTP
// puts the two transport statements on the path a message takes: the topic
// refuses plain HTTP for every SNS action and so does the queue for every SQS
// action, and SNS must still subscribe the queue and deliver into it. That
// depends on SNS reaching SQS over TLS, which only real AWS decides.
func TestLive_EnsureSubscriptions_DeliversThroughATopicAndQueueThatRefuseHTTP(t *testing.T) {
	ctx := context.Background()
	cfg := skipUnlessLiveAWSCredentials(t)
	sqsClient := sqs.NewFromConfig(cfg)
	snsClient := sns.NewFromConfig(cfg)
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	namespace, crName := "live", liveUniqueSuffix(t)

	topicSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	topicLedger, err := snspkg.Ensure(ctx, snsClient, nil, namespace, crName, "uid-1", cfg.Region, *identity.Account, topicSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("sns Ensure() error = %v", err)
	}
	topicARN := ""
	for _, entry := range topicLedger {
		if entry.Type == "sns" && entry.Name == "events" {
			topicARN = entry.ARN
		}
	}
	if topicARN == "" {
		t.Fatal("expected a ledger entry for the topic")
	}
	t.Cleanup(func() { _, _ = snsClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: &topicARN}) })
	topicAttrs, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicARN})
	if err != nil {
		t.Fatalf("real GetTopicAttributes() error = %v", err)
	}
	if !strings.Contains(topicAttrs.Attributes["Policy"], "cloudctl-deny-insecure-transport") {
		t.Fatalf("expected the topic to carry the transport statement, got %s", topicAttrs.Attributes["Policy"])
	}

	queueSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, sqsClient, namespace, crName, "orders") })
	queueLedger, err := Ensure(ctx, sqsClient, nil, nil, nil, namespace, crName, "uid-1", queueSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("sqs Ensure() error = %v", err)
	}
	queueEntry := findEntry(queueLedger, "orders")
	if queueEntry == nil {
		t.Fatal("expected a ledger entry for the queue")
	}
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}

	producer := newAuthorizedTopicProducer("team-b", "platform-service", "events", topicARN, "default", "checkout-service")
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producer).Build()
	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "events"}
	if _, err := EnsureSubscriptions(ctx, snsClient, sqsClient, k8sClient, "default", "checkout-service",
		queueName, *urlOut.QueueUrl, queueEntry.ARN, []depsv1alpha1.ConsumeRef{ref}, nil, nil); err != nil {
		t.Fatalf("EnsureSubscriptions() error = %v", err)
	}
	policy := queuePolicyText(t, sqsClient, *urlOut.QueueUrl)
	if !strings.Contains(policy, topicARN) || !strings.Contains(policy, "cloudctl-deny-insecure-transport") {
		t.Errorf("expected the queue policy to carry the topic's grant and the transport statement, got %s", policy)
	}
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)

	const message = "cloudctl-live-delivery-over-tls-only"
	if _, err := snsClient.Publish(ctx, &sns.PublishInput{TopicArn: &topicARN, Message: aws.String(message)}); err != nil {
		t.Fatalf("real Publish() to the topic over HTTPS error = %v", err)
	}
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: urlOut.QueueUrl, WaitTimeSeconds: 10, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatalf("real ReceiveMessage() error = %v", err)
		}
		for _, m := range out.Messages {
			if strings.Contains(aws.ToString(m.Body), message) {
				return
			}
		}
	}
	t.Fatal("the message never reached the queue: SNS is refused by the queue's or the topic's transport statement")
}

// TestLive_EnsureSubscriptions_TenSubscriptionsFitInTheQueuePolicy fills a
// queue policy as far as the cap on subscriptions allows: ten grants, the
// transport statement and the pending-deletion deny, which has to stay below
// the statement limit SQS enforces.
func TestLive_EnsureSubscriptions_TenSubscriptionsFitInTheQueuePolicy(t *testing.T) {
	ctx := context.Background()
	cfg := skipUnlessLiveAWSCredentials(t)
	sqsClient := sqs.NewFromConfig(cfg)
	snsClient := sns.NewFromConfig(cfg)
	namespace, crName := "live", liveUniqueSuffix(t)

	queueSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	t.Cleanup(func() { deleteQueueIfExists(t, sqsClient, namespace, crName, "orders") })
	queueLedger, err := Ensure(ctx, sqsClient, nil, nil, nil, namespace, crName, "uid-1", queueSpec, nil, nil, nil)
	if err != nil {
		t.Fatalf("sqs Ensure() error = %v", err)
	}
	queueEntry := findEntry(queueLedger, "orders")
	if queueEntry == nil {
		t.Fatal("expected a ledger entry for the queue")
	}
	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "orders", 80)
	urlOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("real GetQueueUrl() error = %v", err)
	}

	const subscriptions = 10
	refs := make([]depsv1alpha1.ConsumeRef, 0, subscriptions)
	producers := make([]client.Object, 0, subscriptions)
	for i := range subscriptions {
		topicName := shortLiveName(t, fmt.Sprintf("t%d", i))
		created, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicName})
		if err != nil {
			t.Fatalf("real CreateTopic() (setup) error = %v", err)
		}
		topicARN := *created.TopicArn
		t.Cleanup(func() { _, _ = snsClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: &topicARN}) })
		producerName := fmt.Sprintf("platform-%d", i)
		producers = append(producers, newAuthorizedTopicProducer("team-b", producerName, "events", topicARN, "default", "checkout-service"))
		refs = append(refs, depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: producerName, ResourceName: "events"})
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForKMSKeyRefTest(t)).WithObjects(producers...).Build()

	ledger, err := EnsureSubscriptions(ctx, snsClient, sqsClient, k8sClient, "default", "checkout-service",
		queueName, *urlOut.QueueUrl, queueEntry.ARN, refs, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() with %d topics error = %v", subscriptions, err)
	}
	if got := len(ledger); got != subscriptions {
		t.Errorf("expected %d subscription entries, got %d", subscriptions, got)
	}
	if err := addPendingDeletionDeny(ctx, sqsClient, *urlOut.QueueUrl, queueEntry.ARN); err != nil {
		t.Fatalf("addPendingDeletionDeny() on a queue with %d subscriptions error = %v", subscriptions, err)
	}

	doc, err := readPolicy(ctx, sqsClient, *urlOut.QueueUrl)
	if err != nil {
		t.Fatalf("readPolicy() error = %v", err)
	}
	if want := subscriptions + 2; len(doc.Statement) != want {
		t.Errorf("expected %d statements (the grants, the transport statement and the deny), got %d", want, len(doc.Statement))
	}
	policy := queuePolicyText(t, sqsClient, *urlOut.QueueUrl)
	t.Logf("a queue policy with %d subscriptions is %d bytes (SQS allows 8192)", subscriptions, len(policy))
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)
}

// TestLive_CleanupSubscriptions_UnsubscribesAfterTheQueueIsGone is the
// teardown that used to leak: the queue is deleted first, so there is no
// policy to edit, and the subscription must still be removed or SNS keeps
// failing deliveries to it. Running the cleanup a second time with the
// ledger entry still in place covers SNS answering for a subscription that no
// longer exists, which the operator counts as already removed.
func TestLive_CleanupSubscriptions_UnsubscribesAfterTheQueueIsGone(t *testing.T) {
	ctx := context.Background()
	f := setupLiveSubscriptionFixture(t)
	ledger, err := EnsureSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, f.queueURL, f.queueARN,
		[]depsv1alpha1.ConsumeRef{f.ref}, nil, nil)
	if err != nil {
		t.Fatalf("EnsureSubscriptions() (setup) error = %v", err)
	}
	ledgerName := subscriptionLedgerName(f.queueName, f.ref)
	entry, found := status.FindManagedResource(ledger, subscriptionResourceType, ledgerName)
	if !found {
		t.Fatal("expected a ledger entry for the subscription")
	}

	if _, err := f.sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: &f.queueURL}); err != nil {
		t.Fatalf("real DeleteQueue() error = %v", err)
	}
	if !waitUntil(2*time.Minute, func() bool {
		_, err := f.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &f.queueName})
		return err != nil
	}) {
		t.Fatal("the queue still resolves after being deleted")
	}

	cleaned, err := CleanupSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, "", nil, ledger, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() after the queue was deleted error = %v", err)
	}
	if status.HasManagedResource(cleaned, subscriptionResourceType, ledgerName) {
		t.Error("expected the ledger entry to be removed although the queue is gone")
	}
	if !waitUntil(time.Minute, func() bool {
		out, err := f.snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &f.topicARN})
		return err == nil && len(out.Subscriptions) == 0
	}) {
		t.Error("the subscription is still on the topic")
	}

	again, err := CleanupSubscriptions(ctx, f.snsClient, f.sqsClient, f.k8sClient,
		"default", "checkout-service", f.queueName, "", nil, []depsv1alpha1.ManagedResource{entry}, nil)
	if err != nil {
		t.Fatalf("CleanupSubscriptions() of a subscription SNS no longer has error = %v: the not-found answer is not recognised", err)
	}
	if status.HasManagedResource(again, subscriptionResourceType, ledgerName) {
		t.Error("expected the entry to be dropped when SNS no longer has the subscription")
	}
}

// withTrustExpired returns the ledger as it would be after the trust window has
// passed, so the next Ensure verifies everything instead of only what is
// checked within the window.
func withTrustExpired(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
	past := metav1.NewTime(time.Now().Add(-(status.TrustWindow + time.Minute)))
	out := append([]depsv1alpha1.ManagedResource{}, ledger...)
	for i := range out {
		out[i].LastVerifiedAt = &past
	}
	return out
}
