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

// Live tests run against a real AWS account - no LocalStack, no Kubernetes
// cluster. Kept deliberately lean: unlike sqs's live tier, this one skips
// plain CRUD (create/tag/adopt) and the dedicated-KMS-key path (already
// exercised against real AWS via sqs's own live test, sharing the exact
// same kms package code with only a different resourceType string - it
// would confirm nothing new here). What's specific to this package and
// worth the real-AWS cost: TopicARN is hand-constructed rather than
// resolved from an API response (SNS has no "get topic by name"), the FIFO
// attribute names, and the policy-based deny mechanism's action strings and
// empty-string-clears-policy behavior - the same class of bug already found
// once in sqs's own live tier (sqs:SendMessageBatch not being a real
// action). Skipped entirely unless real credentials resolve via the
// standard AWS credential chain. Run explicitly with whatever already
// authenticates your AWS CLI:
//
//	go test -tags=live ./internal/resources/sns/... -v
package sns

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/livetest"
	"github.com/Ningendo7/cloudctl-operator/internal/policycheck"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with a
// genuine, harmless STS call.
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

// liveRegionAndAccount resolves the real region/account this test is
// running against - TopicARN needs both to construct the same ARN AWS
// itself would return for a topic in this account.
func liveRegionAndAccount(t *testing.T, cfg aws.Config) (region, accountID string) {
	t.Helper()
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	return cfg.Region, *out.Account
}

// liveUniqueSuffix keeps each test's topic name distinct so repeated runs
// against the same real account never collide.
func liveUniqueSuffix(t *testing.T) string {
	return "live-" + t.Name()[len("TestLive_"):] + "-" + time.Now().UTC().Format("150405")
}

func deleteTopicIfExists(t *testing.T, client *sns.Client, topicArn string) {
	t.Helper()
	_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: &topicArn})
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

// TestLive_Ensure_CreatesRealTopicWithConstructedARNMatchingReal guards the
// single riskiest assumption unique to this package: unlike every other
// resource type, SNS has no name-to-ARN lookup API, so ensureTopic hand
// -builds the ARN it later uses for every subsequent GetTopicAttributes /
// ListTagsForResource / SetTopicAttributes call. If the constructed format
// ever drifted from what AWS actually returns, every real reconcile past
// the first would silently target a non-existent ARN.
func TestLive_Ensure_CreatesRealTopicWithConstructedARNMatchingReal(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	wantArn := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, wantArn) })

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := findEntry(ledger, "events")
	if entry == nil {
		t.Fatal("expected a ledger entry for events")
	}
	if entry.ARN != wantArn {
		t.Errorf("ledger ARN = %q, want the constructed ARN %q", entry.ARN, wantArn)
	}

	attrsOut, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &wantArn})
	if err != nil {
		t.Fatalf("real GetTopicAttributes() against the constructed ARN error = %v — the constructed ARN doesn't match a real topic", err)
	}
	if attrsOut.Attributes["TopicArn"] != wantArn {
		t.Errorf("real topic's own reported ARN = %q, want %q", attrsOut.Attributes["TopicArn"], wantArn)
	}

	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &wantArn})
	if err != nil {
		t.Fatalf("real ListTagsForResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.Tags), namespace, crName, "uid-1") {
		t.Errorf("real topic tags don't satisfy IsOwnedBy: %+v", tagsOut.Tags)
	}
}

// TestLive_Ensure_FIFOTopicAcceptsFifoAndDedupAttributes confirms
// "FifoTopic" and "ContentBasedDeduplication" are attribute names/values
// AWS actually accepts on a real CreateTopic call - a wrong attribute
// string here would silently create a plain topic under a ".fifo"-suffixed
// name rather than a real FIFO one, or be rejected outright.
func TestLive_Ensure_FIFOTopicAcceptsFifoAndDedupAttributes(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	dedup := true
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{
			Name:           "events",
			FIFO:           true,
			DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
			Force:          true,
			Overrides:      &depsv1alpha1.SNSOverrides{ContentBasedDeduplication: &dedup},
		},
	}}
	topicArn := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256-len(".fifo"))+".fifo")
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	ledger, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	entry := findEntry(ledger, "events")
	if entry == nil {
		t.Fatal("expected a ledger entry for the FIFO topic")
	}
	if entry.ARN != topicArn {
		t.Fatalf("ledger ARN = %q, want %q — real AWS didn't accept/return the .fifo-suffixed name the way Ensure assumes", entry.ARN, topicArn)
	}

	attrsOut, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn})
	if err != nil {
		t.Fatalf("real GetTopicAttributes() error = %v", err)
	}
	if attrsOut.Attributes["FifoTopic"] != "true" {
		t.Errorf("real FifoTopic attribute = %q, want true", attrsOut.Attributes["FifoTopic"])
	}
	if attrsOut.Attributes["ContentBasedDeduplication"] != "true" {
		t.Errorf("real ContentBasedDeduplication attribute = %q, want true", attrsOut.Attributes["ContentBasedDeduplication"])
	}
}

// TestLive_Cleanup_DenyPolicyActuallyBlocksPublishThenClearingRestoresIt
// exercises the exact class of bug already found once in sqs's own live
// tier (sqs:SendMessageBatch turning out not to be a real action): calls
// the unexported addPendingDeletionDeny/removePendingDeletionDeny directly
// against a real topic, confirms AWS actually enforces the deny (not just
// silently accepts the policy JSON), and confirms SetTopicAttributes with
// an empty-string Policy value genuinely clears it rather than erroring or
// leaving the topic in an ambiguous policy state - a behavior AWS's own
// docs don't spell out.
func TestLive_Cleanup_DenyPolicyActuallyBlocksPublishThenClearingRestoresIt(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	topicArn := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	if _, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	msg := "hello"
	if _, err := client.Publish(ctx, &sns.PublishInput{TopicArn: &topicArn, Message: &msg}); err != nil {
		t.Fatalf("real Publish() before any deny policy error = %v — expected it to succeed", err)
	}

	if err := addPendingDeletionDeny(ctx, client, topicArn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	_, pubErr := client.Publish(ctx, &sns.PublishInput{TopicArn: &topicArn, Message: &msg})
	var authErr *types.AuthorizationErrorException
	if !errors.As(pubErr, &authErr) {
		t.Fatalf("expected a real AuthorizationErrorException from Publish after denying it, got %v", pubErr)
	}

	if err := removePendingDeletionDeny(ctx, client, topicArn); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}
	if _, err := client.Publish(ctx, &sns.PublishInput{TopicArn: &topicArn, Message: &msg}); err != nil {
		t.Errorf("real Publish() after clearing the deny policy error = %v — expected the empty-string Policy attribute to actually clear it", err)
	}
}

// TestLive_Ensure_RefusesTopicOwnedByADifferentRealCR confirms the
// tag-based stale-UID/ownership check against real, round-tripped AWS
// tags read via SNS's own ListTagsForResource shape (a []Tag slice, not
// SQS's map).
func TestLive_Ensure_RefusesTopicOwnedByADifferentRealCR(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	topicArn := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	ownerSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	if _, err := Ensure(ctx, client, nil, namespace, crName, "owner-uid", region, accountID, ownerSpec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (establishing the real owner) error = %v", err)
	}

	intruderSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, namespace, crName, "intruder-uid", region, accountID, intruderSpec, nil, nil, nil); err == nil {
		t.Error("expected Ensure to refuse a real topic already owned by a different CR's UID, even with adopt:true")
	}
}

// TestLive_Cleanup_RetainedTopicIsLetGoAndAMissingOneIsGone checks that
// letting go removes the tags and the entry, and that a missing topic is
// recognized as gone.
func TestLive_Cleanup_RetainedTopicIsLetGoAndAMissingOneIsGone(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	topicARN := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicARN) })

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}}}
	ledger, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	ledger, results, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 1 || results[0].Reason != CleanupReasonRetained {
		t.Errorf("expected one Retained result, got %+v", results)
	}
	if findEntry(ledger, "events") != nil {
		t.Error("expected the retained topic to leave the ledger")
	}

	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &topicARN})
	if err != nil {
		t.Fatalf("the retained topic should still exist: %v", err)
	}
	if got := tagsToMap(tagsOut.Tags); cloudctlaws.IsOwnedBy(got, namespace, crName, "uid-1") || got[cloudctlaws.ReleasedFromTagKey] != namespace+"/"+crName {
		t.Errorf("expected the ownership tags gone and released-from left, tags: %+v", tagsOut.Tags)
	}

	missing := depsv1alpha1.ManagedResource{Type: resourceType, Name: "never-created", ARN: cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "never-created", 256))}
	_, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, "uid-1", missing, nil)
	if err != nil || !gone {
		t.Errorf("relinquishIfStillTagged() on a missing topic gone=%v err=%v, want gone", gone, err)
	}
}

// TestLive_Ensure_TakesOverATopicFromAPreviousUID checks a restored CR is
// refused a topic tagged with its old UID, and takes it over when told which.
func TestLive_Ensure_TakesOverATopicFromAPreviousUID(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	topicARN := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicARN) })
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true}}}

	if _, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}
	if _, err := Ensure(ctx, client, nil, namespace, crName, "uid-2", region, accountID, spec, nil, nil, nil); err == nil {
		t.Fatal("expected a new UID to be refused without adopt-from-uid")
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, "uid-1")
	if _, err := Ensure(takeover, client, nil, namespace, crName, "uid-2", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &topicARN})
	if err != nil {
		t.Fatalf("ListTagsForResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tagsOut.Tags), namespace, crName, "uid-2") {
		t.Errorf("expected the topic to be tagged with the new UID, got %+v", tagsOut.Tags)
	}
}

// TestLive_Ensure_AdoptingAgainRemovesOnlyTheReleasedTags checks against real
// SNS that taking a released topic back removes the two released tags and
// leaves someone else's tag alone.
func TestLive_Ensure_AdoptingAgainRemovesOnlyTheReleasedTags(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	topicARN := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicARN) })
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}}}

	ledger, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if _, err := client.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &topicARN, Tags: mapToTags(map[string]string{"team": "payments"})}); err != nil {
		t.Fatalf("TagResource() error = %v", err)
	}
	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.SNSSpec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	spec.Resources[0].Adopt = true
	if _, err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() with adopt error = %v", err)
	}
	tagsOut, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &topicARN})
	if err != nil {
		t.Fatalf("ListTagsForResource() error = %v", err)
	}
	got := tagsToMap(tagsOut.Tags)
	if cloudctlaws.HasReleaseTags(got) || got["team"] != "payments" || !cloudctlaws.IsOwnedBy(got, namespace, crName, "uid-1") {
		t.Errorf("expected ownership back, the released tags gone and the other tag kept, got %v", got)
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

// countingSNS counts the topic policy writes Ensure makes.
type countingSNS struct {
	snsAPI
	policyWrites int
}

func (c *countingSNS) SetTopicAttributes(ctx context.Context, in *sns.SetTopicAttributesInput, optFns ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
	if aws.ToString(in.AttributeName) == policyAttrName {
		c.policyWrites++
	}
	return c.snsAPI.SetTopicAttributes(ctx, in, optFns...)
}

func topicPolicyText(t *testing.T, client *sns.Client, topicArn string) string {
	t.Helper()
	out, err := client.GetTopicAttributes(context.Background(), &sns.GetTopicAttributesInput{TopicArn: &topicArn})
	if err != nil {
		t.Fatalf("real GetTopicAttributes() error = %v", err)
	}
	return out.Attributes[policyAttrName]
}

// TestLive_Ensure_TopicsRefuseHTTPAndTheStatementIsNotRewritten confirms SNS
// accepts `sns:*` as the action of the transport statement and Access
// Analyzer finds nothing wrong with it, that a publish over plain HTTP is
// refused with an error the operator classifies as a permission problem while
// HTTPS is not, that a second pass writes nothing (SNS hands the policy back
// reformatted, next to its own default statement, which must still compare
// equal), that the pending-deletion statement joins it and leaves it when
// removed, and that taking the statement away lets HTTP through, so the
// refusal was the statement's.
func TestLive_Ensure_TopicsRefuseHTTPAndTheStatementIsNotRewritten(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := sns.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	topicArn := cloudctlaws.TopicARN(region, accountID, cloudctlaws.ResourceName(namespace, crName, "sns", "events", 256))
	t.Cleanup(func() { deleteTopicIfExists(t, client, topicArn) })

	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	recordEvent, events := newEventCollector()
	counted := &countingSNS{snsAPI: client}
	ledger, err := Ensure(ctx, counted, nil, namespace, crName, "uid-1", region, accountID, spec, nil, nil, recordEvent)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("expected one TransportSecurityEnforced event, got %v", *events)
	}
	policy := topicPolicyText(t, client, topicArn)
	if got := strings.Count(policy, awspolicy.InsecureTransportSid); got != 1 {
		t.Fatalf("expected the transport statement once in the topic policy, found %d in %s", got, policy)
	}
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)

	publish := func(c *sns.Client) error {
		_, err := c.Publish(ctx, &sns.PublishInput{TopicArn: &topicArn, Message: aws.String("hello")})
		return err
	}
	if err := publish(client); err != nil {
		t.Fatalf("real Publish over HTTPS error = %v: the statement must not refuse TLS", err)
	}
	plain := sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String("http://sns." + region + ".amazonaws.com")
		o.HTTPClient = livetest.FreshConnections()
	})
	var refusal error
	if !waitUntil(3*time.Minute, func() bool {
		refusal = publish(plain)
		return refusal != nil && cloudctlaws.IsPermissionDenied(refusal)
	}) {
		t.Fatalf("a publish over plain HTTP was not refused as a permission error: %v", refusal)
	}

	writes := counted.policyWrites
	if _, err := Ensure(ctx, counted, nil, namespace, crName, "uid-1", region, accountID, spec, withTrustExpired(ledger), nil, recordEvent); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if counted.policyWrites != writes {
		t.Errorf("the second pass wrote the topic policy %d more time(s): the stored policy reads as different from the desired one", counted.policyWrites-writes)
	}

	if err := addPendingDeletionDeny(ctx, client, topicArn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	policy = topicPolicyText(t, client, topicArn)
	if !strings.Contains(policy, pendingDeletionDenySid) || !strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("expected both statements after the pending-deletion deny, got %s", policy)
	}
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)
	if err := removePendingDeletionDeny(ctx, client, topicArn); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}
	policy = topicPolicyText(t, client, topicArn)
	if strings.Contains(policy, pendingDeletionDenySid) || !strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("expected only the transport statement left, got %s", policy)
	}

	doc, err := readPolicy(ctx, client, topicArn)
	if err != nil {
		t.Fatalf("readPolicy() error = %v", err)
	}
	doc.Statement, _ = awspolicy.Remove(doc.Statement, awspolicy.InsecureTransportSid)
	if err := writePolicy(ctx, client, topicArn, doc); err != nil {
		t.Fatalf("writePolicy() without the statement error = %v", err)
	}
	if !waitUntil(3*time.Minute, func() bool { return publish(plain) == nil }) {
		t.Error("plain HTTP is still refused after the statement was removed: something else refuses it")
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
