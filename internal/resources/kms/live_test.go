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

// Live tests run against a real AWS account - no LocalStack. Standalone
// KMS has no integration tier at all (see docs/testing.md: LocalStack's
// community edition is the least faithful for KMS, so testing against it
// there risks false confidence more than real coverage) - this is the
// only real-AWS-shaped coverage this package's own resourceType gets at
// all, beyond the dedicated-key path already exercised through
// sqs/sns/s3/dynamodb's own live tests.
//
// Unlike every other resource type, a KMS key cannot be deleted
// immediately - ScheduleKeyDeletion's PendingWindowInDays has a hard AWS
// floor of 7 days. Every test here ends with a real key on a real
// 7-day countdown as a result, scheduled directly rather than through
// this package's own Cleanup - the same pattern the dedicated-key path's
// own live test already uses (sqs/live_test.go's
// TestLive_Ensure_DedicatedKMSKeyEncryptsRealQueue). Cleanup's own
// ScheduleKeyDeletion call (and its deliberate 30-day window - see
// cleanup.go's own doc comment) is pure Go logic plus the same already-
// proven API call, so it's left to the unit tier's fakes rather than
// re-verified here for real: the only thing a live run could catch that
// the dedicated-key test hasn't already proven is the literal window
// value, which isn't a class of bug this tier exists to catch. Run
// explicitly with whatever already authenticates your AWS CLI:
//
//	go test -tags=live ./internal/resources/kms/... -v
package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/policycheck"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with
// a genuine, harmless STS call.
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

func newLiveClient(t *testing.T) *kms.Client {
	t.Helper()
	return kms.NewFromConfig(skipUnlessLiveAWSCredentials(t))
}

// liveUniqueSuffix keeps each test's key alias distinct so repeated runs
// against the same real account never collide.
func liveUniqueSuffix(t *testing.T) string {
	return "live-" + t.Name()[len("TestLive_"):] + "-" + time.Now().UTC().Format("150405")
}

// deleteRealKeyDirectly schedules a real key's deletion at AWS's 7-day
// minimum, bypassing this package's own Cleanup entirely - the same
// pattern sqs/live_test.go's dedicated-key test already uses, for the
// same reason: deletion here is just cleanup, not the thing under test,
// so there's no reason to pay this package's own 30-day policy for it.
func deleteRealKeyDirectly(t *testing.T, client *kms.Client, keyARN string) {
	t.Helper()
	windowDays := int32(7)
	if _, err := client.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyId: &keyARN, PendingWindowInDays: &windowDays,
	}); err != nil {
		t.Logf("key cleanup warning: %v", err)
	}
}

func keyPolicyText(t *testing.T, client *kms.Client, keyARN string) string {
	t.Helper()
	out, err := client.GetKeyPolicy(context.Background(), &kms.GetKeyPolicyInput{KeyId: &keyARN, PolicyName: aws.String(keyPolicyName)})
	if err != nil {
		t.Fatalf("real GetKeyPolicy() error = %v", err)
	}
	return aws.ToString(out.Policy)
}

// waitForKeyPolicy polls until check accepts the key's policy: KMS applies a
// policy change eventually.
func waitForKeyPolicy(t *testing.T, client *kms.Client, keyARN string, check func(policy string) bool) string {
	t.Helper()
	var policy string
	deadline := time.Now().Add(time.Minute)
	for {
		policy = keyPolicyText(t, client, keyARN)
		if check(policy) || time.Now().After(deadline) {
			return policy
		}
		time.Sleep(3 * time.Second)
	}
}

// countingKMS counts the key policy writes SyncServiceGrants makes.
type countingKMS struct {
	kmsAPI
	puts int
}

func (c *countingKMS) PutKeyPolicy(ctx context.Context, in *kms.PutKeyPolicyInput, optFns ...func(*kms.Options)) (*kms.PutKeyPolicyOutput, error) {
	c.puts++
	return c.kmsAPI.PutKeyPolicy(ctx, in, optFns...)
}

const (
	rootSid    = "Enable IAM User Permissions"
	foreignSid = "SomeoneElse"

	// scratchKeyAlias names the one reusable key the tests that rewrite a key's
	// policy, tags or aliases work on, so they don't each leave a key pending
	// deletion.
	scratchKeyAlias = "alias/live-scratch-key-3f9a1c"
)

// scratchKey returns the reusable scratch key, created on its first use and
// found by its alias afterwards. When the test ends it is put back as it was
// found: its original policy, no tags and no other alias. It is never scheduled
// for deletion; schedule it by hand when the live tests are retired.
func scratchKey(t *testing.T, client *kms.Client) string {
	t.Helper()
	ctx := context.Background()
	found, findErr := client.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(scratchKeyAlias)})
	var notFound *types.NotFoundException
	var arn string
	switch {
	case findErr == nil && found.KeyMetadata.KeyState == types.KeyStateEnabled:
		arn = aws.ToString(found.KeyMetadata.Arn)
	case findErr == nil && reviveScratchKey(t, client, found.KeyMetadata):
		arn = aws.ToString(found.KeyMetadata.Arn)
	case findErr == nil || errors.As(findErr, &notFound):
		created, err := client.CreateKey(ctx, &kms.CreateKeyInput{Description: aws.String("cloudctl live tests: scratch key")})
		if err != nil {
			t.Fatalf("real CreateKey() (scratch key) error = %v", err)
		}
		arn = aws.ToString(created.KeyMetadata.Arn)
		if findErr == nil {
			_, err = client.UpdateAlias(ctx, &kms.UpdateAliasInput{AliasName: aws.String(scratchKeyAlias), TargetKeyId: &arn})
		} else {
			_, err = client.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String(scratchKeyAlias), TargetKeyId: &arn})
		}
		if err != nil {
			t.Fatalf("pointing %s at the scratch key: %v", scratchKeyAlias, err)
		}
	default:
		t.Fatalf("real DescribeKey(%s) error = %v", scratchKeyAlias, findErr)
	}
	original := keyPolicyText(t, client, arn)
	t.Cleanup(func() { restoreScratchKey(client, arn, original) })
	return arn
}

// reviveScratchKey brings back a scratch key that was scheduled for deletion
// or disabled, so a run after the key was retired reuses it instead of
// creating another. It reports false for any other state.
func reviveScratchKey(t *testing.T, client *kms.Client, meta *types.KeyMetadata) bool {
	t.Helper()
	ctx := context.Background()
	switch meta.KeyState {
	case types.KeyStatePendingDeletion:
		if _, err := client.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{KeyId: meta.Arn}); err != nil {
			t.Fatalf("real CancelKeyDeletion() (scratch key) error = %v", err)
		}
	case types.KeyStateDisabled:
	default:
		return false
	}
	if _, err := client.EnableKey(ctx, &kms.EnableKeyInput{KeyId: meta.Arn}); err != nil {
		t.Fatalf("real EnableKey() (scratch key) error = %v", err)
	}
	return true
}

// restoreScratchKey is best effort: a failure leaves the key as the test left
// it, which the next test's own assertions would show.
func restoreScratchKey(client *kms.Client, arn, policy string) {
	ctx := context.Background()
	_, _ = client.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{KeyId: &arn, PolicyName: aws.String(keyPolicyName), Policy: &policy})
	if aliases, err := client.ListAliases(ctx, &kms.ListAliasesInput{KeyId: &arn}); err == nil {
		for _, a := range aliases.Aliases {
			if name := aws.ToString(a.AliasName); name != scratchKeyAlias {
				_, _ = client.DeleteAlias(ctx, &kms.DeleteAliasInput{AliasName: &name})
			}
		}
	}
	if tags, err := client.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: &arn}); err == nil && len(tags.Tags) > 0 {
		keys := make([]string, len(tags.Tags))
		for i, tag := range tags.Tags {
			keys[i] = aws.ToString(tag.TagKey)
		}
		_, _ = client.UntagResource(ctx, &kms.UntagResourceInput{KeyId: &arn, TagKeys: keys})
	}
}

func requireIn(t *testing.T, policy string, wanted ...string) {
	t.Helper()
	for _, want := range wanted {
		if !strings.Contains(policy, want) {
			t.Errorf("expected %q in the key policy, got %s", want, policy)
		}
	}
}

func requireNotIn(t *testing.T, policy string, unwanted ...string) {
	t.Helper()
	for _, not := range unwanted {
		if strings.Contains(policy, not) {
			t.Errorf("did not expect %q in the key policy, got %s", not, policy)
		}
	}
}

// putLegacyKeyPolicy leaves the key as earlier releases did: service
// statements any topic or alarm in the account satisfies, and a statement of
// someone else's, next to the account root statement.
func putLegacyKeyPolicy(t *testing.T, client *kms.Client, keyARN, account string) {
	t.Helper()
	doc := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(keyPolicyText(t, client, keyARN)), &doc); err != nil {
		t.Fatalf("parsing the key's policy: %v", err)
	}
	statements, err := statementList(doc["Statement"])
	if err != nil {
		t.Fatalf("reading the key's statements: %v", err)
	}
	accountWide := func(sid, service string) json.RawMessage {
		return json.RawMessage(fmt.Sprintf(
			`{"Sid":%q,"Effect":"Allow","Principal":{"Service":%q},"Action":["kms:GenerateDataKey*","kms:Decrypt"],"Resource":"*","Condition":{"StringEquals":{"aws:SourceAccount":%q}}}`,
			sid, service, account))
	}
	foreign := json.RawMessage(fmt.Sprintf(
		`{"Sid":%q,"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::%s:root"},"Action":"kms:DescribeKey","Resource":"*"}`,
		foreignSid, account))
	statements = append(statements, accountWide(sidSNSDelivery, "sns.amazonaws.com"), accountWide(sidCloudWatchAlarms, "cloudwatch.amazonaws.com"), foreign)
	if doc["Statement"], err = json.Marshal(statements); err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	policy := string(legacy)
	if _, err := client.PutKeyPolicy(context.Background(), &kms.PutKeyPolicyInput{KeyId: &keyARN, PolicyName: aws.String(keyPolicyName), Policy: &policy}); err != nil {
		t.Fatalf("real PutKeyPolicy() (setup, legacy statements) error = %v", err)
	}
}

func TestLive_Ensure_CreatesRealKeyWithAliasTagsAndRotation(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: "primary", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	ledger, err := Ensure(ctx, client, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	entry, entryFound := status.FindManagedResource(ledger, resourceType, "primary")
	if !entryFound {
		t.Fatal("expected a ledger entry for the created key")
	}
	t.Cleanup(func() { deleteRealKeyDirectly(t, client, entry.ARN) })

	wantAlias := aliasName(namespace, crName, "primary", keyOptions{})
	aliasOut, err := client.ListAliases(ctx, &kms.ListAliasesInput{KeyId: &entry.ARN})
	if err != nil {
		t.Fatalf("real ListAliases() error = %v", err)
	}
	found := false
	for _, a := range aliasOut.Aliases {
		if aws.ToString(a.AliasName) == wantAlias {
			found = true
		}
	}
	if !found {
		t.Errorf("expected real alias %q to point at the created key", wantAlias)
	}

	tagsOut, err := client.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: &entry.ARN})
	if err != nil {
		t.Fatalf("real ListResourceTags() error = %v", err)
	}
	tags := make(map[string]string, len(tagsOut.Tags))
	for _, tag := range tagsOut.Tags {
		tags[aws.ToString(tag.TagKey)] = aws.ToString(tag.TagValue)
	}
	if !cloudctlaws.IsOwnedBy(tags, namespace, crName, "uid-1") {
		t.Errorf("real key tags don't satisfy IsOwnedBy: %+v", tags)
	}

	rotationOut, err := client.GetKeyRotationStatus(ctx, &kms.GetKeyRotationStatusInput{KeyId: &entry.ARN})
	if err != nil {
		t.Fatalf("real GetKeyRotationStatus() error = %v", err)
	}
	if !rotationOut.KeyRotationEnabled {
		t.Error("expected automatic key rotation to be enabled on a newly created real key")
	}

	// A new key carries the account root statement only: which services may
	// use it depends on what is encrypted with it, decided later.
	policy := keyPolicyText(t, client, entry.ARN)
	if strings.Contains(policy, sidSNSDelivery) || strings.Contains(policy, sidCloudWatchAlarms) {
		t.Errorf("a new key was given service statements: %s", policy)
	}
	policycheck.ValidatePolicy(t, skipUnlessLiveAWSCredentials(t), policycheck.ResourcePolicy, policycheck.NoResourceType, policy)
}

func TestLive_Ensure_AdoptsRealUntaggedKeyWhenRequested(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)

	keyARN := scratchKey(t, client)
	wantAlias := aliasName(namespace, crName, "primary", keyOptions{})
	if _, err := client.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: &wantAlias, TargetKeyId: &keyARN}); err != nil {
		t.Fatalf("real CreateAlias() (setup, unowned) error = %v", err)
	}

	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: "primary", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}
	if _, err := Ensure(ctx, client, namespace, crName, "uid-1", spec, nil, nil, nil); err == nil {
		t.Fatal("expected Ensure to refuse a real untagged key/alias without adopt:true")
	}

	policyBefore := keyPolicyText(t, client, keyARN)
	spec.Resources[0].Adopt = true
	if _, err := Ensure(ctx, client, namespace, crName, "uid-1", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() with adopt:true error = %v", err)
	}
	if policyAfter := keyPolicyText(t, client, keyARN); policyAfter != policyBefore {
		t.Errorf("adopting a key changed its policy:\nbefore %s\nafter  %s", policyBefore, policyAfter)
	}

	tagsOut, err := client.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: &keyARN})
	if err != nil {
		t.Fatalf("real ListResourceTags() error = %v", err)
	}
	tags := make(map[string]string, len(tagsOut.Tags))
	for _, tag := range tagsOut.Tags {
		tags[aws.ToString(tag.TagKey)] = aws.ToString(tag.TagValue)
	}
	if !cloudctlaws.IsOwnedBy(tags, namespace, crName, "uid-1") {
		t.Errorf("expected the real key to be tagged as owned after adoption, got: %+v", tags)
	}
}

// TestLive_Ensure_TakesOverAKeyFromAPreviousUID moves the shared live-test
// key to another UID and back, so no key is created for it. The key is put
// back even if an assertion fails.
func TestLive_Ensure_TakesOverAKeyFromAPreviousUID(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	const namespace, crName, sharedUID = "live", "shared-key", "live-shared-key"
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: "shared", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true},
	}}
	ledger, err := Ensure(ctx, client, namespace, crName, sharedUID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() (shared key) error = %v", err)
	}
	entry, _ := status.FindManagedResource(ledger, resourceType, "shared")
	t.Cleanup(func() {
		back := cloudctlaws.WithAdoptFromUID(context.Background(), "uid-2")
		if _, err := Ensure(back, client, namespace, crName, sharedUID, spec, nil, nil, nil); err != nil {
			t.Logf("could not give the shared key back to its UID, run the KMS Ensure with adopt-from-uid=uid-2: %v", err)
		}
	})

	strict := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "shared", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain}}}
	if _, err := Ensure(ctx, client, namespace, crName, "uid-2", strict, nil, nil, nil); err == nil {
		t.Fatal("expected a new UID to be refused without adopt-from-uid")
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, sharedUID)
	if _, err := Ensure(takeover, client, namespace, crName, "uid-2", strict, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	tags, err := listAllResourceTags(ctx, client, entry.ARN)
	if err != nil {
		t.Fatalf("listAllResourceTags() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, "uid-2") {
		t.Errorf("expected the key to be tagged with the new UID, got %+v", tags)
	}
}

// TestLive_Cleanup_ReleasingAKeyLeavesProvenanceAndAdoptingTakesItBack lets go
// of the shared live-test key and takes it back, so no key is created for it.
// The key is adopted again even if an assertion fails.
func TestLive_Cleanup_ReleasingAKeyLeavesProvenanceAndAdoptingTakesItBack(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	const namespace, crName, uid = "live", "shared-key", "live-shared-key"
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: "shared", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true},
	}}
	ledger, err := Ensure(ctx, client, namespace, crName, uid, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() (shared key) error = %v", err)
	}
	entry, _ := status.FindManagedResource(ledger, resourceType, "shared")
	t.Cleanup(func() {
		if _, err := Ensure(context.Background(), client, namespace, crName, uid, spec, nil, nil, nil); err != nil {
			t.Logf("could not adopt the shared key again: %v", err)
		}
	})

	relinquished, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, uid, entry, nil)
	if err != nil || gone || !relinquished {
		t.Fatalf("relinquishIfStillTagged() relinquished=%v gone=%v err=%v", relinquished, gone, err)
	}
	tags, err := listAllResourceTags(ctx, client, entry.ARN)
	if err != nil {
		t.Fatalf("listAllResourceTags() error = %v", err)
	}
	got := tagsToMap(tags)
	if _, owned := got[cloudctlaws.OwnerTagKey]; owned || got[cloudctlaws.ReleasedFromTagKey] != namespace+"/"+crName {
		t.Fatalf("expected the ownership tags gone and released-from left, got %v", got)
	}

	if _, err := Ensure(ctx, client, namespace, crName, uid, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() with adopt error = %v", err)
	}
	tags, err = listAllResourceTags(ctx, client, entry.ARN)
	if err != nil {
		t.Fatalf("listAllResourceTags() error = %v", err)
	}
	got = tagsToMap(tags)
	if cloudctlaws.HasReleaseTags(got) || !cloudctlaws.IsOwnedBy(got, namespace, crName, uid) {
		t.Errorf("expected ownership back and the released tags gone, got %v", got)
	}
}

// TestLive_SyncServiceGrants_NarrowsRetiresAndLeavesOthersStatementsAlone
// starts from a key as earlier releases left it and confirms, against real KMS,
// that the scoped statements are accepted and pass Access Analyzer, that the
// account-wide ones are narrowed or retired, that nothing else in the policy is
// touched, that a pass with nothing to change writes nothing (KMS hands the
// policy back reformatted), and that taking the grants away removes the
// operator's statements only.
func TestLive_SyncServiceGrants_NarrowsRetiresAndLeavesOthersStatementsAlone(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := kms.NewFromConfig(cfg)
	ctx := context.Background()
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	account, region := aws.ToString(identity.Account), cfg.Region
	keyARN := scratchKey(t, client)
	putLegacyKeyPolicy(t, client, keyARN, account)

	topics := []string{"arn:aws:sns:" + region + ":" + account + ":live-b", "arn:aws:sns:" + region + ":" + account + ":live-a"}
	alarm := "arn:aws:cloudwatch:" + region + ":" + account + ":alarm:live-alarm"
	recordEvent, events := newEventCollector()
	counted := &countingKMS{kmsAPI: client}
	sync := func(grants ServiceGrants) {
		t.Helper()
		if err := SyncServiceGrants(ctx, counted, keyARN, grants, recordEvent); err != nil {
			t.Fatalf("SyncServiceGrants(%+v) error = %v", grants, err)
		}
	}

	sync(ServiceGrants{SNSTopicARNs: topics})
	policy := waitForKeyPolicy(t, client, keyARN, func(p string) bool { return !strings.Contains(p, sidCloudWatchAlarms) })
	requireIn(t, policy, foreignSid, rootSid, sidSNSDelivery, "ArnEquals", topics[0], topics[1])
	requireNotIn(t, policy, sidCloudWatchAlarms)
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)
	if len(*events) != 1 || (*events)[0].reason != "KeyPolicyUpdated" {
		t.Errorf("expected one KeyPolicyUpdated event, got %v", *events)
	}

	writes := counted.puts
	sync(ServiceGrants{SNSTopicARNs: topics})
	if counted.puts != writes {
		t.Errorf("a pass with nothing to change wrote the key policy %d more time(s): the stored policy reads as different from the desired one", counted.puts-writes)
	}

	sync(ServiceGrants{SNSTopicARNs: topics, AlarmARNs: []string{alarm}})
	policy = waitForKeyPolicy(t, client, keyARN, func(p string) bool { return strings.Contains(p, sidCloudWatchAlarms) })
	requireIn(t, policy, alarm, sidSNSDelivery)
	policycheck.ValidatePolicy(t, cfg, policycheck.ResourcePolicy, policycheck.NoResourceType, policy)

	sync(ServiceGrants{})
	policy = waitForKeyPolicy(t, client, keyARN, func(p string) bool { return !strings.Contains(p, sidSNSDelivery) })
	requireNotIn(t, policy, sidSNSDelivery, sidCloudWatchAlarms)
	requireIn(t, policy, foreignSid, rootSid)
}
