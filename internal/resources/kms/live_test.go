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
// community edition has historically been the least faithful for KMS, so
// testing against it there risks false confidence more than real
// coverage) - this is the only real-AWS-shaped coverage this package's
// own resourceType gets at all, beyond the dedicated-key path already
// exercised through sqs/sns/s3/dynamodb's own live tests.
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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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

	entry := status.FindManagedResource(ledger, resourceType, "primary")
	if entry == nil {
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
}

func TestLive_Ensure_AdoptsRealUntaggedKeyWhenRequested(t *testing.T) {
	client := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{})
	if err != nil {
		t.Fatalf("real CreateKey() (setup, unowned) error = %v", err)
	}
	keyARN := aws.ToString(createOut.KeyMetadata.Arn)
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

	spec.Resources[0].Adopt = true
	if _, err := Ensure(ctx, client, namespace, crName, "uid-1", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() with adopt:true error = %v", err)
	}
	t.Cleanup(func() { deleteRealKeyDirectly(t, client, keyARN) })

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
