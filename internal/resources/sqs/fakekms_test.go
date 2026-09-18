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

package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

// fakeKMSKey and fakeKMSClient are a minimal in-memory stand-in for the
// real KMS client, implementing cloudctlaws.KMSClient, so this package's
// own encryption.enabled wiring (calling into kms.EnsureDedicatedKey) can
// be tested without hitting real AWS. Detailed KMS behavior itself is
// already covered by internal/resources/kms's own unit tests; this fake
// only needs to be complete enough to exercise the dedicated-key call.
type fakeKMSKey struct {
	arn      string
	keyID    string
	tags     map[string]string
	keyState types.KeyState
}

type fakeKMSClient struct {
	keys    map[string]*fakeKMSKey // keyed by ARN
	aliases map[string]string      // alias name -> ARN

	nextKeyNum int
}

func newFakeKMSClient() *fakeKMSClient {
	return &fakeKMSClient{keys: map[string]*fakeKMSKey{}, aliases: map[string]string{}}
}

func (f *fakeKMSClient) resolve(keyID string) string {
	if _, ok := f.keys[keyID]; ok {
		return keyID
	}
	if arn, ok := f.aliases[keyID]; ok {
		return arn
	}
	for arn, k := range f.keys {
		if k.keyID == keyID {
			return arn
		}
	}
	return ""
}

func (f *fakeKMSClient) DescribeKey(_ context.Context, in *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	k := f.keys[arn]
	keyID, keyArn, state := k.keyID, k.arn, k.keyState
	return &kms.DescribeKeyOutput{KeyMetadata: &types.KeyMetadata{KeyId: &keyID, Arn: &keyArn, KeyState: state}}, nil
}

func (f *fakeKMSClient) CreateKey(_ context.Context, in *kms.CreateKeyInput, _ ...func(*kms.Options)) (*kms.CreateKeyOutput, error) {
	f.nextKeyNum++
	id := fmt.Sprintf("11111111-1111-1111-1111-%012d", f.nextKeyNum)
	arn := "arn:aws:kms:us-east-1:123456789012:key/" + id
	f.keys[arn] = &fakeKMSKey{arn: arn, keyID: id, tags: tagsFromKMSSlice(in.Tags), keyState: types.KeyStateEnabled}
	keyID, keyArn := id, arn
	return &kms.CreateKeyOutput{KeyMetadata: &types.KeyMetadata{KeyId: &keyID, Arn: &keyArn, KeyState: types.KeyStateEnabled}}, nil
}

func (f *fakeKMSClient) CreateAlias(_ context.Context, in *kms.CreateAliasInput, _ ...func(*kms.Options)) (*kms.CreateAliasOutput, error) {
	if _, exists := f.aliases[*in.AliasName]; exists {
		return nil, &types.AlreadyExistsException{}
	}
	arn := f.resolve(*in.TargetKeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.aliases[*in.AliasName] = arn
	return &kms.CreateAliasOutput{}, nil
}

func (f *fakeKMSClient) EnableKeyRotation(_ context.Context, in *kms.EnableKeyRotationInput, _ ...func(*kms.Options)) (*kms.EnableKeyRotationOutput, error) {
	if f.resolve(*in.KeyId) == "" {
		return nil, &types.NotFoundException{}
	}
	return &kms.EnableKeyRotationOutput{}, nil
}

func (f *fakeKMSClient) ListResourceTags(_ context.Context, in *kms.ListResourceTagsInput, _ ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error) {
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	return &kms.ListResourceTagsOutput{Tags: tagsToKMSSlice(f.keys[arn].tags)}, nil
}

func (f *fakeKMSClient) TagResource(_ context.Context, in *kms.TagResourceInput, _ ...func(*kms.Options)) (*kms.TagResourceOutput, error) {
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	k := f.keys[arn]
	if k.tags == nil {
		k.tags = map[string]string{}
	}
	for tk, tv := range tagsFromKMSSlice(in.Tags) {
		k.tags[tk] = tv
	}
	return &kms.TagResourceOutput{}, nil
}

func (f *fakeKMSClient) UntagResource(_ context.Context, in *kms.UntagResourceInput, _ ...func(*kms.Options)) (*kms.UntagResourceOutput, error) {
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	for _, k := range in.TagKeys {
		delete(f.keys[arn].tags, k)
	}
	return &kms.UntagResourceOutput{}, nil
}

func (f *fakeKMSClient) ScheduleKeyDeletion(_ context.Context, in *kms.ScheduleKeyDeletionInput, _ ...func(*kms.Options)) (*kms.ScheduleKeyDeletionOutput, error) {
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.keys[arn].keyState = types.KeyStatePendingDeletion
	return &kms.ScheduleKeyDeletionOutput{}, nil
}

func (f *fakeKMSClient) CancelKeyDeletion(_ context.Context, in *kms.CancelKeyDeletionInput, _ ...func(*kms.Options)) (*kms.CancelKeyDeletionOutput, error) {
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.keys[arn].keyState = types.KeyStateEnabled
	return &kms.CancelKeyDeletionOutput{}, nil
}

func tagsFromKMSSlice(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.TagKey != nil && t.TagValue != nil {
			m[*t.TagKey] = *t.TagValue
		}
	}
	return m
}

func tagsToKMSSlice(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{TagKey: &k, TagValue: &v})
	}
	return tags
}
