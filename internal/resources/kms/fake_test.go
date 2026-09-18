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

package kms

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/smithy-go"
)

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (throttling, permission-denied, ...) into fakeKMS calls.
type fakeAWSError struct {
	code  string
	fault smithy.ErrorFault
}

func (e *fakeAWSError) Error() string                 { return e.code }
func (e *fakeAWSError) ErrorCode() string             { return e.code }
func (e *fakeAWSError) ErrorMessage() string          { return e.code }
func (e *fakeAWSError) ErrorFault() smithy.ErrorFault { return e.fault }

// fakeKey and fakeKMS are a minimal in-memory stand-in for the real KMS
// client, implementing just the kmsAPI methods this package calls, so
// Ensure/Cleanup can be tested without hitting real AWS.
type fakeKey struct {
	arn             string
	keyID           string
	tags            map[string]string
	keyState        types.KeyState
	rotationEnabled bool
}

type fakeKMS struct {
	keys    map[string]*fakeKey // keyed by ARN
	aliases map[string]string   // alias name -> ARN

	createKeyErr           error
	createAliasErr         error
	enableKeyRotationErr   error
	listResourceTagsErr    error
	tagResourceErr         error
	untagResourceErr       error
	scheduleKeyDeletionErr error
	cancelKeyDeletionErr   error
	describeKeyErr         error

	nextKeyNum int
}

func newFakeKMS() *fakeKMS {
	return &fakeKMS{keys: map[string]*fakeKey{}, aliases: map[string]string{}}
}

// resolve maps a KeyId input (an alias name, a key ARN, or a bare key ID)
// to the canonical ARN it refers to, or "" if nothing matches - mirroring
// how every real KMS call accepts any of these interchangeably.
func (f *fakeKMS) resolve(keyID string) string {
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

func (f *fakeKMS) DescribeKey(_ context.Context, in *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	if f.describeKeyErr != nil {
		return nil, f.describeKeyErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	k := f.keys[arn]
	keyID, keyArn, state := k.keyID, k.arn, k.keyState
	return &kms.DescribeKeyOutput{KeyMetadata: &types.KeyMetadata{KeyId: &keyID, Arn: &keyArn, KeyState: state}}, nil
}

func (f *fakeKMS) CreateKey(_ context.Context, in *kms.CreateKeyInput, _ ...func(*kms.Options)) (*kms.CreateKeyOutput, error) {
	if f.createKeyErr != nil {
		return nil, f.createKeyErr
	}
	f.nextKeyNum++
	id := fmt.Sprintf("11111111-1111-1111-1111-%012d", f.nextKeyNum)
	arn := "arn:aws:kms:us-east-1:123456789012:key/" + id
	f.keys[arn] = &fakeKey{
		arn:      arn,
		keyID:    id,
		tags:     tagsToMap(in.Tags),
		keyState: types.KeyStateEnabled,
	}
	keyID, keyArn := id, arn
	return &kms.CreateKeyOutput{KeyMetadata: &types.KeyMetadata{KeyId: &keyID, Arn: &keyArn, KeyState: types.KeyStateEnabled}}, nil
}

func (f *fakeKMS) CreateAlias(_ context.Context, in *kms.CreateAliasInput, _ ...func(*kms.Options)) (*kms.CreateAliasOutput, error) {
	if f.createAliasErr != nil {
		return nil, f.createAliasErr
	}
	if _, exists := f.aliases[*in.AliasName]; exists {
		// Real AWS always errors on a pre-existing alias name, even one
		// that already points at the requested target - never a silent
		// idempotent success.
		return nil, &types.AlreadyExistsException{}
	}
	arn := f.resolve(*in.TargetKeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.aliases[*in.AliasName] = arn
	return &kms.CreateAliasOutput{}, nil
}

func (f *fakeKMS) EnableKeyRotation(_ context.Context, in *kms.EnableKeyRotationInput, _ ...func(*kms.Options)) (*kms.EnableKeyRotationOutput, error) {
	if f.enableKeyRotationErr != nil {
		return nil, f.enableKeyRotationErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.keys[arn].rotationEnabled = true
	return &kms.EnableKeyRotationOutput{}, nil
}

func (f *fakeKMS) ListResourceTags(_ context.Context, in *kms.ListResourceTagsInput, _ ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error) {
	if f.listResourceTagsErr != nil {
		return nil, f.listResourceTagsErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	return &kms.ListResourceTagsOutput{Tags: mapToTags(f.keys[arn].tags)}, nil
}

func (f *fakeKMS) TagResource(_ context.Context, in *kms.TagResourceInput, _ ...func(*kms.Options)) (*kms.TagResourceOutput, error) {
	if f.tagResourceErr != nil {
		return nil, f.tagResourceErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	k := f.keys[arn]
	if k.tags == nil {
		k.tags = map[string]string{}
	}
	for tk, tv := range tagsToMap(in.Tags) {
		k.tags[tk] = tv
	}
	return &kms.TagResourceOutput{}, nil
}

func (f *fakeKMS) UntagResource(_ context.Context, in *kms.UntagResourceInput, _ ...func(*kms.Options)) (*kms.UntagResourceOutput, error) {
	if f.untagResourceErr != nil {
		return nil, f.untagResourceErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	for _, k := range in.TagKeys {
		delete(f.keys[arn].tags, k)
	}
	return &kms.UntagResourceOutput{}, nil
}

func (f *fakeKMS) ScheduleKeyDeletion(_ context.Context, in *kms.ScheduleKeyDeletionInput, _ ...func(*kms.Options)) (*kms.ScheduleKeyDeletionOutput, error) {
	if f.scheduleKeyDeletionErr != nil {
		return nil, f.scheduleKeyDeletionErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.keys[arn].keyState = types.KeyStatePendingDeletion
	return &kms.ScheduleKeyDeletionOutput{}, nil
}

func (f *fakeKMS) CancelKeyDeletion(_ context.Context, in *kms.CancelKeyDeletionInput, _ ...func(*kms.Options)) (*kms.CancelKeyDeletionOutput, error) {
	if f.cancelKeyDeletionErr != nil {
		return nil, f.cancelKeyDeletionErr
	}
	arn := f.resolve(*in.KeyId)
	if arn == "" {
		return nil, &types.NotFoundException{}
	}
	f.keys[arn].keyState = types.KeyStateEnabled
	return &kms.CancelKeyDeletionOutput{}, nil
}
