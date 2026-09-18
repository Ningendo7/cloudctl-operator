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

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// KMSClient is the subset of the KMS client the operator uses, exported so
// it can be substituted with a fake at any layer that needs to test AWS
// interaction without a real account — both the internal/resources/kms
// package's own unit tests and controller-level envtest suites.
type KMSClient interface {
	DescribeKey(ctx context.Context, in *kms.DescribeKeyInput, optFns ...func(*kms.Options)) (*kms.DescribeKeyOutput, error)
	CreateKey(ctx context.Context, in *kms.CreateKeyInput, optFns ...func(*kms.Options)) (*kms.CreateKeyOutput, error)
	CreateAlias(ctx context.Context, in *kms.CreateAliasInput, optFns ...func(*kms.Options)) (*kms.CreateAliasOutput, error)
	EnableKeyRotation(ctx context.Context, in *kms.EnableKeyRotationInput, optFns ...func(*kms.Options)) (*kms.EnableKeyRotationOutput, error)
	ListResourceTags(ctx context.Context, in *kms.ListResourceTagsInput, optFns ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error)
	TagResource(ctx context.Context, in *kms.TagResourceInput, optFns ...func(*kms.Options)) (*kms.TagResourceOutput, error)
	UntagResource(ctx context.Context, in *kms.UntagResourceInput, optFns ...func(*kms.Options)) (*kms.UntagResourceOutput, error)
	ScheduleKeyDeletion(ctx context.Context, in *kms.ScheduleKeyDeletionInput, optFns ...func(*kms.Options)) (*kms.ScheduleKeyDeletionOutput, error)
	CancelKeyDeletion(ctx context.Context, in *kms.CancelKeyDeletionInput, optFns ...func(*kms.Options)) (*kms.CancelKeyDeletionOutput, error)
}
