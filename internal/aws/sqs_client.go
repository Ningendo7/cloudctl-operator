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

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSClient is the subset of the SQS client the operator uses, exported so
// it can be substituted with a fake at any layer that needs to test AWS
// interaction without a real account — both the internal/resources/sqs
// package's own unit tests and, importantly, controller-level envtest
// suites that need to fake the whole AWS backend to exercise Reconcile()
// end-to-end. A concrete *sqs.Client satisfies this interface structurally,
// so production code is unaffected.
type SQSClient interface {
	CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
	UntagQueue(ctx context.Context, in *sqs.UntagQueueInput, optFns ...func(*sqs.Options)) (*sqs.UntagQueueOutput, error)
	ListQueueTags(ctx context.Context, in *sqs.ListQueueTagsInput, optFns ...func(*sqs.Options)) (*sqs.ListQueueTagsOutput, error)
	TagQueue(ctx context.Context, in *sqs.TagQueueInput, optFns ...func(*sqs.Options)) (*sqs.TagQueueOutput, error)
	DeleteQueue(ctx context.Context, in *sqs.DeleteQueueInput, optFns ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error)
}
