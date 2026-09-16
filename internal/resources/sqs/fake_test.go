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

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (throttling, permission-denied, ...) into fakeSQS calls,
// so error-classification behavior in the sqs package is actually
// exercisable in tests, not just in internal/aws's own unit tests.
type fakeAWSError struct {
	code  string
	fault smithy.ErrorFault
}

func (e *fakeAWSError) Error() string                 { return e.code }
func (e *fakeAWSError) ErrorCode() string             { return e.code }
func (e *fakeAWSError) ErrorMessage() string          { return e.code }
func (e *fakeAWSError) ErrorFault() smithy.ErrorFault { return e.fault }

// fakeQueue and fakeSQS are a minimal in-memory stand-in for the real SQS
// client, implementing just the sqsAPI methods this package calls, so
// Ensure/Cleanup can be tested without hitting real AWS or the full SDK.
type fakeQueue struct {
	url                   string
	arn                   string
	tags                  map[string]string
	approxMessages        string
	approxMessagesHidden  string
	approxMessagesDelayed string
	policy                string
	attributes            map[string]string
}

type fakeSQS struct {
	queues map[string]*fakeQueue // keyed by queue name

	// Error injection for exercising error-classification paths that
	// normal queue-state manipulation can't reach.
	createQueueErr error
	getQueueUrlErr error
}

func newFakeSQS() *fakeSQS {
	return &fakeSQS{queues: map[string]*fakeQueue{}}
}

func (f *fakeSQS) CreateQueue(_ context.Context, in *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	if f.createQueueErr != nil {
		return nil, f.createQueueErr
	}
	name := *in.QueueName
	url := "https://sqs.us-east-1.amazonaws.com/000000000000/" + name
	f.queues[name] = &fakeQueue{
		url:                  url,
		arn:                  "arn:aws:sqs:us-east-1:000000000000:" + name,
		tags:                 in.Tags,
		approxMessages:        "0",
		approxMessagesHidden:  "0",
		approxMessagesDelayed: "0",
		attributes:            in.Attributes,
	}
	return &sqs.CreateQueueOutput{QueueUrl: &url}, nil
}

func (f *fakeSQS) GetQueueUrl(_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if f.getQueueUrlErr != nil {
		return nil, f.getQueueUrlErr
	}
	q, ok := f.queues[*in.QueueName]
	if !ok {
		return nil, &types.QueueDoesNotExist{}
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: &q.url}, nil
}

func (f *fakeSQS) GetQueueAttributes(_ context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	q := f.findByURL(*in.QueueUrl)
	if q == nil {
		return nil, fmt.Errorf("queue not found: %s", *in.QueueUrl)
	}
	out := map[string]string{
		string(types.QueueAttributeNameQueueArn):                              q.arn,
		string(types.QueueAttributeNameApproximateNumberOfMessages):           q.approxMessages,
		string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible): q.approxMessagesHidden,
		string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed):    q.approxMessagesDelayed,
		string(types.QueueAttributeNamePolicy):                                q.policy,
	}
	for k, v := range q.attributes {
		out[k] = v
	}
	return &sqs.GetQueueAttributesOutput{Attributes: out}, nil
}

func (f *fakeSQS) SetQueueAttributes(_ context.Context, in *sqs.SetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	q := f.findByURL(*in.QueueUrl)
	if q == nil {
		return nil, fmt.Errorf("queue not found: %s", *in.QueueUrl)
	}
	if policy, ok := in.Attributes[string(types.QueueAttributeNamePolicy)]; ok {
		q.policy = policy
	}
	if q.attributes == nil {
		q.attributes = map[string]string{}
	}
	for k, v := range in.Attributes {
		if k == string(types.QueueAttributeNamePolicy) {
			continue
		}
		q.attributes[k] = v
	}
	return &sqs.SetQueueAttributesOutput{}, nil
}

func (f *fakeSQS) UntagQueue(_ context.Context, in *sqs.UntagQueueInput, _ ...func(*sqs.Options)) (*sqs.UntagQueueOutput, error) {
	q := f.findByURL(*in.QueueUrl)
	if q == nil {
		return nil, fmt.Errorf("queue not found: %s", *in.QueueUrl)
	}
	for _, k := range in.TagKeys {
		delete(q.tags, k)
	}
	return &sqs.UntagQueueOutput{}, nil
}

func (f *fakeSQS) TagQueue(_ context.Context, in *sqs.TagQueueInput, _ ...func(*sqs.Options)) (*sqs.TagQueueOutput, error) {
	q := f.findByURL(*in.QueueUrl)
	if q == nil {
		return nil, fmt.Errorf("queue not found: %s", *in.QueueUrl)
	}
	if q.tags == nil {
		q.tags = map[string]string{}
	}
	for k, v := range in.Tags {
		q.tags[k] = v
	}
	return &sqs.TagQueueOutput{}, nil
}

func (f *fakeSQS) ListQueueTags(_ context.Context, in *sqs.ListQueueTagsInput, _ ...func(*sqs.Options)) (*sqs.ListQueueTagsOutput, error) {
	q := f.findByURL(*in.QueueUrl)
	if q == nil {
		return nil, fmt.Errorf("queue not found: %s", *in.QueueUrl)
	}
	return &sqs.ListQueueTagsOutput{Tags: q.tags}, nil
}

func (f *fakeSQS) DeleteQueue(_ context.Context, in *sqs.DeleteQueueInput, _ ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error) {
	for name, q := range f.queues {
		if q.url == *in.QueueUrl {
			delete(f.queues, name)
			return &sqs.DeleteQueueOutput{}, nil
		}
	}
	return &sqs.DeleteQueueOutput{}, nil
}

func (f *fakeSQS) findByURL(url string) *fakeQueue {
	for _, q := range f.queues {
		if q.url == url {
			return q
		}
	}
	return nil
}
