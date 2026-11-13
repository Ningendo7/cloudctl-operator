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
	"encoding/json"
	"fmt"
	"maps"
	"strconv"

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
	// notReadableAfterCreate makes the next GetQueueAttributes after a
	// CreateQueue answer QueueDoesNotExist, as SQS can for about a second.
	notReadableAfterCreate bool
	justCreated            bool

	createQueueCalls        int
	getQueueAttributesCalls int
	setQueueAttributesCalls int
	getQueueUrlCalls        int
}

func newFakeSQS() *fakeSQS {
	return &fakeSQS{queues: map[string]*fakeQueue{}}
}

func (f *fakeSQS) CreateQueue(_ context.Context, in *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	f.createQueueCalls++
	if f.createQueueErr != nil {
		return nil, f.createQueueErr
	}
	f.justCreated = true
	name := *in.QueueName
	url := "https://sqs.us-east-1.amazonaws.com/000000000000/" + name
	f.queues[name] = &fakeQueue{
		url:                   url,
		arn:                   "arn:aws:sqs:us-east-1:000000000000:" + name,
		tags:                  in.Tags,
		approxMessages:        "0",
		approxMessagesHidden:  "0",
		approxMessagesDelayed: "0",
		attributes:            in.Attributes,
	}
	return &sqs.CreateQueueOutput{QueueUrl: &url}, nil
}

func (f *fakeSQS) GetQueueUrl(_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	f.getQueueUrlCalls++
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
	f.getQueueAttributesCalls++
	if f.justCreated && f.notReadableAfterCreate {
		f.justCreated = false
		return nil, &types.QueueDoesNotExist{}
	}
	f.justCreated = false
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
	maps.Copy(out, q.attributes)
	if policy, ok := out["RedrivePolicy"]; ok {
		out["RedrivePolicy"] = redrivePolicyAsReturned(policy)
	}
	return &sqs.GetQueueAttributesOutput{Attributes: out}, nil
}

// redrivePolicyAsReturned is a RedrivePolicy the way SQS hands it back, not
// the way it was sent: compact JSON, members in name order, and
// maxReceiveCount a number even when the request quoted it. q.attributes
// keeps what was sent, so tests can still check the request.
func redrivePolicyAsReturned(sent string) string {
	var doc map[string]any
	if err := json.Unmarshal([]byte(sent), &doc); err != nil {
		return sent
	}
	if count, ok := doc["maxReceiveCount"].(string); ok {
		if n, err := strconv.Atoi(count); err == nil {
			doc["maxReceiveCount"] = n
		}
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return sent
	}
	return string(out)
}

func (f *fakeSQS) SetQueueAttributes(_ context.Context, in *sqs.SetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	f.setQueueAttributesCalls++
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
	maps.Copy(q.tags, in.Tags)
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
