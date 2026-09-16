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

package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"
)

// fakeTopic and fakeSNS are a minimal in-memory stand-in for the real SNS
// client, implementing just the snsAPI methods this package calls, so
// Ensure/Cleanup can be tested without hitting real AWS or the full SDK.
type fakeTopic struct {
	arn           string
	tags          map[string]string
	attributes    map[string]string
	policy        string
	subscriptions int

	// listSubsCallCount tracks pagination progress for tests exercising
	// hasSubscriptions' NextToken loop.
	listSubsCallCount int
}

type fakeSNS struct {
	topics map[string]*fakeTopic // keyed by topic ARN

	createTopicErr         error
	listTagsForResourceErr error
}

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (throttling, permission-denied, ...) into fakeSNS calls.
type fakeAWSError struct {
	code  string
	fault smithy.ErrorFault
}

func (e *fakeAWSError) Error() string                  { return e.code }
func (e *fakeAWSError) ErrorCode() string               { return e.code }
func (e *fakeAWSError) ErrorMessage() string            { return e.code }
func (e *fakeAWSError) ErrorFault() smithy.ErrorFault { return e.fault }

func newFakeSNS() *fakeSNS {
	return &fakeSNS{topics: map[string]*fakeTopic{}}
}

func (f *fakeSNS) CreateTopic(_ context.Context, in *sns.CreateTopicInput, _ ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	if f.createTopicErr != nil {
		return nil, f.createTopicErr
	}
	arn := "arn:aws:sns:us-east-1:123456789012:" + *in.Name
	f.topics[arn] = &fakeTopic{arn: arn, tags: tagsToMap(in.Tags), attributes: in.Attributes}
	return &sns.CreateTopicOutput{TopicArn: &arn}, nil
}

func (f *fakeSNS) GetTopicAttributes(_ context.Context, in *sns.GetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	topic, ok := f.topics[*in.TopicArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	out := map[string]string{"Policy": topic.policy}
	for k, v := range topic.attributes {
		out[k] = v
	}
	return &sns.GetTopicAttributesOutput{Attributes: out}, nil
}

func (f *fakeSNS) SetTopicAttributes(_ context.Context, in *sns.SetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
	topic, ok := f.topics[*in.TopicArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	if in.AttributeName == nil {
		return &sns.SetTopicAttributesOutput{}, nil
	}
	if *in.AttributeName == "Policy" {
		if in.AttributeValue != nil {
			topic.policy = *in.AttributeValue
		} else {
			topic.policy = ""
		}
		return &sns.SetTopicAttributesOutput{}, nil
	}
	if topic.attributes == nil {
		topic.attributes = map[string]string{}
	}
	if in.AttributeValue != nil {
		topic.attributes[*in.AttributeName] = *in.AttributeValue
	}
	return &sns.SetTopicAttributesOutput{}, nil
}

func (f *fakeSNS) ListTagsForResource(_ context.Context, in *sns.ListTagsForResourceInput, _ ...func(*sns.Options)) (*sns.ListTagsForResourceOutput, error) {
	if f.listTagsForResourceErr != nil {
		return nil, f.listTagsForResourceErr
	}
	topic, ok := f.topics[*in.ResourceArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	return &sns.ListTagsForResourceOutput{Tags: mapToTags(topic.tags)}, nil
}

func (f *fakeSNS) TagResource(_ context.Context, in *sns.TagResourceInput, _ ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	topic, ok := f.topics[*in.ResourceArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	if topic.tags == nil {
		topic.tags = map[string]string{}
	}
	for k, v := range tagsToMap(in.Tags) {
		topic.tags[k] = v
	}
	return &sns.TagResourceOutput{}, nil
}

func (f *fakeSNS) UntagResource(_ context.Context, in *sns.UntagResourceInput, _ ...func(*sns.Options)) (*sns.UntagResourceOutput, error) {
	topic, ok := f.topics[*in.ResourceArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	for _, k := range in.TagKeys {
		delete(topic.tags, k)
	}
	return &sns.UntagResourceOutput{}, nil
}

func (f *fakeSNS) DeleteTopic(_ context.Context, in *sns.DeleteTopicInput, _ ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	delete(f.topics, *in.TopicArn)
	return &sns.DeleteTopicOutput{}, nil
}

func (f *fakeSNS) ListSubscriptionsByTopic(_ context.Context, in *sns.ListSubscriptionsByTopicInput, _ ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error) {
	topic, ok := f.topics[*in.TopicArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	if topic.subscriptions == 0 {
		return &sns.ListSubscriptionsByTopicOutput{}, nil
	}

	call := topic.listSubsCallCount
	topic.listSubsCallCount++

	// Simulate a real paginated response: the first page comes back empty
	// with a NextToken still set (something SNS actually does), and the
	// real subscription only shows up on the second page. A pagination loop
	// that stops at the first empty page without checking NextToken would
	// incorrectly miss it.
	if call == 0 {
		token := "page-2"
		return &sns.ListSubscriptionsByTopicOutput{NextToken: &token}, nil
	}

	arn := "sub-1"
	return &sns.ListSubscriptionsByTopicOutput{
		Subscriptions: []types.Subscription{{SubscriptionArn: &arn, TopicArn: in.TopicArn}},
	}, nil
}
