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

package controller

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// fakeTopic and fakeSNSClient are a minimal in-memory stand-in for the real
// SNS client, implementing cloudctlaws.SNSClient, so the controller's own
// wiring can be tested without hitting real AWS. Detailed SNS behavior
// (adoption, subscription-based pending-deletion, drift correction) is
// already covered by internal/resources/sns's own unit tests; this fake
// only needs to be complete enough to exercise the controller's plumbing.
type fakeTopic struct {
	arn           string
	tags          map[string]string
	attributes    map[string]string
	policy        string
	subscriptions int
}

type fakeSNSClient struct {
	topics map[string]*fakeTopic // keyed by topic ARN

	createTopicErr error
}

func newFakeSNSClient() *fakeSNSClient {
	return &fakeSNSClient{topics: map[string]*fakeTopic{}}
}

func (f *fakeSNSClient) CreateTopic(_ context.Context, in *sns.CreateTopicInput, _ ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	if f.createTopicErr != nil {
		return nil, f.createTopicErr
	}
	arn := "arn:aws:sns:us-east-1:123456789012:" + *in.Name
	f.topics[arn] = &fakeTopic{arn: arn, tags: tagsFromSlice(in.Tags), attributes: in.Attributes}
	return &sns.CreateTopicOutput{TopicArn: &arn}, nil
}

func (f *fakeSNSClient) GetTopicAttributes(_ context.Context, in *sns.GetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
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

func (f *fakeSNSClient) SetTopicAttributes(_ context.Context, in *sns.SetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
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

func (f *fakeSNSClient) ListTagsForResource(_ context.Context, in *sns.ListTagsForResourceInput, _ ...func(*sns.Options)) (*sns.ListTagsForResourceOutput, error) {
	topic, ok := f.topics[*in.ResourceArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	return &sns.ListTagsForResourceOutput{Tags: tagsToSlice(topic.tags)}, nil
}

func (f *fakeSNSClient) TagResource(_ context.Context, in *sns.TagResourceInput, _ ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	topic, ok := f.topics[*in.ResourceArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	if topic.tags == nil {
		topic.tags = map[string]string{}
	}
	for k, v := range tagsFromSlice(in.Tags) {
		topic.tags[k] = v
	}
	return &sns.TagResourceOutput{}, nil
}

func (f *fakeSNSClient) UntagResource(_ context.Context, in *sns.UntagResourceInput, _ ...func(*sns.Options)) (*sns.UntagResourceOutput, error) {
	topic, ok := f.topics[*in.ResourceArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	for _, k := range in.TagKeys {
		delete(topic.tags, k)
	}
	return &sns.UntagResourceOutput{}, nil
}

func (f *fakeSNSClient) DeleteTopic(_ context.Context, in *sns.DeleteTopicInput, _ ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	delete(f.topics, *in.TopicArn)
	return &sns.DeleteTopicOutput{}, nil
}

func (f *fakeSNSClient) ListSubscriptionsByTopic(_ context.Context, in *sns.ListSubscriptionsByTopicInput, _ ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error) {
	topic, ok := f.topics[*in.TopicArn]
	if !ok {
		return nil, &types.NotFoundException{}
	}
	if topic.subscriptions == 0 {
		return &sns.ListSubscriptionsByTopicOutput{}, nil
	}
	arn := "sub-1"
	return &sns.ListSubscriptionsByTopicOutput{
		Subscriptions: []types.Subscription{{SubscriptionArn: &arn, TopicArn: in.TopicArn}},
	}, nil
}

func tagsFromSlice(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func tagsToSlice(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{Key: &k, Value: &v})
	}
	return tags
}
