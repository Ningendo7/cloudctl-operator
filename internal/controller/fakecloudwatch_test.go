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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// fakeAlarm and fakeCloudWatchClient are a minimal in-memory stand-in for
// the real CloudWatch client, implementing cloudctlaws.CloudWatchClient, so
// the controller's own wiring (alarm reconciliation as part of a full
// Reconcile()) can be tested without hitting real AWS. Detailed alarm
// behavior (naming, drift, ownership refusal) is already covered by
// internal/resources/alarms's own unit tests; this fake only needs to be
// complete enough to exercise the controller's plumbing around it.
type fakeAlarm struct {
	input *cloudwatch.PutMetricAlarmInput
	tags  map[string]string
}

type fakeCloudWatchClient struct {
	alarms map[string]*fakeAlarm
}

func newFakeCloudWatchClient() *fakeCloudWatchClient {
	return &fakeCloudWatchClient{alarms: map[string]*fakeAlarm{}}
}

func (f *fakeCloudWatchClient) DescribeAlarms(_ context.Context, in *cloudwatch.DescribeAlarmsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.DescribeAlarmsOutput, error) {
	prefix := aws.ToString(in.AlarmNamePrefix)
	out := &cloudwatch.DescribeAlarmsOutput{}
	for name, a := range f.alarms {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		out.MetricAlarms = append(out.MetricAlarms, types.MetricAlarm{
			AlarmName: a.input.AlarmName,
			AlarmArn:  aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:" + name),
		})
	}
	return out, nil
}

func (f *fakeCloudWatchClient) PutMetricAlarm(_ context.Context, in *cloudwatch.PutMetricAlarmInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricAlarmOutput, error) {
	name := aws.ToString(in.AlarmName)
	existing, ok := f.alarms[name]
	tags := map[string]string{}
	if ok {
		tags = existing.tags
	} else {
		for _, t := range in.Tags {
			tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}
	}
	f.alarms[name] = &fakeAlarm{input: in, tags: tags}
	return &cloudwatch.PutMetricAlarmOutput{}, nil
}

func (f *fakeCloudWatchClient) DeleteAlarms(_ context.Context, in *cloudwatch.DeleteAlarmsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.DeleteAlarmsOutput, error) {
	for _, name := range in.AlarmNames {
		delete(f.alarms, name)
	}
	return &cloudwatch.DeleteAlarmsOutput{}, nil
}

func (f *fakeCloudWatchClient) ListTagsForResource(_ context.Context, in *cloudwatch.ListTagsForResourceInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.ListTagsForResourceOutput, error) {
	arn := aws.ToString(in.ResourceARN)
	for name, a := range f.alarms {
		if strings.HasSuffix(arn, ":alarm:"+name) {
			out := &cloudwatch.ListTagsForResourceOutput{}
			for k, v := range a.tags {
				out.Tags = append(out.Tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
			}
			return out, nil
		}
	}
	return &cloudwatch.ListTagsForResourceOutput{}, nil
}
