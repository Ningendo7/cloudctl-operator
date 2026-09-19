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

package alarms

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// fakeAlarm mirrors just enough of a real CloudWatch alarm to drive the
// tests: its full PutMetricAlarm input (so tests can assert on any field)
// plus the tags it was created with.
type fakeAlarm struct {
	input *cloudwatch.PutMetricAlarmInput
	tags  map[string]string
}

type fakeCloudWatch struct {
	alarms map[string]*fakeAlarm
}

func newFakeCloudWatch() *fakeCloudWatch {
	return &fakeCloudWatch{alarms: map[string]*fakeAlarm{}}
}

func (f *fakeCloudWatch) DescribeAlarms(_ context.Context, in *cloudwatch.DescribeAlarmsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.DescribeAlarmsOutput, error) {
	prefix := aws.ToString(in.AlarmNamePrefix)
	out := &cloudwatch.DescribeAlarmsOutput{}
	for name, a := range f.alarms {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		out.MetricAlarms = append(out.MetricAlarms, types.MetricAlarm{
			AlarmName:          a.input.AlarmName,
			AlarmArn:           aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:" + name),
			Namespace:          a.input.Namespace,
			MetricName:         a.input.MetricName,
			Dimensions:         a.input.Dimensions,
			Statistic:          a.input.Statistic,
			ComparisonOperator: a.input.ComparisonOperator,
			Threshold:          a.input.Threshold,
			EvaluationPeriods:  a.input.EvaluationPeriods,
			Period:             a.input.Period,
			TreatMissingData:   a.input.TreatMissingData,
			AlarmActions:       a.input.AlarmActions,
			OKActions:          a.input.OKActions,
		})
	}
	return out, nil
}

func (f *fakeCloudWatch) PutMetricAlarm(_ context.Context, in *cloudwatch.PutMetricAlarmInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricAlarmOutput, error) {
	name := aws.ToString(in.AlarmName)
	existing, ok := f.alarms[name]
	tags := map[string]string{}
	if ok {
		// Mirrors real PutMetricAlarm behavior: Tags are ignored on update.
		tags = existing.tags
	} else {
		for _, t := range in.Tags {
			tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}
	}
	f.alarms[name] = &fakeAlarm{input: in, tags: tags}
	return &cloudwatch.PutMetricAlarmOutput{}, nil
}

func (f *fakeCloudWatch) DeleteAlarms(_ context.Context, in *cloudwatch.DeleteAlarmsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.DeleteAlarmsOutput, error) {
	for _, name := range in.AlarmNames {
		delete(f.alarms, name)
	}
	return &cloudwatch.DeleteAlarmsOutput{}, nil
}

func (f *fakeCloudWatch) ListTagsForResource(_ context.Context, in *cloudwatch.ListTagsForResourceInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.ListTagsForResourceOutput, error) {
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
