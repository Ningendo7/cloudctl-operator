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

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

// CloudWatchClient is the subset of the CloudWatch client the operator
// uses, exported so it can be substituted with a fake — both the
// internal/resources/alarms package's own unit tests and controller-level
// envtest suites.
type CloudWatchClient interface {
	DescribeAlarms(ctx context.Context, in *cloudwatch.DescribeAlarmsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.DescribeAlarmsOutput, error)
	PutMetricAlarm(ctx context.Context, in *cloudwatch.PutMetricAlarmInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricAlarmOutput, error)
	DeleteAlarms(ctx context.Context, in *cloudwatch.DeleteAlarmsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.DeleteAlarmsOutput, error)
	ListTagsForResource(ctx context.Context, in *cloudwatch.ListTagsForResourceInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListTagsForResourceOutput, error)
}
