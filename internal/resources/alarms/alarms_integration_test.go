//go:build integration

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

// Integration tests run the real AWS SDK against a LocalStack container
// instead of this package's own fakeCloudWatch. The fake only ever
// encodes our own belief about how CloudWatch's alarm API behaves; these
// catch the case where that belief is wrong. Excluded from
// `go test ./...` by the "integration" build tag - see docs/testing.md
// for how to run them (LOCALSTACK_ENDPOINT, or `make test-integration`).
package alarms

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

const (
	integrationRegion    = "us-east-1"
	integrationAccountID = "000000000000"
)

// newIntegrationClient builds a real CloudWatch client pointed at
// LocalStack. Deliberately never uses config.LoadDefaultConfig or picks
// up the environment's own AWS credentials/profile - an integration test
// must be structurally incapable of ever reaching real AWS by accident.
func newIntegrationClient(t *testing.T) *cloudwatch.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return cloudwatch.New(cloudwatch.Options{
		Region:       integrationRegion,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})
}

// uniqueSuffix keeps each test's alarm names distinct so parallel/repeated
// runs against the same long-lived LocalStack container never collide.
func uniqueSuffix(t *testing.T) string {
	return "test-" + t.Name()[len("TestIntegration_"):]
}

func deleteAlarmsIfExist(t *testing.T, client *cloudwatch.Client, namespace, crName string) {
	t.Helper()
	ctx := context.Background()
	existing, err := listAlarmsByPrefix(ctx, client, namespace+"-"+crName+"-")
	if err != nil || len(existing) == 0 {
		return
	}
	names := make([]string, 0, len(existing))
	for _, a := range existing {
		names = append(names, aws.ToString(a.AlarmName))
	}
	_, _ = client.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{AlarmNames: names})
}

func TestIntegration_Ensure_CreatesRealAlarmWithTags(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	t.Cleanup(func() { deleteAlarmsIfExist(t, client, namespace, crName) })

	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	if err := Ensure(ctx, client, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarmName := cloudctlaws.ResourceName(namespace, crName, "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)
	out, err := client.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{alarmName}})
	if err != nil {
		t.Fatalf("real DescribeAlarms() error = %v", err)
	}
	if len(out.MetricAlarms) != 1 {
		t.Fatalf("expected the real alarm to exist, got %d matching alarms", len(out.MetricAlarms))
	}

	arn := alarmARN(integrationRegion, integrationAccountID, alarmName)
	tagsOut, err := client.ListTagsForResource(ctx, &cloudwatch.ListTagsForResourceInput{ResourceARN: &arn})
	if err != nil {
		t.Fatalf("real ListTagsForResource() error = %v", err)
	}
	tags := make(map[string]string, len(tagsOut.Tags))
	for _, tag := range tagsOut.Tags {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if !cloudctlaws.IsOwnedBy(tags, namespace, crName, "uid-1") {
		t.Errorf("real alarm tags don't satisfy IsOwnedBy: %+v", tags)
	}
}

func TestIntegration_Ensure_IsIdempotentAgainstRealAWS(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	t.Cleanup(func() { deleteAlarmsIfExist(t, client, namespace, crName) })

	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	for i := 0; i < 2; i++ {
		if err := Ensure(ctx, client, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID,
			&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
			t.Fatalf("Ensure() call %d error = %v", i+1, err)
		}
	}

	existing, err := listAlarmsByPrefix(ctx, client, namespace+"-"+crName+"-")
	if err != nil {
		t.Fatalf("listAlarmsByPrefix() error = %v", err)
	}
	if len(existing) != 1 {
		t.Fatalf("expected exactly one real alarm after two reconciles, got %d", len(existing))
	}
}

func TestIntegration_Ensure_RefusesAlreadyExistingUnownedAlarm(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	t.Cleanup(func() { deleteAlarmsIfExist(t, client, namespace, crName) })

	alarmName := cloudctlaws.ResourceName(namespace, crName, "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)
	if _, err := client.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
		AlarmName:          &alarmName,
		Namespace:          aws.String("AWS/SQS"),
		MetricName:         aws.String("ApproximateAgeOfOldestMessage"),
		Dimensions:         []types.Dimension{{Name: aws.String("QueueName"), Value: aws.String("someone-elses-queue")}},
		Statistic:          types.StatisticMaximum,
		ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(900),
		EvaluationPeriods:  aws.Int32(3),
		Period:             aws.Int32(300),
	}); err != nil {
		t.Fatalf("real PutMetricAlarm() (setup, unowned) error = %v", err)
	}

	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	err := Ensure(ctx, client, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil)
	if err == nil {
		t.Fatal("expected Ensure to refuse overwriting a real alarm it doesn't own")
	}
}

func TestIntegration_Cleanup_DeletesRealAlarm(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	t.Cleanup(func() { deleteAlarmsIfExist(t, client, namespace, crName) })

	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	if err := Ensure(ctx, client, nil, namespace, crName, "uid-1", integrationRegion, integrationAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if err := Cleanup(ctx, client, namespace, crName, "uid-1", integrationRegion, integrationAccountID); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	existing, err := listAlarmsByPrefix(ctx, client, namespace+"-"+crName+"-")
	if err != nil {
		t.Fatalf("listAlarmsByPrefix() error = %v", err)
	}
	if len(existing) != 0 {
		t.Errorf("expected the real alarm to be deleted, got %d still present", len(existing))
	}
}
