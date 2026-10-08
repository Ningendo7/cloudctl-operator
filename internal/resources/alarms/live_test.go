//go:build live

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

// Live tests run against a real AWS account - no LocalStack. The
// integration tier (alarms_integration_test.go) catches a wrong belief
// about CloudWatch's API shape against LocalStack's own simulation of it;
// this tier catches the case where LocalStack's simulation itself diverges
// from the real thing. Skipped entirely unless real credentials resolve
// via the standard AWS credential chain. An alarm holds no data and is
// trivially reversible (see alarms.go's own package doc comment), so
// unlike KMS this tier has nothing irreversible to be careful about -
// every alarm this creates is deleted again via t.Cleanup. Run explicitly
// with whatever already authenticates your AWS CLI:
//
//	go test -tags=live ./internal/resources/alarms/... -v
package alarms

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with
// a genuine, harmless STS call.
func skipUnlessLiveAWSCredentials(t *testing.T) (aws.Config, string) {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}
	return cfg, aws.ToString(identity.Account)
}

func newLiveClient(t *testing.T) (*cloudwatch.Client, string) {
	t.Helper()
	cfg, accountID := skipUnlessLiveAWSCredentials(t)
	return cloudwatch.NewFromConfig(cfg), accountID
}

// liveUniqueSuffix keeps each test's alarm names distinct so repeated runs
// against the same real account never collide.
func liveUniqueSuffix(t *testing.T) string {
	return "live-" + t.Name()[len("TestLive_"):] + "-" + time.Now().UTC().Format("150405")
}

func TestLive_Ensure_CreatesRealAlarmWithTagsAndCleansUp(t *testing.T) {
	client, accountID := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	region := "us-east-1"
	t.Cleanup(func() {
		existing, err := listAlarmsByPrefix(context.Background(), client, namespace+"-"+crName+"-")
		if err != nil || len(existing) == 0 {
			return
		}
		names := make([]string, 0, len(existing))
		for _, a := range existing {
			names = append(names, aws.ToString(a.AlarmName))
		}
		_, _ = client.DeleteAlarms(context.Background(), &cloudwatch.DeleteAlarmsInput{AlarmNames: names})
	})

	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	if err := Ensure(ctx, client, nil, namespace, crName, "uid-1", region, accountID,
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

	arn := alarmARN(region, accountID, alarmName)
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

	if err := Cleanup(ctx, client, namespace, crName, "uid-1", region, accountID); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	existing, err := listAlarmsByPrefix(ctx, client, namespace+"-"+crName+"-")
	if err != nil {
		t.Fatalf("listAlarmsByPrefix() error = %v", err)
	}
	if len(existing) != 0 {
		t.Errorf("expected Cleanup to delete the real alarm, got %d still present", len(existing))
	}
}
