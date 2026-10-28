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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/livetest"
	kmspkg "github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	snspkg "github.com/Ningendo7/cloudctl-operator/internal/resources/sns"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
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
	if err := ensureAlarms(ctx, client, nil, namespace, crName, "uid-1", region, accountID,
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

// TestLive_AlarmReachesAnEncryptedTopic forces an alarm into ALARM and reads
// the notification from a queue subscribed to a topic encrypted with the
// shared key, which needs the CloudWatch key policy statement for this alarm.
func TestLive_AlarmReachesAnEncryptedTopic(t *testing.T) {
	cfg, accountID := skipUnlessLiveAWSCredentials(t)
	ctx := context.Background()
	cwClient := cloudwatch.NewFromConfig(cfg)
	snsClient := snssdk.NewFromConfig(cfg)
	sqsClient := sqssdk.NewFromConfig(cfg)
	region := cfg.Region
	namespace, crName := "live", liveUniqueSuffix(t)

	key := livetest.SharedKey(t, cfg)
	snsSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
		{Name: "alerts", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: livetest.SharedKeyName}},
	}}
	ledger, err := snspkg.Ensure(ctx, snsClient, nil, namespace, crName, "uid-1", region, accountID, snsSpec, []depsv1alpha1.ManagedResource{key}, nil, nil)
	if err != nil {
		t.Fatalf("sns Ensure() error = %v", err)
	}
	topic, topicFound := status.FindManagedResource(ledger, "sns", "alerts")
	if !topicFound {
		t.Fatal("expected a ledger entry for the topic")
	}
	t.Cleanup(func() {
		_, _ = snsClient.DeleteTopic(context.Background(), &snssdk.DeleteTopicInput{TopicArn: &topic.ARN})
	})

	queueName := cloudctlaws.ResourceName(namespace, crName, "sqs", "inbox", 80)
	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("CreateQueue() error = %v", err)
	}
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(context.Background(), &sqssdk.DeleteQueueInput{QueueUrl: queueOut.QueueUrl})
	})
	attrs, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl: queueOut.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes() error = %v", err)
	}
	queueARN := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":"sqs:SendMessage","Resource":%q,"Condition":{"ArnEquals":{"aws:SourceArn":%q}}}]}`, queueARN, topic.ARN)
	if _, err := sqsClient.SetQueueAttributes(ctx, &sqssdk.SetQueueAttributesInput{
		QueueUrl: queueOut.QueueUrl, Attributes: map[string]string{"Policy": policy},
	}); err != nil {
		t.Fatalf("SetQueueAttributes() error = %v", err)
	}
	if _, err := snsClient.Subscribe(ctx, &snssdk.SubscribeInput{TopicArn: &topic.ARN, Protocol: aws.String("sqs"), Endpoint: &queueARN}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	own := ownership{namespace: namespace, crName: crName, crUID: "uid-1", region: region, accountID: accountID}
	desired := desiredAlarms(namespace, crName, &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}, nil, nil)
	if len(desired) == 0 {
		t.Fatal("expected at least one desired alarm")
	}
	// What the key policy sync grants: CloudWatch may use the key for this
	// alarm only.
	livetest.GrantServices(t, cfg, key, kmspkg.ServiceGrants{AlarmARNs: []string{alarmARN(region, accountID, desired[0].name)}})
	t.Cleanup(func() {
		if err := Cleanup(context.Background(), cwClient, namespace, crName, "uid-1", region, accountID); err != nil {
			t.Logf("alarm cleanup warning: %v", err)
		}
	})
	if err := reconcileAlarms(ctx, cwClient, own, desired[:1], []string{topic.ARN}); err != nil {
		t.Fatalf("reconcileAlarms() error = %v", err)
	}
	if _, err := cwClient.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{
		AlarmName: aws.String(desired[0].name), StateValue: cwtypes.StateValueAlarm, StateReason: aws.String("cloudctl live test"),
	}); err != nil {
		t.Fatalf("SetAlarmState() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{QueueUrl: queueOut.QueueUrl, WaitTimeSeconds: 10, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatalf("ReceiveMessage() error = %v", err)
		}
		for _, m := range out.Messages {
			if strings.Contains(aws.ToString(m.Body), desired[0].name) {
				return
			}
		}
	}
	t.Fatal("the alarm notification never reached the queue: CloudWatch could not publish to the encrypted topic")
}

// TestLive_Ensure_TakesOverAnAlarmFromAPreviousUID checks a restored CR
// takes over an unchanged alarm tagged with its old UID when told which.
func TestLive_Ensure_TakesOverAnAlarmFromAPreviousUID(t *testing.T) {
	client, accountID := newLiveClient(t)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	region := "us-east-1"
	t.Cleanup(func() {
		if err := Cleanup(context.Background(), client, namespace, crName, "uid-2", region, accountID); err != nil {
			t.Logf("alarm cleanup warning: %v", err)
		}
	})
	alarmsSpec := &depsv1alpha1.AlarmsSpec{Enabled: true}
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	if err := ensureAlarms(ctx, client, nil, namespace, crName, "uid-1", region, accountID, alarmsSpec, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, "uid-1")
	if err := ensureAlarms(takeover, client, nil, namespace, crName, "uid-2", region, accountID, alarmsSpec, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	alarmName := cloudctlaws.ResourceName(namespace, crName, "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)
	arn := alarmARN(region, accountID, alarmName)
	tagsOut, err := client.ListTagsForResource(ctx, &cloudwatch.ListTagsForResourceInput{ResourceARN: &arn})
	if err != nil {
		t.Fatalf("ListTagsForResource() error = %v", err)
	}
	tags := make(map[string]string, len(tagsOut.Tags))
	for _, tag := range tagsOut.Tags {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if !cloudctlaws.IsOwnedBy(tags, namespace, crName, "uid-2") {
		t.Errorf("expected the alarm to be tagged with the new UID, got %+v", tags)
	}
}

// TestLive_CreatingATaggedAlarmNeedsTagResource checks the permissions
// table's "docs, verify" entry for cloudwatch:TagResource on a role that has
// only what PutMetricAlarm itself needs, with and without it.
func TestLive_CreatingATaggedAlarmNeedsTagResource(t *testing.T) {
	cfg, _ := skipUnlessLiveAWSCredentials(t)
	admin := cloudwatch.NewFromConfig(cfg)
	ctx := context.Background()

	tagged := map[string]bool{}
	put := func(actions []string, label string) error {
		limited := cloudwatch.NewFromConfig(livetest.LimitedConfig(t, cfg, actions))
		if !livetest.WaitUntil(90*time.Second, func() bool {
			_, err := limited.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{MaxRecords: aws.Int32(1)})
			return err == nil
		}) {
			t.Fatalf("the limited role's policy never took effect")
		}
		name := fmt.Sprintf("cloudctl-live-tagged-%s-%d", label, time.Now().UnixNano()%1_000_000_000)
		t.Cleanup(func() {
			_, _ = admin.DeleteAlarms(context.Background(), &cloudwatch.DeleteAlarmsInput{AlarmNames: []string{name}})
		})
		_, err := limited.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
			AlarmName: &name, Namespace: aws.String("cloudctl/live"), MetricName: aws.String("probe"),
			Statistic: cwtypes.StatisticSum, Period: aws.Int32(300), EvaluationPeriods: aws.Int32(1), Threshold: aws.Float64(1),
			ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
			Tags:               []cwtypes.Tag{{Key: aws.String("cloudctl.io/owner"), Value: aws.String("live/test")}},
		})
		if err == nil {
			described, descErr := admin.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{name}})
			if descErr != nil || len(described.MetricAlarms) != 1 {
				t.Fatalf("describing the alarm just created: %v", descErr)
			}
			tags, tagErr := admin.ListTagsForResource(ctx, &cloudwatch.ListTagsForResourceInput{ResourceARN: described.MetricAlarms[0].AlarmArn})
			if tagErr != nil {
				t.Fatalf("listing the alarm's tags: %v", tagErr)
			}
			tagged[label] = len(tags.Tags) > 0
		}
		return err
	}
	base := []string{"cloudwatch:PutMetricAlarm", "cloudwatch:DescribeAlarms"}
	with := put(append(append([]string{}, base...), "cloudwatch:TagResource"), "with")
	without := put(base, "without")
	livetest.CheckActionNeeded(t, "cloudwatch:TagResource", with, without)
	if with == nil && !tagged["with"] {
		t.Error("an alarm created with cloudwatch:TagResource allowed has no tags")
	}
	if without == nil && !tagged["without"] {
		t.Error("PutMetricAlarm without cloudwatch:TagResource succeeds but drops the tags, so the operator would lose its ownership tag on an alarm")
	}
}
