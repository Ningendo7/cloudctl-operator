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
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

// ensureAlarms runs Ensure as a first pass would, without a recorded answer
// on whether alarms may exist.
func ensureAlarms(
	ctx context.Context,
	cwClient cloudctlaws.CloudWatchClient,
	k8sClient ctrlclient.Client,
	namespace, crName, crUID, region, accountID string,
	alarmsSpec *depsv1alpha1.AlarmsSpec,
	sqsSpec *depsv1alpha1.SQSSpec,
	snsSpec *depsv1alpha1.SNSSpec,
	dynamodbSpec *depsv1alpha1.DynamoDBSpec,
) error {
	_, err := Ensure(ctx, cwClient, k8sClient, namespace, crName, crUID, region, accountID, alarmsSpec, sqsSpec, snsSpec, dynamodbSpec, nil)
	return err
}

func TestEnsure_ListsAlarmsOnlyWhileSomeMayExist(t *testing.T) {
	cw := newFakeCloudWatch()
	enabled := &depsv1alpha1.AlarmsSpec{Enabled: true}
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	var mayExist *bool
	pass := func(alarmsSpec *depsv1alpha1.AlarmsSpec) {
		t.Helper()
		got, err := Ensure(context.Background(), cw, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
			alarmsSpec, sqsSpec, nil, nil, mayExist)
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
		mayExist = &got
	}

	pass(nil)
	if cw.describeCalls != 1 || *mayExist {
		t.Fatalf("first pass: describeCalls = %d, mayExist = %v; want one list, then false", cw.describeCalls, *mayExist)
	}
	pass(nil)
	if cw.describeCalls != 1 {
		t.Fatalf("describeCalls = %d after a pass with no alarms known or declared, want still 1", cw.describeCalls)
	}

	pass(enabled)
	if cw.describeCalls != 2 || !*mayExist || len(cw.alarms) == 0 {
		t.Fatalf("enabling alarms: describeCalls = %d, mayExist = %v, alarms = %d", cw.describeCalls, *mayExist, len(cw.alarms))
	}

	// Disabling must still list once, to delete what was created.
	pass(nil)
	if cw.describeCalls != 3 || *mayExist || len(cw.alarms) != 0 {
		t.Fatalf("disabling alarms: describeCalls = %d, mayExist = %v, alarms left = %d", cw.describeCalls, *mayExist, len(cw.alarms))
	}
	pass(nil)
	if cw.describeCalls != 3 {
		t.Errorf("describeCalls = %d after the alarms were deleted, want still 3", cw.describeCalls)
	}
}

func TestEnsure_AFailedPassAssumesAlarmsMayExist(t *testing.T) {
	cw := newFakeCloudWatch()
	enabled := &depsv1alpha1.AlarmsSpec{Enabled: true}
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	if err := ensureAlarms(context.Background(), cw, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		enabled, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	// An alarm that has drifted and lost its tags can't be deleted, so it
	// may still exist after the pass.
	for _, a := range cw.alarms {
		a.tags = map[string]string{}
		a.input.Threshold = aws.Float64(12345)
	}
	known := false
	got, err := Ensure(context.Background(), cw, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		enabled, sqsSpec, nil, nil, &known)
	if err == nil {
		t.Fatal("expected the untagged alarm to be refused")
	}
	if !got {
		t.Error("a failed pass must report that alarms may exist")
	}
}

func TestEnsure_DisabledCreatesNoAlarms(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: false}, sqsSpec, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if len(client.alarms) != 0 {
		t.Errorf("expected no alarms when disabled, got %d", len(client.alarms))
	}
}

func TestEnsure_CreatesSQSAgeAlarm(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	wantName := cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)
	alarm, ok := client.alarms[wantName]
	if !ok {
		t.Fatalf("expected alarm %q to exist, got %v", wantName, mapKeys(client.alarms))
	}
	if got := aws.ToString(alarm.input.MetricName); got != "ApproximateAgeOfOldestMessage" {
		t.Errorf("MetricName = %q, want ApproximateAgeOfOldestMessage", got)
	}
	if len(alarm.input.Dimensions) != 1 || aws.ToString(alarm.input.Dimensions[0].Value) != cloudctlaws.QueueOrTopicName("default", "checkout-service", "sqs", false, sqsNameMaxLen, "orders") {
		t.Errorf("unexpected dimensions: %+v", alarm.input.Dimensions)
	}
	if alarm.tags[cloudctlaws.OwnerTagKey] != cloudctlaws.OwnerTagValue("default", "checkout-service") {
		t.Errorf("expected owner tag to be set on create, got %+v", alarm.tags)
	}
}

func TestEnsure_FIFOQueueUsesFIFODimensionValue(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", FIFO: true}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarm := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)]
	if alarm == nil {
		t.Fatal("expected the age alarm to exist")
	}
	if got := aws.ToString(alarm.input.Dimensions[0].Value); got != cloudctlaws.QueueOrTopicName("default", "checkout-service", "sqs", true, sqsNameMaxLen, "orders") {
		t.Errorf("dimension value = %q, want the .fifo-suffixed queue name", got)
	}
}

func TestEnsure_DLQGetsBacklogAlarmSharingParentFIFOness(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DLQ: true, FIFO: true}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	wantName := cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey(cloudctlaws.DerivedKey("orders", "dlq"), sqsDLQBackLogAlarmSuffix), alarmNameMaxLen)
	alarm, ok := client.alarms[wantName]
	if !ok {
		t.Fatalf("expected DLQ backlog alarm %q, got %v", wantName, mapKeys(client.alarms))
	}
	if got := aws.ToString(alarm.input.MetricName); got != "ApproximateNumberOfMessagesVisible" {
		t.Errorf("MetricName = %q, want ApproximateNumberOfMessagesVisible", got)
	}
	// The alarm watches the queue by name, so it has to be the DLQ's real
	// name: the one the queue was created under, not a re-derivation of it.
	if got, want := aws.ToString(alarm.input.Dimensions[0].Value), cloudctlaws.QueueOrTopicName("default", "checkout-service", "sqs", true, 80, "orders", "dlq"); got != want {
		t.Errorf("dimension value = %q, want the real DLQ name %q", got, want)
	}
}

// A CR named with a dot gets cleaned queue and topic names, and the alarms
// have to watch those names, not the uncleaned ones.
func TestEnsure_AlarmsWatchTheRealNamesOfQueuesAndTopicsOfADottedCR(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DLQ: true}}}
	snsSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	if err := ensureAlarms(context.Background(), client, nil, "default", "orders.v2", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, snsSpec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	want := map[string]bool{
		cloudctlaws.QueueOrTopicName("default", "orders.v2", "sqs", false, 80, "orders"):        true,
		cloudctlaws.QueueOrTopicName("default", "orders.v2", "sqs", false, 80, "orders", "dlq"): true,
		cloudctlaws.QueueOrTopicName("default", "orders.v2", "sns", false, 256, "events"):       true,
	}
	for name, alarm := range client.alarms {
		if dim := aws.ToString(alarm.input.Dimensions[0].Value); !want[dim] {
			t.Errorf("alarm %s watches %q, which is not a real queue or topic name", name, dim)
		}
	}
}

func TestEnsure_CreatesSNSDeliveryFailedAlarm(t *testing.T) {
	client := newFakeCloudWatch()
	snsSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, nil, snsSpec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarm, ok := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sns", cloudctlaws.DerivedKey("events", snsDeliveryFailedSuffix), alarmNameMaxLen)]
	if !ok {
		t.Fatal("expected the SNS delivery-failed alarm to exist")
	}
	if got := aws.ToString(alarm.input.Namespace); got != "AWS/SNS" {
		t.Errorf("Namespace = %q, want AWS/SNS", got)
	}
}

func TestEnsure_CreatesDynamoDBThrottleAlarms(t *testing.T) {
	client := newFakeCloudWatch()
	ddbSpec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{Name: "sessions", PartitionKey: "id"}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, nil, nil, ddbSpec); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if _, ok := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", cloudctlaws.DerivedKey("sessions", dynamoReadThrottleSuffix), alarmNameMaxLen)]; !ok {
		t.Error("expected a read-throttle alarm")
	}
	if _, ok := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "dynamodb", cloudctlaws.DerivedKey("sessions", dynamoWriteThrottleSuffix), alarmNameMaxLen)]; !ok {
		t.Error("expected a write-throttle alarm")
	}
}

func TestEnsure_IsIdempotentAndPreservesOwnerTags(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	run := func() error {
		return ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
			&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil)
	}
	if err := run(); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if err := run(); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if len(client.alarms) != 1 {
		t.Fatalf("expected exactly one alarm after two reconciles, got %d", len(client.alarms))
	}
	alarm := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)]
	if alarm.tags[cloudctlaws.OwnerTagKey] == "" {
		t.Error("expected owner tag to survive the update reconcile")
	}
}

func TestEnsure_RemovesAlarmForResourceNoLongerDeclared(t *testing.T) {
	client := newFakeCloudWatch()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}, {Name: "shipments"}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if len(client.alarms) != 2 {
		t.Fatalf("expected 2 alarms, got %d", len(client.alarms))
	}

	spec.Resources = spec.Resources[:1] // "shipments" removed from spec
	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if _, ok := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)]; !ok {
		t.Error("expected the still-declared queue's alarm to survive")
	}
	if _, ok := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("shipments", sqsAgeAlarmSuffix), alarmNameMaxLen)]; ok {
		t.Error("expected the removed queue's alarm to be deleted")
	}
}

func TestEnsure_DisablingAlarmsRemovesAllOfThem(t *testing.T) {
	client := newFakeCloudWatch()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: false}, spec, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if len(client.alarms) != 0 {
		t.Errorf("expected all alarms removed once disabled, got %d", len(client.alarms))
	}
}

func TestEnsure_RefusesToOverwriteUnownedAlarm(t *testing.T) {
	client := newFakeCloudWatch()
	// Simulate a pre-existing alarm at our exact deterministic name, created
	// by something else (no owner tags).
	client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)] = &fakeAlarm{
		input: &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String(cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen))},
		tags:  map[string]string{},
	}
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil)
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.NameTaken {
		t.Fatalf("expected a %s refusal to overwrite an unowned alarm, got %v", reasons.NameTaken, err)
	}
}

func TestEnsure_SnsTopicRefRetriesWhenNotYetAuthorized(t *testing.T) {
	client := newFakeCloudWatch()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := ensureAlarms(context.Background(), client, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: &depsv1alpha1.ConsumeRef{
			Namespace: "platform", Name: "platform-alerts", ResourceName: "pagerduty-bridge",
		}}, spec, nil, nil)
	if err == nil {
		t.Fatal("expected an error - the producer CR doesn't exist yet")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Fatalf("expected a retryable ReconcileError (self-resolving forward reference), got %v", err)
	}
	if len(client.alarms) != 0 {
		t.Error("expected no alarms to be created while the topic ref is unresolved")
	}
}

func TestEnsure_SnsTopicRefWiresAlarmActionsWhenAuthorized(t *testing.T) {
	client := newFakeCloudWatch()
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "platform-alerts"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
				{Name: "pagerduty-bridge", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "default", Name: "checkout-service"},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sns", Name: "pagerduty-bridge", ARN: "arn:aws:sns:us-east-1:123456789012:pagerduty-bridge"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := ensureAlarms(context.Background(), client, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: &depsv1alpha1.ConsumeRef{
			Namespace: "platform", Name: "platform-alerts", ResourceName: "pagerduty-bridge",
		}}, spec, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarm := client.alarms[cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)]
	if alarm == nil {
		t.Fatal("expected the age alarm to exist")
	}
	wantARN := "arn:aws:sns:us-east-1:123456789012:pagerduty-bridge"
	if len(alarm.input.AlarmActions) != 1 || alarm.input.AlarmActions[0] != wantARN {
		t.Errorf("AlarmActions = %v, want [%s]", alarm.input.AlarmActions, wantARN)
	}
	if len(alarm.input.OKActions) != 1 || alarm.input.OKActions[0] != wantARN {
		t.Errorf("OKActions = %v, want [%s]", alarm.input.OKActions, wantARN)
	}
}

func TestCleanup_RemovesEveryOwnedAlarmForThisCR(t *testing.T) {
	client := newFakeCloudWatch()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if len(client.alarms) == 0 {
		t.Fatal("expected at least one alarm before Cleanup")
	}

	if err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", testRegion, testAccountID); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(client.alarms) != 0 {
		t.Errorf("expected Cleanup to remove every alarm, got %d remaining", len(client.alarms))
	}
}

func mapKeys(m map[string]*fakeAlarm) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestEnsure_UnchangedAlarm_CostsNoTagReadOrWrite(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	run := func() error {
		return ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
			&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil)
	}
	if err := run(); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	puts, tagReads := client.putCalls, client.listTagsCalls

	if err := run(); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if client.putCalls != puts || client.listTagsCalls != tagReads {
		t.Errorf("expected no further put/tag-read for an unchanged alarm, got puts %d->%d, tag reads %d->%d", puts, client.putCalls, tagReads, client.listTagsCalls)
	}
}

func TestEnsure_DriftedAlarm_IsCorrected(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	run := func() error {
		return ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
			&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil)
	}
	if err := run(); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	name := cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)
	want := aws.ToFloat64(client.alarms[name].input.Threshold)
	client.alarms[name].input.Threshold = aws.Float64(want + 1000)
	puts := client.putCalls

	if err := run(); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if client.putCalls != puts+1 {
		t.Errorf("expected exactly one corrective put, got %d", client.putCalls-puts)
	}
	if got := aws.ToFloat64(client.alarms[name].input.Threshold); got != want {
		t.Errorf("threshold = %v, want it restored to %v", got, want)
	}
}

func TestAlarmMatches_DisabledAlarmIsDrift(t *testing.T) {
	d := alarmDef{name: "a", dimensionName: "QueueName", dimensionValue: "q", period: 60, evaluationPeriods: 1}
	current := types.MetricAlarm{
		Dimensions:        []types.Dimension{{Name: aws.String("QueueName"), Value: aws.String("q")}},
		Period:            aws.Int32(60),
		EvaluationPeriods: aws.Int32(1),
		Threshold:         aws.Float64(0),
	}
	if !alarmMatches(current, d, nil) {
		t.Fatal("expected the baseline alarm to match")
	}
	current.ActionsEnabled = aws.Bool(false)
	if alarmMatches(current, d, nil) {
		t.Error("an alarm someone disabled must count as drift")
	}
}

func TestEnsure_TakesOverAnAlarmFromTheUIDItWasTold(t *testing.T) {
	for name, tc := range map[string]struct {
		from        string
		transferred bool
	}{"the previous UID": {"old-uid", true}, "a different UID": {"another-uid", false}, "no UID": {"", false}} {
		t.Run(name, func(t *testing.T) {
			client := newFakeCloudWatch()
			alarmName := cloudctlaws.ResourceName("default", "checkout-service", "sqs", cloudctlaws.DerivedKey("orders", sqsAgeAlarmSuffix), alarmNameMaxLen)
			client.alarms[alarmName] = &fakeAlarm{
				input: &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String(alarmName)},
				tags: map[string]string{
					cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
					cloudctlaws.OwnerUIDTagKey: "old-uid",
				},
			}
			spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

			ctx := cloudctlaws.WithAdoptFromUID(context.Background(), tc.from)
			err := ensureAlarms(ctx, client, nil, "default", "checkout-service", "new-uid", testRegion, testAccountID,
				&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil)

			tags := client.alarms[alarmName].tags
			if !tc.transferred {
				if err == nil || tags[cloudctlaws.OwnerUIDTagKey] != "old-uid" {
					t.Fatalf("expected a refusal that leaves the alarm alone, got err=%v tags=%v", err, tags)
				}
				return
			}
			if err != nil {
				t.Fatalf("Ensure() error = %v", err)
			}
			if tags[cloudctlaws.OwnerUIDTagKey] != "new-uid" {
				t.Errorf("expected the alarm re-created under the new UID, got %v", tags)
			}
		})
	}
}

func TestEnsure_TakesOverAnUnchangedAlarmWhenToldTo(t *testing.T) {
	client := newFakeCloudWatch()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	enabled := &depsv1alpha1.AlarmsSpec{Enabled: true}
	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "old-uid", testRegion, testAccountID, enabled, spec, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}

	ctx := cloudctlaws.WithAdoptFromUID(context.Background(), "old-uid")
	if err := ensureAlarms(ctx, client, nil, "default", "checkout-service", "new-uid", testRegion, testAccountID, enabled, spec, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	for name, alarm := range client.alarms {
		if alarm.tags[cloudctlaws.OwnerUIDTagKey] != "new-uid" {
			t.Errorf("alarm %s still carries %v", name, alarm.tags)
		}
	}

	puts := client.putCalls
	if err := ensureAlarms(ctx, client, nil, "default", "checkout-service", "new-uid", testRegion, testAccountID, enabled, spec, nil, nil); err != nil {
		t.Fatalf("Ensure() (again) error = %v", err)
	}
	if client.putCalls != puts {
		t.Errorf("alarms already owned and unchanged were written again: %d calls", client.putCalls-puts)
	}
}

// The key policy of a topic's key lists these ARNs, so they must be exactly
// the alarms Ensure creates, whatever the spec has in it.
func TestAlarmARNs_AreTheAlarmsEnsureCreates(t *testing.T) {
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DLQ: true}, {Name: "audit", FIFO: true}}}
	snsSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	ddbSpec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{Name: "sessions", PartitionKey: "id"}}}
	alarmsSpec := &depsv1alpha1.AlarmsSpec{Enabled: true}
	client := newFakeCloudWatch()
	if err := ensureAlarms(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		alarmsSpec, sqsSpec, snsSpec, ddbSpec); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	want := make([]string, 0, len(client.alarms))
	for name := range client.alarms {
		want = append(want, alarmARN(testRegion, testAccountID, name))
	}
	slices.Sort(want)

	got := AlarmARNs(testRegion, testAccountID, "default", "checkout-service", alarmsSpec, sqsSpec, snsSpec, ddbSpec)
	slices.Sort(got)

	if len(want) == 0 || !slices.Equal(got, want) {
		t.Errorf("AlarmARNs() = %v, want the ARNs of the alarms Ensure created: %v", got, want)
	}
}

func TestAlarmARNs_NoneWhenAlarmsAreNotEnabled(t *testing.T) {
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	for name, spec := range map[string]*depsv1alpha1.AlarmsSpec{"nil": nil, "disabled": {Enabled: false}} {
		if got := AlarmARNs(testRegion, testAccountID, "default", "checkout-service", spec, sqsSpec, nil, nil); len(got) != 0 {
			t.Errorf("%s: AlarmARNs() = %v, want none", name, got)
		}
	}
}
