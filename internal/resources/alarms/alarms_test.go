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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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

func TestEnsure_DisabledCreatesNoAlarms(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
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

	err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	wantName := "default-checkout-service-orders-age"
	alarm, ok := client.alarms[wantName]
	if !ok {
		t.Fatalf("expected alarm %q to exist, got %v", wantName, mapKeys(client.alarms))
	}
	if got := aws.ToString(alarm.input.MetricName); got != "ApproximateAgeOfOldestMessage" {
		t.Errorf("MetricName = %q, want ApproximateAgeOfOldestMessage", got)
	}
	if len(alarm.input.Dimensions) != 1 || aws.ToString(alarm.input.Dimensions[0].Value) != "default-checkout-service-orders" {
		t.Errorf("unexpected dimensions: %+v", alarm.input.Dimensions)
	}
	if alarm.tags[cloudctlaws.OwnerTagKey] != cloudctlaws.OwnerTagValue("default", "checkout-service") {
		t.Errorf("expected owner tag to be set on create, got %+v", alarm.tags)
	}
}

func TestEnsure_FIFOQueueUsesFIFODimensionValue(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", FIFO: true}}}

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarm := client.alarms["default-checkout-service-orders-age"]
	if alarm == nil {
		t.Fatal("expected the age alarm to exist")
	}
	if got := aws.ToString(alarm.input.Dimensions[0].Value); got != "default-checkout-service-orders.fifo" {
		t.Errorf("dimension value = %q, want the .fifo-suffixed queue name", got)
	}
}

func TestEnsure_DLQGetsBacklogAlarmSharingParentFIFOness(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", DLQ: true, FIFO: true}}}

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, sqsSpec, nil, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	wantName := "default-checkout-service-orders-dlq-dlq-backlog"
	alarm, ok := client.alarms[wantName]
	if !ok {
		t.Fatalf("expected DLQ backlog alarm %q, got %v", wantName, mapKeys(client.alarms))
	}
	if got := aws.ToString(alarm.input.MetricName); got != "ApproximateNumberOfMessagesVisible" {
		t.Errorf("MetricName = %q, want ApproximateNumberOfMessagesVisible", got)
	}
	if got := aws.ToString(alarm.input.Dimensions[0].Value); got != "default-checkout-service-orders-dlq.fifo" {
		t.Errorf("dimension value = %q, want the DLQ's .fifo-suffixed name", got)
	}
}

func TestEnsure_CreatesSNSDeliveryFailedAlarm(t *testing.T) {
	client := newFakeCloudWatch()
	snsSpec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, nil, snsSpec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarm, ok := client.alarms["default-checkout-service-events-delivery-failed"]
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

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, nil, nil, ddbSpec); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if _, ok := client.alarms["default-checkout-service-sessions-read-throttle"]; !ok {
		t.Error("expected a read-throttle alarm")
	}
	if _, ok := client.alarms["default-checkout-service-sessions-write-throttle"]; !ok {
		t.Error("expected a write-throttle alarm")
	}
}

func TestEnsure_IsIdempotentAndPreservesOwnerTags(t *testing.T) {
	client := newFakeCloudWatch()
	sqsSpec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	run := func() error {
		return Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
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
	alarm := client.alarms["default-checkout-service-orders-age"]
	if alarm.tags[cloudctlaws.OwnerTagKey] == "" {
		t.Error("expected owner tag to survive the update reconcile")
	}
}

func TestEnsure_RemovesAlarmForResourceNoLongerDeclared(t *testing.T) {
	client := newFakeCloudWatch()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}, {Name: "shipments"}}}

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if len(client.alarms) != 2 {
		t.Fatalf("expected 2 alarms, got %d", len(client.alarms))
	}

	spec.Resources = spec.Resources[:1] // "shipments" removed from spec
	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	if _, ok := client.alarms["default-checkout-service-orders-age"]; !ok {
		t.Error("expected the still-declared queue's alarm to survive")
	}
	if _, ok := client.alarms["default-checkout-service-shipments-age"]; ok {
		t.Error("expected the removed queue's alarm to be deleted")
	}
}

func TestEnsure_DisablingAlarmsRemovesAllOfThem(t *testing.T) {
	client := newFakeCloudWatch()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil); err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
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
	client.alarms["default-checkout-service-orders-age"] = &fakeAlarm{
		input: &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("default-checkout-service-orders-age")},
		tags:  map[string]string{},
	}
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true}, spec, nil, nil)
	if err == nil {
		t.Fatal("expected an error refusing to overwrite an unowned alarm")
	}
}

func TestEnsure_SnsTopicRefRetriesWhenNotYetAuthorized(t *testing.T) {
	client := newFakeCloudWatch()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}

	err := Ensure(context.Background(), client, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID,
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

	err := Ensure(context.Background(), client, k8sClient, "default", "checkout-service", "uid-1", testRegion, testAccountID,
		&depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: &depsv1alpha1.ConsumeRef{
			Namespace: "platform", Name: "platform-alerts", ResourceName: "pagerduty-bridge",
		}}, spec, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	alarm := client.alarms["default-checkout-service-orders-age"]
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

	if err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID,
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
