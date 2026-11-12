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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// awsCalls counts the AWS calls a reconcile makes, by "service.Operation".
type awsCalls map[string]int

// countAWSCalls wraps every client the controller tests use, so each call is
// counted before it reaches the fake.
func countAWSCalls(clients *cloudctlaws.Clients) awsCalls {
	calls := awsCalls{}
	clients.SQS = countingSQS{clients.SQS, calls}
	clients.SNS = countingSNS{clients.SNS, calls}
	clients.IAM = countingIAM{clients.IAM, calls}
	clients.KMS = countingKMS{clients.KMS, calls}
	clients.CloudWatch = countingCloudWatch{clients.CloudWatch, calls}
	// A nil client switches its section off, so it stays nil.
	if clients.RDS != nil {
		clients.RDS = countingRDS{clients.RDS, calls}
	}
	if clients.EC2 != nil {
		clients.EC2 = countingEC2{clients.EC2, calls}
	}
	if clients.SecretsManager != nil {
		clients.SecretsManager = countingSecretsManager{clients.SecretsManager, calls}
	}
	return calls
}

// The wrappers hold the client in a named field rather than embedding it: a
// method added to a client interface then stops this file compiling until it
// is counted too, instead of passing through uncounted.

type countingSQS struct {
	client cloudctlaws.SQSClient
	calls  awsCalls
}

func (w countingSQS) CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	w.calls["sqs.CreateQueue"]++
	return w.client.CreateQueue(ctx, in, optFns...)
}

func (w countingSQS) GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	w.calls["sqs.GetQueueUrl"]++
	return w.client.GetQueueUrl(ctx, in, optFns...)
}

func (w countingSQS) GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	w.calls["sqs.GetQueueAttributes"]++
	return w.client.GetQueueAttributes(ctx, in, optFns...)
}

func (w countingSQS) SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	w.calls["sqs.SetQueueAttributes"]++
	return w.client.SetQueueAttributes(ctx, in, optFns...)
}

func (w countingSQS) UntagQueue(ctx context.Context, in *sqs.UntagQueueInput, optFns ...func(*sqs.Options)) (*sqs.UntagQueueOutput, error) {
	w.calls["sqs.UntagQueue"]++
	return w.client.UntagQueue(ctx, in, optFns...)
}

func (w countingSQS) ListQueueTags(ctx context.Context, in *sqs.ListQueueTagsInput, optFns ...func(*sqs.Options)) (*sqs.ListQueueTagsOutput, error) {
	w.calls["sqs.ListQueueTags"]++
	return w.client.ListQueueTags(ctx, in, optFns...)
}

func (w countingSQS) TagQueue(ctx context.Context, in *sqs.TagQueueInput, optFns ...func(*sqs.Options)) (*sqs.TagQueueOutput, error) {
	w.calls["sqs.TagQueue"]++
	return w.client.TagQueue(ctx, in, optFns...)
}

func (w countingSQS) DeleteQueue(ctx context.Context, in *sqs.DeleteQueueInput, optFns ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error) {
	w.calls["sqs.DeleteQueue"]++
	return w.client.DeleteQueue(ctx, in, optFns...)
}

type countingSNS struct {
	client cloudctlaws.SNSClient
	calls  awsCalls
}

func (w countingSNS) CreateTopic(ctx context.Context, in *sns.CreateTopicInput, optFns ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	w.calls["sns.CreateTopic"]++
	return w.client.CreateTopic(ctx, in, optFns...)
}

func (w countingSNS) GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, optFns ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	w.calls["sns.GetTopicAttributes"]++
	return w.client.GetTopicAttributes(ctx, in, optFns...)
}

func (w countingSNS) SetTopicAttributes(ctx context.Context, in *sns.SetTopicAttributesInput, optFns ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
	w.calls["sns.SetTopicAttributes"]++
	return w.client.SetTopicAttributes(ctx, in, optFns...)
}

func (w countingSNS) ListTagsForResource(ctx context.Context, in *sns.ListTagsForResourceInput, optFns ...func(*sns.Options)) (*sns.ListTagsForResourceOutput, error) {
	w.calls["sns.ListTagsForResource"]++
	return w.client.ListTagsForResource(ctx, in, optFns...)
}

func (w countingSNS) TagResource(ctx context.Context, in *sns.TagResourceInput, optFns ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	w.calls["sns.TagResource"]++
	return w.client.TagResource(ctx, in, optFns...)
}

func (w countingSNS) UntagResource(ctx context.Context, in *sns.UntagResourceInput, optFns ...func(*sns.Options)) (*sns.UntagResourceOutput, error) {
	w.calls["sns.UntagResource"]++
	return w.client.UntagResource(ctx, in, optFns...)
}

func (w countingSNS) DeleteTopic(ctx context.Context, in *sns.DeleteTopicInput, optFns ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	w.calls["sns.DeleteTopic"]++
	return w.client.DeleteTopic(ctx, in, optFns...)
}

func (w countingSNS) ListSubscriptionsByTopic(ctx context.Context, in *sns.ListSubscriptionsByTopicInput, optFns ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error) {
	w.calls["sns.ListSubscriptionsByTopic"]++
	return w.client.ListSubscriptionsByTopic(ctx, in, optFns...)
}

func (w countingSNS) Subscribe(ctx context.Context, in *sns.SubscribeInput, optFns ...func(*sns.Options)) (*sns.SubscribeOutput, error) {
	w.calls["sns.Subscribe"]++
	return w.client.Subscribe(ctx, in, optFns...)
}

func (w countingSNS) Unsubscribe(ctx context.Context, in *sns.UnsubscribeInput, optFns ...func(*sns.Options)) (*sns.UnsubscribeOutput, error) {
	w.calls["sns.Unsubscribe"]++
	return w.client.Unsubscribe(ctx, in, optFns...)
}

type countingIAM struct {
	client cloudctlaws.IAMClient
	calls  awsCalls
}

func (w countingIAM) GetRole(ctx context.Context, in *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	w.calls["iam.GetRole"]++
	return w.client.GetRole(ctx, in, optFns...)
}

func (w countingIAM) CreateRole(ctx context.Context, in *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	w.calls["iam.CreateRole"]++
	return w.client.CreateRole(ctx, in, optFns...)
}

func (w countingIAM) UpdateAssumeRolePolicy(ctx context.Context, in *iam.UpdateAssumeRolePolicyInput, optFns ...func(*iam.Options)) (*iam.UpdateAssumeRolePolicyOutput, error) {
	w.calls["iam.UpdateAssumeRolePolicy"]++
	return w.client.UpdateAssumeRolePolicy(ctx, in, optFns...)
}

func (w countingIAM) ListRoleTags(ctx context.Context, in *iam.ListRoleTagsInput, optFns ...func(*iam.Options)) (*iam.ListRoleTagsOutput, error) {
	w.calls["iam.ListRoleTags"]++
	return w.client.ListRoleTags(ctx, in, optFns...)
}

func (w countingIAM) GetRolePolicy(ctx context.Context, in *iam.GetRolePolicyInput, optFns ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	w.calls["iam.GetRolePolicy"]++
	return w.client.GetRolePolicy(ctx, in, optFns...)
}

func (w countingIAM) PutRolePolicy(ctx context.Context, in *iam.PutRolePolicyInput, optFns ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	w.calls["iam.PutRolePolicy"]++
	return w.client.PutRolePolicy(ctx, in, optFns...)
}

func (w countingIAM) DeleteRolePolicy(ctx context.Context, in *iam.DeleteRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	w.calls["iam.DeleteRolePolicy"]++
	return w.client.DeleteRolePolicy(ctx, in, optFns...)
}

func (w countingIAM) DeleteRole(ctx context.Context, in *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	w.calls["iam.DeleteRole"]++
	return w.client.DeleteRole(ctx, in, optFns...)
}

func (w countingIAM) TagRole(ctx context.Context, in *iam.TagRoleInput, optFns ...func(*iam.Options)) (*iam.TagRoleOutput, error) {
	w.calls["iam.TagRole"]++
	return w.client.TagRole(ctx, in, optFns...)
}

func (w countingIAM) ListAttachedRolePolicies(ctx context.Context, in *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	w.calls["iam.ListAttachedRolePolicies"]++
	return w.client.ListAttachedRolePolicies(ctx, in, optFns...)
}

func (w countingIAM) ListInstanceProfilesForRole(ctx context.Context, in *iam.ListInstanceProfilesForRoleInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error) {
	w.calls["iam.ListInstanceProfilesForRole"]++
	return w.client.ListInstanceProfilesForRole(ctx, in, optFns...)
}

type countingKMS struct {
	client cloudctlaws.KMSClient
	calls  awsCalls
}

func (w countingKMS) DescribeKey(ctx context.Context, in *kms.DescribeKeyInput, optFns ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	w.calls["kms.DescribeKey"]++
	return w.client.DescribeKey(ctx, in, optFns...)
}

func (w countingKMS) CreateKey(ctx context.Context, in *kms.CreateKeyInput, optFns ...func(*kms.Options)) (*kms.CreateKeyOutput, error) {
	w.calls["kms.CreateKey"]++
	return w.client.CreateKey(ctx, in, optFns...)
}

func (w countingKMS) CreateAlias(ctx context.Context, in *kms.CreateAliasInput, optFns ...func(*kms.Options)) (*kms.CreateAliasOutput, error) {
	w.calls["kms.CreateAlias"]++
	return w.client.CreateAlias(ctx, in, optFns...)
}

func (w countingKMS) EnableKey(ctx context.Context, in *kms.EnableKeyInput, optFns ...func(*kms.Options)) (*kms.EnableKeyOutput, error) {
	w.calls["kms.EnableKey"]++
	return w.client.EnableKey(ctx, in, optFns...)
}

func (w countingKMS) EnableKeyRotation(ctx context.Context, in *kms.EnableKeyRotationInput, optFns ...func(*kms.Options)) (*kms.EnableKeyRotationOutput, error) {
	w.calls["kms.EnableKeyRotation"]++
	return w.client.EnableKeyRotation(ctx, in, optFns...)
}

func (w countingKMS) GetKeyRotationStatus(ctx context.Context, in *kms.GetKeyRotationStatusInput, optFns ...func(*kms.Options)) (*kms.GetKeyRotationStatusOutput, error) {
	w.calls["kms.GetKeyRotationStatus"]++
	return w.client.GetKeyRotationStatus(ctx, in, optFns...)
}

func (w countingKMS) GetKeyPolicy(ctx context.Context, in *kms.GetKeyPolicyInput, optFns ...func(*kms.Options)) (*kms.GetKeyPolicyOutput, error) {
	w.calls["kms.GetKeyPolicy"]++
	return w.client.GetKeyPolicy(ctx, in, optFns...)
}

func (w countingKMS) PutKeyPolicy(ctx context.Context, in *kms.PutKeyPolicyInput, optFns ...func(*kms.Options)) (*kms.PutKeyPolicyOutput, error) {
	w.calls["kms.PutKeyPolicy"]++
	return w.client.PutKeyPolicy(ctx, in, optFns...)
}

func (w countingKMS) ListResourceTags(ctx context.Context, in *kms.ListResourceTagsInput, optFns ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error) {
	w.calls["kms.ListResourceTags"]++
	return w.client.ListResourceTags(ctx, in, optFns...)
}

func (w countingKMS) TagResource(ctx context.Context, in *kms.TagResourceInput, optFns ...func(*kms.Options)) (*kms.TagResourceOutput, error) {
	w.calls["kms.TagResource"]++
	return w.client.TagResource(ctx, in, optFns...)
}

func (w countingKMS) UntagResource(ctx context.Context, in *kms.UntagResourceInput, optFns ...func(*kms.Options)) (*kms.UntagResourceOutput, error) {
	w.calls["kms.UntagResource"]++
	return w.client.UntagResource(ctx, in, optFns...)
}

func (w countingKMS) ScheduleKeyDeletion(ctx context.Context, in *kms.ScheduleKeyDeletionInput, optFns ...func(*kms.Options)) (*kms.ScheduleKeyDeletionOutput, error) {
	w.calls["kms.ScheduleKeyDeletion"]++
	return w.client.ScheduleKeyDeletion(ctx, in, optFns...)
}

func (w countingKMS) CancelKeyDeletion(ctx context.Context, in *kms.CancelKeyDeletionInput, optFns ...func(*kms.Options)) (*kms.CancelKeyDeletionOutput, error) {
	w.calls["kms.CancelKeyDeletion"]++
	return w.client.CancelKeyDeletion(ctx, in, optFns...)
}

type countingCloudWatch struct {
	client cloudctlaws.CloudWatchClient
	calls  awsCalls
}

func (w countingCloudWatch) DescribeAlarms(ctx context.Context, in *cloudwatch.DescribeAlarmsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.DescribeAlarmsOutput, error) {
	w.calls["cloudwatch.DescribeAlarms"]++
	return w.client.DescribeAlarms(ctx, in, optFns...)
}

func (w countingCloudWatch) PutMetricAlarm(ctx context.Context, in *cloudwatch.PutMetricAlarmInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricAlarmOutput, error) {
	w.calls["cloudwatch.PutMetricAlarm"]++
	return w.client.PutMetricAlarm(ctx, in, optFns...)
}

func (w countingCloudWatch) DeleteAlarms(ctx context.Context, in *cloudwatch.DeleteAlarmsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.DeleteAlarmsOutput, error) {
	w.calls["cloudwatch.DeleteAlarms"]++
	return w.client.DeleteAlarms(ctx, in, optFns...)
}

func (w countingCloudWatch) ListTagsForResource(ctx context.Context, in *cloudwatch.ListTagsForResourceInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListTagsForResourceOutput, error) {
	w.calls["cloudwatch.ListTagsForResource"]++
	return w.client.ListTagsForResource(ctx, in, optFns...)
}

type countingRDS struct {
	client cloudctlaws.RDSClient
	calls  awsCalls
}

func (w countingRDS) CreateDBInstance(ctx context.Context, in *rds.CreateDBInstanceInput, optFns ...func(*rds.Options)) (*rds.CreateDBInstanceOutput, error) {
	w.calls["rds.CreateDBInstance"]++
	return w.client.CreateDBInstance(ctx, in, optFns...)
}

func (w countingRDS) ModifyDBInstance(ctx context.Context, in *rds.ModifyDBInstanceInput, optFns ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error) {
	w.calls["rds.ModifyDBInstance"]++
	return w.client.ModifyDBInstance(ctx, in, optFns...)
}

func (w countingRDS) DescribeDBInstances(ctx context.Context, in *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	w.calls["rds.DescribeDBInstances"]++
	return w.client.DescribeDBInstances(ctx, in, optFns...)
}

func (w countingRDS) DescribeDBSubnetGroups(ctx context.Context, in *rds.DescribeDBSubnetGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSubnetGroupsOutput, error) {
	w.calls["rds.DescribeDBSubnetGroups"]++
	return w.client.DescribeDBSubnetGroups(ctx, in, optFns...)
}

func (w countingRDS) DescribeDBEngineVersions(ctx context.Context, in *rds.DescribeDBEngineVersionsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBEngineVersionsOutput, error) {
	w.calls["rds.DescribeDBEngineVersions"]++
	return w.client.DescribeDBEngineVersions(ctx, in, optFns...)
}

func (w countingRDS) ListTagsForResource(ctx context.Context, in *rds.ListTagsForResourceInput, optFns ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error) {
	w.calls["rds.ListTagsForResource"]++
	return w.client.ListTagsForResource(ctx, in, optFns...)
}

func (w countingRDS) AddTagsToResource(ctx context.Context, in *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error) {
	w.calls["rds.AddTagsToResource"]++
	return w.client.AddTagsToResource(ctx, in, optFns...)
}

func (w countingRDS) RemoveTagsFromResource(ctx context.Context, in *rds.RemoveTagsFromResourceInput, optFns ...func(*rds.Options)) (*rds.RemoveTagsFromResourceOutput, error) {
	w.calls["rds.RemoveTagsFromResource"]++
	return w.client.RemoveTagsFromResource(ctx, in, optFns...)
}

func (w countingRDS) CreateDBSnapshot(ctx context.Context, in *rds.CreateDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error) {
	w.calls["rds.CreateDBSnapshot"]++
	return w.client.CreateDBSnapshot(ctx, in, optFns...)
}

func (w countingRDS) DescribeDBSnapshots(ctx context.Context, in *rds.DescribeDBSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error) {
	w.calls["rds.DescribeDBSnapshots"]++
	return w.client.DescribeDBSnapshots(ctx, in, optFns...)
}

func (w countingRDS) DeleteDBSnapshot(ctx context.Context, in *rds.DeleteDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.DeleteDBSnapshotOutput, error) {
	w.calls["rds.DeleteDBSnapshot"]++
	return w.client.DeleteDBSnapshot(ctx, in, optFns...)
}

type countingEC2 struct {
	client cloudctlaws.EC2Client
	calls  awsCalls
}

func (w countingEC2) CreateSecurityGroup(ctx context.Context, in *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
	w.calls["ec2.CreateSecurityGroup"]++
	return w.client.CreateSecurityGroup(ctx, in, optFns...)
}

func (w countingEC2) DeleteSecurityGroup(ctx context.Context, in *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
	w.calls["ec2.DeleteSecurityGroup"]++
	return w.client.DeleteSecurityGroup(ctx, in, optFns...)
}

func (w countingEC2) DescribeSecurityGroups(ctx context.Context, in *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	w.calls["ec2.DescribeSecurityGroups"]++
	return w.client.DescribeSecurityGroups(ctx, in, optFns...)
}

func (w countingEC2) AuthorizeSecurityGroupIngress(ctx context.Context, in *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	w.calls["ec2.AuthorizeSecurityGroupIngress"]++
	return w.client.AuthorizeSecurityGroupIngress(ctx, in, optFns...)
}

func (w countingEC2) RevokeSecurityGroupIngress(ctx context.Context, in *ec2.RevokeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.RevokeSecurityGroupIngressOutput, error) {
	w.calls["ec2.RevokeSecurityGroupIngress"]++
	return w.client.RevokeSecurityGroupIngress(ctx, in, optFns...)
}

func (w countingEC2) CreateTags(ctx context.Context, in *ec2.CreateTagsInput, optFns ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	w.calls["ec2.CreateTags"]++
	return w.client.CreateTags(ctx, in, optFns...)
}

type countingSecretsManager struct {
	client cloudctlaws.SecretsManagerClient
	calls  awsCalls
}

func (w countingSecretsManager) GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	w.calls["secretsmanager.GetSecretValue"]++
	return w.client.GetSecretValue(ctx, in, optFns...)
}

// writes are the calls that change something in AWS: everything but reads.
func (c awsCalls) writes() map[string]int {
	out := map[string]int{}
	for call, n := range c {
		_, op, _ := strings.Cut(call, ".")
		if !strings.HasPrefix(op, "Get") && !strings.HasPrefix(op, "List") && !strings.HasPrefix(op, "Describe") {
			out[call] = n
		}
	}
	return out
}

// A drift pass over a CR with nothing to change must change nothing in AWS:
// every write is an extra request on every pass, for every CR, and one
// repeated forever usually means a comparison that can never match.
func TestReconcile_AnIdlePassWritesNothingToAWS(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "app"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", DLQ: true, Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
				{Name: "jobs"},
			}},
			SNS:    &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}},
			Alarms: &depsv1alpha1.AlarmsSpec{Enabled: true},
		},
	}
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	calls := countAWSCalls(r.AWSClients)
	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "app"}}
	pass := func() {
		t.Helper()
		clear(calls)
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
	}
	// Two passes create everything and let the trust windows settle.
	pass()
	pass()

	pass()
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("a pass inside the trust window wrote to AWS: %v", writes)
	}

	var stored depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	for i := range stored.Status.ManagedResources {
		stored.Status.ManagedResources[i].LastVerifiedAt = &stale
	}
	if err := r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}
	pass()
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("the recheck after the trust window wrote to AWS: %v", writes)
	}
	if len(calls) == 0 {
		t.Error("the recheck made no calls at all, so it checked nothing")
	}
	t.Logf("calls at the recheck: %v", calls)
}
