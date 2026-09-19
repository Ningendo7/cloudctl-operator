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

// Package alarms reconciles CloudWatch alarms for whatever SQS/SNS/DynamoDB
// resources a CR declares, gated by spec.alarms.enabled.
//
// Deliberately lighter-weight than every other resource package in this
// operator: no status.managedResources ledger entries, no adopt/force, no
// quiet window or PendingDeletion state machine. Two things make that safe
// here in a way it isn't for SQS/S3/DynamoDB/KMS:
//
//   - PutMetricAlarm is a genuine create-or-update by alarm name (confirmed
//     via AWS's own API docs) — there's no "already exists" race to protect
//     against the way CreateQueue/CreateTopic/CreateTable have.
//   - An alarm holds no data. Deleting one is trivially reversible (it's
//     recreated the next reconcile if still desired), unlike deleting a
//     populated queue or bucket.
//
// What's kept from the other packages: the same ownership-tag-before-mutate
// discipline, so this never silently overwrites or deletes an alarm a human
// (or another tool) created at the same deterministic name. There's no
// adopt escape hatch for that conflict, unlike every other resource type —
// given alarms are pure derived config with next to no blast radius, an
// unresolvable naming collision is expected to be vanishingly rare (same
// namespace+CR-name+resource-key argument the other packages already rely
// on) and not worth a whole adoption workflow.
package alarms

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
)

const resourceType = "sns" // ConsumeRef resolution for snsTopicRef always targets an SNS topic.

// Alarm name suffixes. Combined with cloudctlaws.ResourceName's own
// namespace-crName-resourceKey scheme to form the full deterministic
// AlarmName, e.g. "default-checkout-service-orders-age".
const (
	sqsAgeAlarmSuffix         = "age"
	sqsDLQBackLogAlarmSuffix  = "dlq-backlog"
	snsDeliveryFailedSuffix   = "delivery-failed"
	dynamoReadThrottleSuffix  = "read-throttle"
	dynamoWriteThrottleSuffix = "write-throttle"
)

// alarmDef is a fully-resolved, AWS-ready alarm definition — everything
// PutMetricAlarm needs except the actions, which are the same for every
// alarm in a given reconcile (one CR-wide snsTopicRef, not per-resource).
type alarmDef struct {
	name               string
	description        string
	namespace          string
	metricName         string
	dimensionName      string
	dimensionValue     string
	statistic          types.Statistic
	comparisonOperator types.ComparisonOperator
	threshold          float64
	evaluationPeriods  int32
	period             int32
	treatMissingData   string // "" leaves AWS's own default in place.
}

// ownership identifies whose alarms these are, for tagging and for the
// pre-mutate ownership check — the same namespace+name+UID triple every
// other resource type's ownership tag carries.
type ownership struct {
	namespace, crName, crUID string
	region, accountID        string
}

// Ensure reconciles this CR's alarms against AWS: computes the desired set
// from spec (empty if alarms are disabled), resolves the shared
// notification topic if one is declared, and diffs against what currently
// exists under this CR's deterministic name prefix — creating, updating,
// or deleting as needed. k8sClient is only ever touched when
// alarms.snsTopicRef is set.
func Ensure(
	ctx context.Context,
	client cloudctlaws.CloudWatchClient,
	k8sClient client.Client,
	namespace, crName, crUID, region, accountID string,
	alarmsSpec *depsv1alpha1.AlarmsSpec,
	sqsSpec *depsv1alpha1.SQSSpec,
	snsSpec *depsv1alpha1.SNSSpec,
	dynamodbSpec *depsv1alpha1.DynamoDBSpec,
) error {
	own := ownership{
		namespace: namespace,
		crName:    crName,
		crUID:     crUID,
		region:    region,
		accountID: accountID,
	}

	var desired []alarmDef
	if alarmsSpec != nil && alarmsSpec.Enabled {
		desired = desiredAlarms(namespace, crName, sqsSpec, snsSpec, dynamodbSpec)
	}

	var alarmActions []string
	if alarmsSpec != nil && alarmsSpec.SnsTopicRef != nil {
		ref := *alarmsSpec.SnsTopicRef
		consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      crName,
		}}
		arn, ok := iam.ResolveConsumeARN(ctx, k8sClient, consumer, resourceType, ref)
		if !ok {
			return &cloudctlaws.ReconcileError{
				Err:       fmt.Errorf("alarms.snsTopicRef %s/%s/%s is not yet authorized (producer must list this CR in the topic's sharedWith) or does not exist yet", ref.Namespace, ref.Name, ref.ResourceName),
				Retryable: true,
			}
		}
		alarmActions = []string{arn}
	}

	return reconcileAlarms(ctx, client, own, desired, alarmActions)
}

// Cleanup removes every alarm this CR owns, unconditionally — called from
// finalize. Unlike every other resource type's Cleanup, there's no
// PendingDeletion wait: an alarm holds no data, so there's nothing gained
// by delaying its removal.
func Cleanup(ctx context.Context, client cloudctlaws.CloudWatchClient, namespace, crName, crUID, region, accountID string) error {
	own := ownership{namespace: namespace, crName: crName, crUID: crUID, region: region, accountID: accountID}
	return reconcileAlarms(ctx, client, own, nil, nil)
}

// desiredAlarms computes every alarm this CR's current spec implies,
// purely from spec — no AWS calls, no ledger reads. Safe because every
// dimension value (queue/topic/table name) is itself fully deterministic
// from namespace+crName+resourceKey, the same scheme every other package
// uses to create the resource in the first place.
func desiredAlarms(
	namespace, crName string,
	sqsSpec *depsv1alpha1.SQSSpec,
	snsSpec *depsv1alpha1.SNSSpec,
	dynamodbSpec *depsv1alpha1.DynamoDBSpec,
) []alarmDef {
	var defs []alarmDef

	if sqsSpec != nil {
		for _, q := range sqsSpec.Resources {
			queueName := awsResourceNameWithFIFO(namespace, crName, q.Name, q.FIFO)
			defs = append(defs, alarmDef{
				name:               cloudctlaws.ResourceName(namespace, crName, q.Name) + "-" + sqsAgeAlarmSuffix,
				description:        fmt.Sprintf("Queue %q has a message older than 15 minutes - processing is falling behind.", q.Name),
				namespace:          "AWS/SQS",
				metricName:         "ApproximateAgeOfOldestMessage",
				dimensionName:      "QueueName",
				dimensionValue:     queueName,
				statistic:          types.StatisticMaximum,
				comparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
				threshold:          900,
				evaluationPeriods:  3,
				period:             300,
				// SQS doesn't emit this metric at all for a queue with zero
				// active messages, which would otherwise leave the alarm
				// permanently INSUFFICIENT_DATA on an idle queue rather than
				// reporting the healthy state it actually is.
				treatMissingData: "notBreaching",
			})

			if q.DLQ {
				dlqName := awsResourceNameWithFIFO(namespace, crName, q.Name+"-dlq", q.FIFO)
				defs = append(defs, alarmDef{
					name:               cloudctlaws.ResourceName(namespace, crName, q.Name+"-dlq") + "-" + sqsDLQBackLogAlarmSuffix,
					description:        fmt.Sprintf("Queue %q's dead-letter queue has at least one message - something is failing repeatedly.", q.Name),
					namespace:          "AWS/SQS",
					metricName:         "ApproximateNumberOfMessagesVisible",
					dimensionName:      "QueueName",
					dimensionValue:     dlqName,
					statistic:          types.StatisticMaximum,
					comparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
					threshold:          0,
					evaluationPeriods:  1,
					period:             300,
					treatMissingData:   "notBreaching",
				})
			}
		}
	}

	if snsSpec != nil {
		for _, t := range snsSpec.Resources {
			topicName := awsResourceNameWithFIFO(namespace, crName, t.Name, t.FIFO)
			defs = append(defs, alarmDef{
				name:               cloudctlaws.ResourceName(namespace, crName, t.Name) + "-" + snsDeliveryFailedSuffix,
				description:        fmt.Sprintf("Topic %q failed to deliver at least one notification.", t.Name),
				namespace:          "AWS/SNS",
				metricName:         "NumberOfNotificationsFailed",
				dimensionName:      "TopicName",
				dimensionValue:     topicName,
				statistic:          types.StatisticSum,
				comparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
				threshold:          0,
				evaluationPeriods:  1,
				period:             300,
				treatMissingData:   "notBreaching",
			})
		}
	}

	if dynamodbSpec != nil {
		for _, tbl := range dynamodbSpec.Resources {
			tableName := cloudctlaws.ResourceName(namespace, crName, tbl.Name)
			// TreatMissingData is left unset for both: AWS/DynamoDB metrics
			// always ignore missing data regardless of what's configured, so
			// setting it here would be a no-op AWS silently overrides anyway.
			defs = append(defs,
				alarmDef{
					name:               tableName + "-" + dynamoReadThrottleSuffix,
					description:        fmt.Sprintf("Table %q is being read-throttled.", tbl.Name),
					namespace:          "AWS/DynamoDB",
					metricName:         "ReadThrottleEvents",
					dimensionName:      "TableName",
					dimensionValue:     tableName,
					statistic:          types.StatisticSum,
					comparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
					threshold:          0,
					evaluationPeriods:  1,
					period:             300,
				},
				alarmDef{
					name:               tableName + "-" + dynamoWriteThrottleSuffix,
					description:        fmt.Sprintf("Table %q is being write-throttled.", tbl.Name),
					namespace:          "AWS/DynamoDB",
					metricName:         "WriteThrottleEvents",
					dimensionName:      "TableName",
					dimensionValue:     tableName,
					statistic:          types.StatisticSum,
					comparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
					threshold:          0,
					evaluationPeriods:  1,
					period:             300,
				},
			)
		}
	}

	return defs
}

// awsResourceNameWithFIFO mirrors the sqs/sns packages' own
// name-plus-".fifo"-suffix construction — the CloudWatch dimension value
// must match the real AWS-side resource name exactly, FIFO suffix
// included, or the alarm will never see any data.
func awsResourceNameWithFIFO(namespace, crName, resourceKey string, fifo bool) string {
	name := cloudctlaws.ResourceName(namespace, crName, resourceKey)
	if fifo {
		name += ".fifo"
	}
	return name
}

// alarmARN constructs an alarm's ARN deterministically — confirmed format
// from CloudWatch's own TagResource/ListTagsForResource docs. No
// "look it up first" call needed since, unlike an SQS queue URL or an SNS
// topic ARN, this is fully derivable from the account/region we already
// know plus the name we chose ourselves.
func alarmARN(region, accountID, alarmName string) string {
	return fmt.Sprintf("arn:aws:cloudwatch:%s:%s:alarm:%s", region, accountID, alarmName)
}

// reconcileAlarms is the single declarative diff both Ensure and Cleanup
// funnel through: list what currently exists under this CR's name prefix,
// create or update everything in desired, and delete whatever this CR owns
// that's no longer desired (all of it, for Cleanup's empty-desired case).
func reconcileAlarms(
	ctx context.Context,
	client cloudctlaws.CloudWatchClient,
	own ownership,
	desired []alarmDef,
	alarmActions []string,
) error {
	prefix := cloudctlaws.ResourceName(own.namespace, own.crName, "")
	existing, err := listAlarmsByPrefix(ctx, client, prefix)
	if err != nil {
		return wrapAWSError(err, "listing alarms")
	}

	existingByName := make(map[string]struct{}, len(existing))
	for _, a := range existing {
		existingByName[aws.ToString(a.AlarmName)] = struct{}{}
	}

	var firstErr error
	desiredByName := make(map[string]struct{}, len(desired))
	for _, d := range desired {
		desiredByName[d.name] = struct{}{}

		_, exists := existingByName[d.name]
		if exists {
			owned, err := isAlarmOwnedByUs(ctx, client, own, d.name)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !owned {
				if firstErr == nil {
					firstErr = fmt.Errorf("alarm %q already exists but isn't tagged as owned by this CR — refusing to overwrite it", d.name)
				}
				continue
			}
		}

		if err := putAlarm(ctx, client, own, d, !exists, alarmActions); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	var toDelete []string
	for name := range existingByName {
		if _, stillDesired := desiredByName[name]; stillDesired {
			continue
		}
		owned, err := isAlarmOwnedByUs(ctx, client, own, name)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if owned {
			toDelete = append(toDelete, name)
		}
	}
	if len(toDelete) > 0 {
		if _, err := client.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{AlarmNames: toDelete}); err != nil {
			if firstErr == nil {
				firstErr = wrapAWSError(err, "deleting alarms no longer declared")
			}
		}
	}

	return firstErr
}

// listAlarmsByPrefix paginates through every alarm whose name starts with
// this CR's deterministic prefix ("<namespace>-<crName>-"), regardless of
// ownership — ownership is checked per-alarm afterward, since a
// name-prefix match alone is never sufficient proof (same rule every other
// resource type in this operator follows).
func listAlarmsByPrefix(ctx context.Context, client cloudctlaws.CloudWatchClient, prefix string) ([]types.MetricAlarm, error) {
	var all []types.MetricAlarm
	var nextToken *string
	for {
		out, err := client.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{
			AlarmNamePrefix: &prefix,
			NextToken:       nextToken,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, out.MetricAlarms...)
		if out.NextToken == nil {
			return all, nil
		}
		nextToken = out.NextToken
	}
}

// isAlarmOwnedByUs reads an existing alarm's tags and checks them against
// this CR's ownership triple — the same check every other resource type
// runs before ever mutating or deleting something it didn't just create.
func isAlarmOwnedByUs(ctx context.Context, client cloudctlaws.CloudWatchClient, own ownership, alarmName string) (bool, error) {
	arn := alarmARN(own.region, own.accountID, alarmName)
	out, err := client.ListTagsForResource(ctx, &cloudwatch.ListTagsForResourceInput{ResourceARN: &arn})
	if err != nil {
		return false, wrapAWSError(err, "reading alarm tags")
	}
	tags := make(map[string]string, len(out.Tags))
	for _, t := range out.Tags {
		if t.Key != nil && t.Value != nil {
			tags[*t.Key] = *t.Value
		}
	}
	return cloudctlaws.IsOwnedBy(tags, own.namespace, own.crName, own.crUID), nil
}

// putAlarm creates or updates one alarm. Tags are only ever sent on
// create — PutMetricAlarm silently ignores the Tags field on an update
// (confirmed via AWS's own API docs), and since ownership never changes
// for an already-owned alarm, there's nothing to re-tag on the update
// path anyway.
func putAlarm(ctx context.Context, client cloudctlaws.CloudWatchClient, own ownership, d alarmDef, isCreate bool, alarmActions []string) error {
	input := &cloudwatch.PutMetricAlarmInput{
		AlarmName:          aws.String(d.name),
		AlarmDescription:   aws.String(d.description),
		Namespace:          aws.String(d.namespace),
		MetricName:         aws.String(d.metricName),
		Dimensions:         []types.Dimension{{Name: aws.String(d.dimensionName), Value: aws.String(d.dimensionValue)}},
		Statistic:          d.statistic,
		ComparisonOperator: d.comparisonOperator,
		Threshold:          aws.Float64(d.threshold),
		EvaluationPeriods:  aws.Int32(d.evaluationPeriods),
		Period:             aws.Int32(d.period),
		// Notify on both the alarm firing and clearing — an alerting
		// channel that only ever hears about the start of an incident,
		// never its resolution, is a worse experience than not being
		// wired up cleanly for both directions.
		AlarmActions: alarmActions,
		OKActions:    alarmActions,
	}
	if d.treatMissingData != "" {
		input.TreatMissingData = aws.String(d.treatMissingData)
	}
	if isCreate {
		input.Tags = []types.Tag{
			{Key: aws.String(cloudctlaws.OwnerTagKey), Value: aws.String(cloudctlaws.OwnerTagValue(own.namespace, own.crName))},
			{Key: aws.String(cloudctlaws.OwnerUIDTagKey), Value: aws.String(own.crUID)},
		}
	}

	_, err := client.PutMetricAlarm(ctx, input)
	return wrapAWSError(err, fmt.Sprintf("creating/updating alarm %q", d.name))
}

func wrapAWSError(err error, context string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", context, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
