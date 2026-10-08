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

package rds

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// podNetworkIdentityResourceType and podNetworkIdentityLedgerName are the
// ledger key a consuming CR's own dedicated pod security group is
// published under - read here (on the producer side) to build this
// instance's ingress rule, written by the consumer-side logic that
// creates it (a separate, later piece). A forward reference that hasn't
// resolved yet (the consumer hasn't reconciled its own pod identity) is
// tolerated the same way every other cross-CR reference in this project
// is: skipped silently, picked up on a later pass once it exists.
const podNetworkIdentityResourceType = "ec2-securitygroup"
const podNetworkIdentityLedgerName = "pod-network-identity"

// enginePort maps a declared engine to its standard listener port -
// needed to scope the ingress rule to exactly the port the database
// actually listens on, not a wide-open range.
func enginePort(engine string) int32 {
	if engine == "postgres" {
		return 5432
	}
	return 3306 // mysql, mariadb
}

// EnsureSecurityGroup creates and owns one security group dedicated to
// exactly this RDS instance - never a pre-existing or shared one, so
// nobody else should ever be modifying it, and this package's own
// drift-correction for it (the ingress rules below) can safely treat its
// own desired state as authoritative, unlike a shared security group
// where an external change might be a deliberate security decision, not
// drift to correct. Returns the security group's ARN, for the caller to
// derive the bare ID from when attaching it via CreateDBInstance's
// VpcSecurityGroupIds.
func EnsureSecurityGroup(
	ctx context.Context,
	rdsClient rdsAPI,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, crUID, resourceName, dbSubnetGroupName, engine, region, accountID string,
	sharedWith []depsv1alpha1.SharedWithEntry,
) (securityGroupARN string, err error) {
	vpcID, err := resolveVPCID(ctx, rdsClient, dbSubnetGroupName)
	if err != nil {
		return "", err
	}

	groupName := cloudctlaws.ResourceName(namespace, crName, "rds-sg", resourceName, 255)
	groupID, err := findOrCreateSecurityGroup(ctx, ec2Client, namespace, crName, crUID, groupName, vpcID)
	if err != nil {
		return "", err
	}
	arn := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, groupID)

	if err := reconcileIngress(ctx, ec2Client, k8sClient, groupID, engine, sharedWith); err != nil {
		return arn, err
	}

	return arn, nil
}

// SecurityGroupIDFromARN extracts the bare group ID EC2 calls like
// AuthorizeSecurityGroupIngress and CreateDBInstance's
// VpcSecurityGroupIds need, from the deterministically-constructed ARN
// this package stores in the ledger (EC2 never hands one back directly,
// unlike every other service this operator calls).
func SecurityGroupIDFromARN(arn string) string {
	idx := strings.LastIndex(arn, "/")
	if idx == -1 {
		return arn
	}
	return arn[idx+1:]
}

func resolveVPCID(ctx context.Context, rdsClient rdsAPI, dbSubnetGroupName string) (string, error) {
	out, err := rdsClient.DescribeDBSubnetGroups(ctx, &rds.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: &dbSubnetGroupName,
	})
	if err != nil {
		return "", wrapAWSError(err, "resolving VPC for DB subnet group")
	}
	if len(out.DBSubnetGroups) == 0 || out.DBSubnetGroups[0].VpcId == nil {
		return "", fmt.Errorf("DB subnet group %q did not report a VPC ID", dbSubnetGroupName)
	}
	return *out.DBSubnetGroups[0].VpcId, nil
}

func findOrCreateSecurityGroup(ctx context.Context, ec2Client cloudctlaws.EC2Client, namespace, crName, crUID, groupName, vpcID string) (string, error) {
	describeOut, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []types.Filter{
			{Name: aws.String("group-name"), Values: []string{groupName}},
			{Name: aws.String("vpc-id"), Values: []string{vpcID}},
		},
	})
	if err != nil {
		return "", wrapEC2Error(err, "looking up security group")
	}
	if len(describeOut.SecurityGroups) > 0 {
		// Already exists - this operator created it at this exact
		// deterministic name and it's never shared, so there's no
		// adopt-vs-refuse ownership check needed the way every AWS
		// resource elsewhere in this project needs one. A human
		// hand-creating something at this exact name is the same
		// vanishingly-rare collision class already accepted for alarms.
		return *describeOut.SecurityGroups[0].GroupId, nil
	}

	createOut, err := ec2Client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   &groupName,
		Description: aws.String("cloudctl-operator: dedicated security group for one RDS instance"),
		VpcId:       &vpcID,
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeSecurityGroup,
			Tags:         mapToEC2Tags(ownerTags(namespace, crName, crUID)),
		}},
	})
	if err != nil {
		return "", wrapEC2Error(err, "creating security group")
	}
	return *createOut.GroupId, nil
}

// reconcileIngress adds (never removes - see cleanup.go for the removal
// half, a later piece) an ingress rule for every currently-authorized
// sharedWith entry, scoped to exactly the engine's own port.
// AuthorizeSecurityGroupIngress is naturally idempotent against a rule
// that already exists (InvalidPermission.Duplicate), so this is safe to
// call every reconcile without tracking which rules already exist itself.
func reconcileIngress(ctx context.Context, ec2Client cloudctlaws.EC2Client, k8sClient client.Client, groupID, engine string, sharedWith []depsv1alpha1.SharedWithEntry) error {
	port := enginePort(engine)
	var firstErr error
	for _, consumer := range sharedWith {
		consumerGroupARN, ok := consumerPodSecurityGroupARN(ctx, k8sClient, consumer.Namespace, consumer.Name)
		if !ok {
			// Forward reference not resolved yet - self-resolves on a
			// later pass, same tolerance as every other cross-CR
			// reference in this project.
			continue
		}
		consumerGroupID := SecurityGroupIDFromARN(consumerGroupARN)

		_, err := ec2Client.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
			GroupId: &groupID,
			IpPermissions: []types.IpPermission{{
				IpProtocol:       aws.String("tcp"),
				FromPort:         aws.Int32(port),
				ToPort:           aws.Int32(port),
				UserIdGroupPairs: []types.UserIdGroupPair{{GroupId: &consumerGroupID}},
			}},
		})
		if err != nil && !isDuplicatePermission(err) {
			if firstErr == nil {
				firstErr = wrapEC2Error(err, fmt.Sprintf("granting %s/%s ingress", consumer.Namespace, consumer.Name))
			}
		}
	}
	return firstErr
}

// consumerPodSecurityGroupARN reads a consuming CR's own published pod
// security group from its status ledger - the same "only ever read what
// the other side already published, never reach into its resources"
// split IAM policy derivation already uses for reading a producer's ARN,
// just inverted: here this instance (the producer) reads what a consumer
// published about itself.
func consumerPodSecurityGroupARN(ctx context.Context, k8sClient client.Client, namespace, crName string) (string, bool) {
	var consumer depsv1alpha1.AppDependencies
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: crName}, &consumer); err != nil {
		return "", false
	}
	entry := status.FindManagedResource(consumer.Status.ManagedResources, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		return "", false
	}
	return entry.ARN, true
}

func isDuplicatePermission(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidPermission.Duplicate"
}

func mapToEC2Tags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return tags
}

func wrapEC2Error(err error, errContext string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", errContext, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
