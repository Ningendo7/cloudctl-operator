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
	"maps"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
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
	if engine == enginePostgres {
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
	adopt bool,
	sharedWith []depsv1alpha1.SharedWithEntry,
) (securityGroupARN string, err error) {
	return ensureInstanceSecurityGroup(ctx, rdsClient, ec2Client, k8sClient, namespace, crName, crUID, resourceName, dbSubnetGroupName, engine, region, accountID, adopt, sharedWith, "")
}

// narrowInstanceIngress removes the rules of consumers that are no longer
// shared with, and adds nothing, so it can only ever reduce access. It does
// nothing when the recorded group is missing: there is no rule to remove, and
// creating a group is not narrowing.
func narrowInstanceIngress(
	ctx context.Context,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, crUID, resourceName, engine string,
	sharedWith []depsv1alpha1.SharedWithEntry,
	recordedARN string,
) error {
	known, ok := describeOwnedGroup(ctx, ec2Client, recordedARN, namespace, crName, crUID)
	if !ok {
		return nil
	}
	return reconcileIngress(ctx, ec2Client, k8sClient, aws.ToString(known.GroupId), known.IpPermissions, engine, namespace, crName, resourceName, sharedWith, true)
}

// ensureInstanceSecurityGroup is EnsureSecurityGroup given the ARN of the
// group the ledger already records for this instance, if any. A recorded
// group that still exists and is tagged as ours is used as it is: the
// instance's subnet group can't change and a subnet group's VPC is
// immutable, so there is no need to look either up again.
func ensureInstanceSecurityGroup(
	ctx context.Context,
	rdsClient rdsAPI,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, crUID, resourceName, dbSubnetGroupName, engine, region, accountID string,
	adopt bool,
	sharedWith []depsv1alpha1.SharedWithEntry,
	recordedARN string,
) (securityGroupARN string, err error) {
	var groupID string
	var currentIngress []types.IpPermission
	if known, ok := describeOwnedGroup(ctx, ec2Client, recordedARN, namespace, crName, crUID); ok {
		groupID, currentIngress = aws.ToString(known.GroupId), known.IpPermissions
	} else {
		vpcID, err := resolveVPCID(ctx, rdsClient, dbSubnetGroupName)
		if err != nil {
			return "", err
		}
		groupName := cloudctlaws.ResourceName(namespace, crName, "rds-sg", resourceName, 255)
		groupID, currentIngress, err = findOrCreateSecurityGroup(ctx, ec2Client, namespace, crName, crUID, groupName, vpcID, adopt)
		if err != nil {
			return "", err
		}
	}
	arn := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, groupID)

	if err := reconcileIngress(ctx, ec2Client, k8sClient, groupID, currentIngress, engine, namespace, crName, resourceName, sharedWith, false); err != nil {
		return arn, err
	}

	return arn, nil
}

// instanceSecurityGroupSuffix keeps an instance's security group apart from
// the pod identity entry in the ledger, whatever the instance is called.
const instanceSecurityGroupSuffix = "#securitygroup"

func instanceSecurityGroupLedgerName(instance string) string {
	return instance + instanceSecurityGroupSuffix
}

// recordInstanceSecurityGroup remembers an instance's security group in the
// ledger, so it can still be found and deleted after the instance is gone.
func recordInstanceSecurityGroup(ledger []depsv1alpha1.ManagedResource, instance, arn string) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	name := instanceSecurityGroupLedgerName(instance)
	if existing, found := status.FindManagedResource(ledger, podNetworkIdentityResourceType, name); found {
		createdAt = existing.CreatedAt
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           podNetworkIdentityResourceType,
		Name:           name,
		ARN:            arn,
		DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
	})
	return ledger
}

// CleanupInstanceSecurityGroups deletes the security group of every instance
// that is no longer declared and no longer tracked, which means it is gone
// from AWS: an instance is never deleted by the operator, only by hand. The
// group would otherwise count against the VPC quota forever, and its ingress
// rules, which name the consumers' groups, would keep those from being
// deleted. A group of an instance that still exists, or is still declared,
// is left alone.
func CleanupInstanceSecurityGroups(
	ctx context.Context,
	ec2Client cloudctlaws.EC2Client,
	namespace, crName, crUID string,
	spec *depsv1alpha1.RDSSpec,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
) ([]depsv1alpha1.ManagedResource, error) {
	declared := map[string]bool{}
	if spec != nil && !deleting {
		for _, r := range spec.Resources {
			declared[r.Name] = true
		}
	}

	updated := ledger
	var firstErr error
	for _, entry := range ledger {
		instance, isInstanceGroup := strings.CutSuffix(entry.Name, instanceSecurityGroupSuffix)
		if entry.Type != podNetworkIdentityResourceType || !isInstanceGroup {
			continue
		}
		if declared[instance] || status.HasManagedResource(ledger, resourceType, instance) {
			continue
		}

		groupID := SecurityGroupIDFromARN(entry.ARN)
		owned, err := securityGroupOwnedBy(ctx, ec2Client, groupID, namespace, crName, crUID)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if owned {
			if _, err := ec2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: &groupID}); err != nil && !isGroupNotFound(err) {
				if firstErr == nil {
					firstErr = deleteGroupError(err, fmt.Sprintf("deleting the security group of removed instance %q", instance))
				}
				continue
			}
		}
		status.RemoveManagedResource(&updated, podNetworkIdentityResourceType, entry.Name)
	}
	return updated, firstErr
}

// deleteGroupError classifies a failed DeleteSecurityGroup: a group another
// group's rule or a network interface still uses is waited out, not failed.
func deleteGroupError(err error, errContext string) error {
	if isDependencyViolation(err) {
		return &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("%s: still in use by another group's rule or a network interface; retrying: %w", errContext, err),
			Retryable: true,
			Reason:    reasons.WaitingForRuleRelease,
		}
	}
	return wrapEC2Error(err, errContext)
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

// describeOwnedGroup returns the group arn names if it still exists and is
// tagged as this CR's. Anything else, an error included, is false, and the
// caller does the full lookup, which reports it properly.
func describeOwnedGroup(ctx context.Context, ec2Client cloudctlaws.EC2Client, arn, namespace, crName, crUID string) (types.SecurityGroup, bool) {
	if arn == "" {
		return types.SecurityGroup{}, false
	}
	out, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{SecurityGroupIDFromARN(arn)}})
	if err != nil || len(out.SecurityGroups) == 0 {
		return types.SecurityGroup{}, false
	}
	group := out.SecurityGroups[0]
	if !cloudctlaws.IsOwnedBy(ec2TagsToMap(group.Tags), namespace, crName, crUID) {
		return types.SecurityGroup{}, false
	}
	return group, true
}

// findOrCreateSecurityGroup returns the group named groupName in vpcID,
// creating it if absent. An existing group is only reused if it carries
// this CR's ownership tags: a name match alone is not ownership, and the
// operator goes on to rewrite this group's ingress rules. adopt takes over
// an untagged group, or one left by an earlier incarnation of this same CR
// (a retained instance outlives its CR); another CR's group is never taken.
func findOrCreateSecurityGroup(ctx context.Context, ec2Client cloudctlaws.EC2Client, namespace, crName, crUID, groupName, vpcID string, adopt bool) (groupID string, ingress []types.IpPermission, err error) {
	describeOut, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []types.Filter{
			{Name: aws.String("group-name"), Values: []string{groupName}},
			{Name: aws.String("vpc-id"), Values: []string{vpcID}},
		},
	})
	if err != nil {
		return "", nil, wrapEC2Error(err, "looking up security group")
	}
	if len(describeOut.SecurityGroups) > 0 {
		existing := describeOut.SecurityGroups[0]
		groupID = aws.ToString(existing.GroupId)
		tags := ec2TagsToMap(existing.Tags)
		if cloudctlaws.IsOwnedBy(tags, namespace, crName, crUID) {
			return groupID, existing.IpPermissions, nil
		}
		if owner, ok := tags[cloudctlaws.OwnerTagKey]; ok && owner != cloudctlaws.OwnerTagValue(namespace, crName) {
			return "", nil, cloudctlaws.OwnedByAnotherCRError("security group", groupID, owner)
		}
		staleUID, stale := cloudctlaws.IsStaleUID(tags, namespace, crName, crUID)
		if !adopt && (!stale || !cloudctlaws.MayTransferFrom(ctx, staleUID)) {
			if stale {
				return "", nil, cloudctlaws.PreviousIncarnationError("security group", groupID, staleUID)
			}
			return "", nil, cloudctlaws.AdoptRequiredError("security group", groupID)
		}
		if _, err := ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
			Resources: []string{groupID},
			Tags:      mapToEC2Tags(ownerTags(namespace, crName, crUID)),
		}); err != nil {
			return "", nil, wrapEC2Error(err, "adopting security group (tagging)")
		}
		return groupID, existing.IpPermissions, nil
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
		if isDuplicateGroup(err) {
			// The lookup above missed a group an earlier pass created: EC2
			// reads lag its writes. The next pass finds it.
			return "", nil, &cloudctlaws.ReconcileError{
				Err:       fmt.Errorf("security group %q was created moments ago and is not visible yet: %w", groupName, err),
				Retryable: true,
			}
		}
		return "", nil, wrapEC2Error(err, "creating security group")
	}
	return *createOut.GroupId, nil, nil
}

// reconcileIngress grants the consumers missing from current, the rules the
// group lookup already returned, and revokes the ones no longer authorized.
// A grant that already exists would fail on every pass.
func reconcileIngress(
	ctx context.Context,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	groupID string,
	current []types.IpPermission,
	engine, ownerNamespace, ownerName, resourceName string,
	sharedWith []depsv1alpha1.SharedWithEntry,
	revokeOnly bool,
) error {
	port := enginePort(engine)
	have := map[string]bool{}
	for _, perm := range current {
		if aws.ToInt32(perm.FromPort) != port || aws.ToInt32(perm.ToPort) != port {
			continue
		}
		for _, pair := range perm.UserIdGroupPairs {
			if pair.GroupId != nil {
				have[*pair.GroupId] = true
			}
		}
	}

	desired := map[string]bool{}
	var missing []types.UserIdGroupPair
	for _, consumer := range sharedWith {
		consumerGroupARN, ok, err := consumerPodSecurityGroupARN(ctx, k8sClient, consumer.Namespace, consumer.Name,
			depsv1alpha1.ConsumeRef{Namespace: ownerNamespace, Name: ownerName, ResourceName: resourceName})
		if err != nil {
			// A failed read is not a revocation: keep every rule this pass.
			return err
		}
		if !ok {
			// Forward reference not resolved yet - self-resolves on a
			// later pass, same tolerance as every other cross-CR
			// reference in this project.
			continue
		}
		consumerGroupID := SecurityGroupIDFromARN(consumerGroupARN)
		if !desired[consumerGroupID] && !have[consumerGroupID] {
			missing = append(missing, types.UserIdGroupPair{GroupId: aws.String(consumerGroupID)})
		}
		desired[consumerGroupID] = true
	}

	var firstErr error
	if len(missing) > 0 && !revokeOnly {
		_, err := ec2Client.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
			GroupId: &groupID,
			IpPermissions: []types.IpPermission{{
				IpProtocol:       aws.String("tcp"),
				FromPort:         aws.Int32(port),
				ToPort:           aws.Int32(port),
				UserIdGroupPairs: missing,
			}},
		})
		if err != nil && !isDuplicatePermission(err) {
			firstErr = wrapEC2Error(err, fmt.Sprintf("granting ingress to %d consumer group(s)", len(missing)))
		}
	}

	if err := revokeStaleIngress(ctx, ec2Client, groupID, port, have, desired); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// revokeStaleIngress removes ingress rules, on the engine's own port, for
// any consumer group in have but not in desired. Only touches rules shaped
// like ones this package creates, so anything unexpected is left alone.
func revokeStaleIngress(ctx context.Context, ec2Client cloudctlaws.EC2Client, groupID string, port int32, have, desired map[string]bool) error {
	var firstErr error
	for _, staleGroupID := range slices.Sorted(maps.Keys(have)) {
		if desired[staleGroupID] {
			continue
		}
		_, err := ec2Client.RevokeSecurityGroupIngress(ctx, &ec2.RevokeSecurityGroupIngressInput{
			GroupId: &groupID,
			IpPermissions: []types.IpPermission{{
				IpProtocol:       aws.String("tcp"),
				FromPort:         aws.Int32(port),
				ToPort:           aws.Int32(port),
				UserIdGroupPairs: []types.UserIdGroupPair{{GroupId: aws.String(staleGroupID)}},
			}},
		})
		if err != nil && !isNotFoundPermission(err) && firstErr == nil {
			firstErr = wrapEC2Error(err, fmt.Sprintf("revoking stale ingress for security group %q", staleGroupID))
		}
	}
	return firstErr
}

// consumerPodSecurityGroupARN reads a consuming CR's own published pod
// security group from its status ledger - the same "only ever read what
// the other side already published, never reach into its resources"
// split IAM policy derivation already uses for reading a producer's ARN,
// just inverted: here this instance (the producer) reads what a consumer
// published about itself. A consumer that is being deleted, or no longer
// lists this instance in rds.consumes, is not asked for access, and its
// group is not granted: it would only hold on to a group the consumer is
// trying to delete, and a CR named in sharedWith that never asked for the
// instance should not reach it.
func consumerPodSecurityGroupARN(ctx context.Context, k8sClient client.Client, namespace, crName string, instance depsv1alpha1.ConsumeRef) (string, bool, error) {
	var consumer depsv1alpha1.AppDependencies
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: crName}, &consumer); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, &cloudctlaws.ReconcileError{Err: fmt.Errorf("reading consumer %s/%s: %w", namespace, crName, err), Retryable: true}
	}
	if consumer.DeletionTimestamp != nil || consumer.Spec.RDS == nil || !slices.Contains(consumer.Spec.RDS.Consumes, instance) {
		return "", false, nil
	}
	arn, ok := PodNetworkIdentityARN(&consumer)
	return arn, ok, nil
}

func isDuplicateGroup(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeInvalidGroupDuplicate
}

func isDuplicatePermission(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeInvalidPermissionDuplicate
}

// isNotFoundPermission reports a revoke racing against (or retrying
// after) a rule that's already gone - treated as success, the same
// idempotent tolerance isDuplicatePermission gives the add side.
func isNotFoundPermission(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeInvalidPermissionNotFound
}

func ec2TagsToMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
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
