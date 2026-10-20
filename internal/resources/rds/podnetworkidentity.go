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
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/serviceaccount"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// securityGroupPolicyFieldOwner names this package's claim on the
// SecurityGroupPolicy objects it creates - a foreign CRD (AWS's VPC
// resource controller, not something this operator has any other reason
// to depend on), interacted with via unstructured.Unstructured rather
// than a vendored Go type, since nothing here needs more than setting two
// fields.
const securityGroupPolicyFieldOwner = client.FieldOwner("cloudctl-operator-network-identity")

// PodNetworkIdentityARN returns the ARN this CR has published for its own
// pod network identity, and whether one exists at all - lets another
// package (a watch predicate, specifically) detect when it changes
// without needing to know the ledger key it's stored under.
func PodNetworkIdentityARN(cr *depsv1alpha1.AppDependencies) (string, bool) {
	entry, entryFound := status.FindManagedResource(cr.Status.ManagedResources, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if !entryFound || entry.PendingDeletionSince != nil {
		// A group being released is no longer one to grant ingress to: its
		// owner has to revoke the rule that names it before it can be deleted.
		return "", false
	}
	return entry.ARN, true
}

// PodIdentityOption tunes EnsurePodNetworkIdentity.
type PodIdentityOption func(*podIdentityOptions)

type podIdentityOptions struct {
	baseGroupIDs []string
	recordEvent  status.EventRecorder
}

// WithBaseSecurityGroups lists the groups the consumer's pods keep next to
// the database group. Security Groups for Pods replaces a pod's groups with
// the ones in the policy, and a group with no ingress rules admits nothing,
// so without the pod's normal groups its Service, load balancer and probe
// traffic is cut off.
func WithBaseSecurityGroups(ids []string) PodIdentityOption {
	return func(o *podIdentityOptions) { o.baseGroupIDs = ids }
}

// WithEventRecorder reports a missing base group configuration.
func WithEventRecorder(recordEvent status.EventRecorder) PodIdentityOption {
	return func(o *podIdentityOptions) { o.recordEvent = recordEvent }
}

// EnsurePodNetworkIdentity creates and owns a security group dedicated to
// this CR's own pods (never shared - same reasoning as the per-instance
// security group above), labels the ServiceAccount those pods actually
// run as so EKS's Security Groups for Pods feature associates them, and
// publishes the security group's ARN into this CR's own status ledger -
// read by whichever RDS instance's own Ensure grants this CR ingress via
// sharedWith. Only ever needs calling when spec.RDS.Consumes is
// non-empty; a CR that never consumes an RDS instance has no reason to
// own a pod security group at all.
func EnsurePodNetworkIdentity(
	ctx context.Context,
	rdsClient rdsAPI,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, crUID, serviceAccountName, region, accountID string,
	consumes []depsv1alpha1.ConsumeRef,
	ledger []depsv1alpha1.ManagedResource,
	opts ...PodIdentityOption,
) ([]depsv1alpha1.ManagedResource, error) {
	var options podIdentityOptions
	for _, opt := range opts {
		opt(&options)
	}

	// Sticky VPC resolution: if a pod-identity security group already
	// exists and at least one of today's consumes entries still resolves
	// to its VPC, keep using that VPC rather than re-deriving one from
	// scratch. Re-deriving from scratch every pass would pick whichever
	// consumes entry resolves *first* in spec order - if an earlier entry
	// only starts resolving on a later pass (its producer was briefly
	// unready, say), resolution would flip to a different VPC than the
	// one already in use, silently orphaning the existing security group
	// (same name, but findOrCreateSecurityGroup's group-name+vpc-id
	// filter won't find it under a different VPC, so it just creates a
	// new one and overwrites the ledger's only record of the old one).
	//
	// anyConsumedVPCMatches itself is only re-run once the existing entry's
	// own trust window expires - re-verifying it every single pass would
	// mean a DescribeDBSubnetGroups call per consumes entry forever, for an
	// invariant that's already settled the vast majority of passes.
	existingEntry, existingEntryFound := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	freshlyVerified := false
	recordedGroup, stickyOK := existingPodIdentityGroup(ctx, ec2Client, ledger, namespace, crName, crUID)
	vpcID := aws.ToString(recordedGroup.VpcId)
	if stickyOK && existingEntryFound && !status.NeedsRevalidation(existingEntry) {
		// Within the trust window - trust the existing VPC as-is.
	} else if stickyOK {
		stickyOK = anyConsumedVPCMatches(ctx, rdsClient, k8sClient, consumes, vpcID)
		freshlyVerified = stickyOK
	}
	if !stickyOK {
		dbSubnetGroupName, ok := firstConsumedSubnetGroupName(ctx, k8sClient, consumes)
		if !ok {
			// No consumed producer has resolved yet - self-resolves on a
			// later pass once at least one does, same forward-reference
			// tolerance as every other cross-CR reference in this project.
			return ledger, nil
		}
		var err error
		vpcID, err = resolveVPCID(ctx, rdsClient, dbSubnetGroupName)
		if err != nil {
			return ledger, err
		}
		freshlyVerified = true
	}

	// The group just described for the sticky VPC is the one wanted, if that
	// VPC is still kept.
	groupID := aws.ToString(recordedGroup.GroupId)
	if !stickyOK {
		groupName := cloudctlaws.ResourceName(namespace, crName, "rds-pod-sg", "network-identity", 255)
		var err error
		groupID, _, err = findOrCreateSecurityGroup(ctx, ec2Client, namespace, crName, crUID, groupName, vpcID, false)
		if err != nil {
			return ledger, err
		}
	}
	arn := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, groupID)

	if err := serviceaccount.EnsureNetworkIdentityLabel(ctx, k8sClient, namespace, crName, crUID, serviceAccountName); err != nil {
		return ledger, fmt.Errorf("labeling ServiceAccount %q for network identity: %w", serviceAccountName, err)
	}
	if len(options.baseGroupIDs) == 0 && options.recordEvent != nil {
		options.recordEvent("Warning", "PodBaseSecurityGroupsUnset", fmt.Sprintf(
			"No base security groups are configured, so the pods of ServiceAccount %q lose their normal security groups and may stop receiving Service, load balancer and probe traffic; set the operator's pod base security groups",
			serviceAccountName))
	}
	if err := ensureSecurityGroupPolicy(ctx, k8sClient, namespace, crName, serviceAccountName, podGroupIDs(options.baseGroupIDs, groupID)); err != nil {
		return ledger, err
	}

	now := metav1.Now()
	createdAt := now
	lastVerifiedAt := now
	if existingEntryFound {
		createdAt = existingEntry.CreatedAt
		if !freshlyVerified && existingEntry.LastVerifiedAt != nil {
			lastVerifiedAt = *existingEntry.LastVerifiedAt
		}
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type: podNetworkIdentityResourceType,
		Name: podNetworkIdentityLedgerName,
		ARN:  arn,
		// Holds no data and is purely derived - trivially reversible,
		// same reasoning as alarms' own Delete-only lifecycle.
		DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		CreatedAt:      createdAt,
		LastVerifiedAt: &lastVerifiedAt,
	})
	return ledger, nil
}

// firstConsumedSubnetGroupName reads the DB subnet group a pod security
// group should live in from whichever producer CR in consumes has
// already declared an rds resource matching the reference. A consuming
// CR's own pod security group doesn't correspond to any one instance -
// it's a property of the CR's own pods - but EC2 security groups are
// VPC-scoped, so it still needs exactly one VPC to live in; the first
// resolved producer is used as that VPC's source of truth. A CR
// consuming instances genuinely split across different VPCs isn't
// handled here - a documented limitation, not silently wrong behavior,
// since every ingress grant this operator derives already assumes one
// shared network.
func firstConsumedSubnetGroupName(ctx context.Context, k8sClient client.Client, consumes []depsv1alpha1.ConsumeRef) (string, bool) {
	for _, ref := range consumes {
		var producer depsv1alpha1.AppDependencies
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &producer); err != nil {
			continue
		}
		if producer.Spec.RDS == nil {
			continue
		}
		for _, r := range producer.Spec.RDS.Resources {
			if r.Name == ref.ResourceName {
				return r.DBSubnetGroupName, true
			}
		}
	}
	return "", false
}

// existingPodIdentityGroup returns a CR's already-published pod security
// group, if it still exists and is tagged as ours. Its VPC is the one to keep
// using; see EnsurePodNetworkIdentity.
func existingPodIdentityGroup(ctx context.Context, ec2Client cloudctlaws.EC2Client, ledger []depsv1alpha1.ManagedResource, namespace, crName, crUID string) (types.SecurityGroup, bool) {
	entry, entryFound := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if !entryFound {
		return types.SecurityGroup{}, false
	}
	return describeOwnedGroup(ctx, ec2Client, entry.ARN, namespace, crName, crUID)
}

// anyConsumedVPCMatches reports whether at least one of today's consumes
// entries still resolves to wantVPCID - the test for whether an existing
// pod-identity security group's VPC remains justified, or whether every
// producer that once backed it is now gone or pointed elsewhere.
func anyConsumedVPCMatches(ctx context.Context, rdsClient rdsAPI, k8sClient client.Client, consumes []depsv1alpha1.ConsumeRef, wantVPCID string) bool {
	for _, ref := range consumes {
		var producer depsv1alpha1.AppDependencies
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &producer); err != nil || producer.Spec.RDS == nil {
			continue
		}
		for _, r := range producer.Spec.RDS.Resources {
			if r.Name != ref.ResourceName {
				continue
			}
			if vpcID, err := resolveVPCID(ctx, rdsClient, r.DBSubnetGroupName); err == nil && vpcID == wantVPCID {
				return true
			}
		}
	}
	return false
}

// podGroupIDs is the base groups followed by the database group, without
// repeats.
func podGroupIDs(base []string, groupID string) []string {
	ids := make([]string, 0, len(base)+1)
	for _, id := range append(slices.Clone(base), groupID) {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

// securityGroupPolicyObject builds the bare (GVK + namespace/name)
// SecurityGroupPolicy identity - shared by ensureSecurityGroupPolicy and
// CleanupPodNetworkIdentity so the two can never compute different names
// for what must be the same object.
func securityGroupPolicyObject(namespace, crName string) *unstructured.Unstructured {
	policy := &unstructured.Unstructured{}
	policy.SetAPIVersion("vpcresources.k8s.aws/v1beta1")
	policy.SetKind("SecurityGroupPolicy")
	policy.SetNamespace(namespace)
	policy.SetName(cloudctlaws.ResourceName(namespace, crName, "rds-pod-sg-policy", "network-identity", 253))
	return policy
}

// ensureSecurityGroupPolicy creates (via server-side apply, so re-running
// this is always idempotent) the vpcresources.k8s.aws/v1beta1
// SecurityGroupPolicy object selecting serviceAccountName's own label and
// attaching groupIDs - the two things EKS's Security Groups for Pods needs
// to actually associate this CR's pods with the dedicated security group
// above.
func ensureSecurityGroupPolicy(ctx context.Context, k8sClient client.Client, namespace, crName, serviceAccountName string, groupIDs []string) error {
	policy := securityGroupPolicyObject(namespace, crName)

	if err := unstructured.SetNestedStringMap(policy.Object,
		map[string]string{serviceaccount.NetworkIdentityLabelKey: serviceAccountName},
		"spec", "serviceAccountSelector", "matchLabels"); err != nil {
		return fmt.Errorf("building SecurityGroupPolicy spec: %w", err)
	}
	if err := unstructured.SetNestedStringSlice(policy.Object,
		groupIDs, "spec", "securityGroups", "groupIds"); err != nil {
		return fmt.Errorf("building SecurityGroupPolicy spec: %w", err)
	}

	// Not cached, but a read is cheaper than a write through every admission
	// webhook. Any error, like a missing CRD, falls through to the apply.
	existing := securityGroupPolicyObject(namespace, crName)
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(existing), existing); err == nil &&
		equality.Semantic.DeepEqual(existing.Object["spec"], policy.Object["spec"]) &&
		status.AppliedBy(existing, string(securityGroupPolicyFieldOwner)) {
		return nil
	}

	if err := k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(policy), securityGroupPolicyFieldOwner, client.ForceOwnership); err != nil {
		return fmt.Errorf("applying SecurityGroupPolicy (is the EKS VPC resource controller installed? Security Groups for Pods is a hard prerequisite for RDS sharedWith): %w", err)
	}
	return nil
}

// reasonWaitingForRuleRelease marks a pod group that can't be deleted yet
// because another group's rule, or a running pod, still uses it.
const reasonWaitingForRuleRelease = "WaitingForRuleRelease"

// CleanupPodNetworkIdentity tears down everything EnsurePodNetworkIdentity
// creates, once consumes is empty or the CR is being deleted. A no-op if
// nothing was ever set up. Holds no data, so unlike the RDS instance
// itself there's no deletion-safety guard needed here.
//
// The group can't be deleted while an owner's ingress rule names it, and an
// owner only revokes that rule once the group stops reading as published. So
// the entry is first marked as releasing and kept, the policy and label are
// dropped so no new pod gets the group, and the delete is retried until the
// rule and the pods using the group are gone.
func CleanupPodNetworkIdentity(
	ctx context.Context,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, crUID, serviceAccountName string,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	entry, entryFound := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if !entryFound {
		return ledger, nil
	}
	if entry.PendingDeletionSince == nil {
		now := metav1.Now()
		entry.PendingDeletionSince = &now
		status.UpsertManagedResource(&ledger, entry)
	}

	policy := securityGroupPolicyObject(namespace, crName)
	if err := k8sClient.Delete(ctx, policy); err != nil && !apierrors.IsNotFound(err) {
		return ledger, fmt.Errorf("deleting SecurityGroupPolicy: %w", err)
	}
	if err := serviceaccount.ReleaseNetworkIdentityLabel(ctx, k8sClient, namespace, crName, crUID, serviceAccountName); err != nil {
		return ledger, fmt.Errorf("releasing network identity label on ServiceAccount %q: %w", serviceAccountName, err)
	}

	groupID := SecurityGroupIDFromARN(entry.ARN)
	owned, err := securityGroupOwnedBy(ctx, ec2Client, groupID, namespace, crName, crUID)
	if err != nil {
		return ledger, err
	}
	if owned {
		if _, err := ec2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: &groupID}); err != nil && !isGroupNotFound(err) {
			return ledger, deleteGroupError(err, "deleting pod network identity security group")
		}
	}

	status.RemoveManagedResource(&ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	return ledger, nil
}

// securityGroupOwnedBy reports whether groupID still carries this CR's
// ownership tags. A group that no longer exists counts as not owned, so
// there is nothing to delete.
func securityGroupOwnedBy(ctx context.Context, ec2Client cloudctlaws.EC2Client, groupID, namespace, crName, crUID string) (bool, error) {
	out, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{groupID}})
	if err != nil {
		if isGroupNotFound(err) {
			return false, nil
		}
		return false, wrapEC2Error(err, "checking security group ownership before deleting it")
	}
	if len(out.SecurityGroups) == 0 {
		return false, nil
	}
	return cloudctlaws.IsOwnedBy(ec2TagsToMap(out.SecurityGroups[0].Tags), namespace, crName, crUID), nil
}

func isDependencyViolation(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeDependencyViolation
}

func isGroupNotFound(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeInvalidGroupNotFound
}
