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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
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
	entry := status.FindManagedResource(cr.Status.ManagedResources, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		return "", false
	}
	return entry.ARN, true
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
) ([]depsv1alpha1.ManagedResource, error) {
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
	existingEntry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	freshlyVerified := false
	vpcID, stickyOK := existingPodIdentityVPCID(ctx, ec2Client, ledger)
	if stickyOK && existingEntry != nil && !status.NeedsRevalidation(*existingEntry) {
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

	groupName := cloudctlaws.ResourceName(namespace, crName, "rds-pod-sg", "network-identity", 255)
	groupID, err := findOrCreateSecurityGroup(ctx, ec2Client, namespace, crName, crUID, groupName, vpcID)
	if err != nil {
		return ledger, err
	}
	arn := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, groupID)

	if err := serviceaccount.EnsureNetworkIdentityLabel(ctx, k8sClient, namespace, serviceAccountName); err != nil {
		return ledger, fmt.Errorf("labeling ServiceAccount %q for network identity: %w", serviceAccountName, err)
	}
	if err := ensureSecurityGroupPolicy(ctx, k8sClient, namespace, crName, serviceAccountName, groupID); err != nil {
		return ledger, err
	}

	now := metav1.Now()
	createdAt := now
	lastVerifiedAt := now
	if existingEntry != nil {
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

// existingPodIdentityVPCID returns the VPC a CR's already-published
// pod-identity security group currently lives in, read from EC2 directly
// rather than trusting anything cached - the ledger only stores an ARN,
// never the VPC itself. False if nothing's published yet, or the group
// it points at is already gone.
func existingPodIdentityVPCID(ctx context.Context, ec2Client cloudctlaws.EC2Client, ledger []depsv1alpha1.ManagedResource) (string, bool) {
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		return "", false
	}
	groupID := SecurityGroupIDFromARN(entry.ARN)
	out, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{groupID}})
	if err != nil || len(out.SecurityGroups) == 0 {
		return "", false
	}
	return aws.ToString(out.SecurityGroups[0].VpcId), true
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

// securityGroupPolicyObject builds the bare (GVK + namespace/name)
// SecurityGroupPolicy identity - shared by ensureSecurityGroupPolicy and
// CleanupPodNetworkIdentity so the two can never compute different names
// for what must be the same object.
func securityGroupPolicyObject(namespace, crName string) *unstructured.Unstructured {
	policy := &unstructured.Unstructured{}
	policy.SetAPIVersion("vpcresources.k8s.aws/v1beta1")
	policy.SetKind("SecurityGroupPolicy")
	policy.SetNamespace(namespace)
	policy.SetName(cloudctlaws.ResourceName(namespace, crName, "rds-pod-sg-policy", "network-identity", 255))
	return policy
}

// ensureSecurityGroupPolicy creates (via server-side apply, so re-running
// this is always idempotent) the vpcresources.k8s.aws/v1beta1
// SecurityGroupPolicy object selecting serviceAccountName's own label and
// attaching groupID - the two things EKS's Security Groups for Pods needs
// to actually associate this CR's pods with the dedicated security group
// above.
func ensureSecurityGroupPolicy(ctx context.Context, k8sClient client.Client, namespace, crName, serviceAccountName, groupID string) error {
	policy := securityGroupPolicyObject(namespace, crName)

	if err := unstructured.SetNestedStringMap(policy.Object,
		map[string]string{serviceaccount.NetworkIdentityLabelKey: serviceAccountName},
		"spec", "serviceAccountSelector", "matchLabels"); err != nil {
		return fmt.Errorf("building SecurityGroupPolicy spec: %w", err)
	}
	if err := unstructured.SetNestedStringSlice(policy.Object,
		[]string{groupID}, "spec", "securityGroups", "groupIds"); err != nil {
		return fmt.Errorf("building SecurityGroupPolicy spec: %w", err)
	}

	if err := k8sClient.Patch(ctx, policy, client.Apply, securityGroupPolicyFieldOwner, client.ForceOwnership); err != nil {
		return fmt.Errorf("applying SecurityGroupPolicy (is the EKS VPC resource controller installed? Security Groups for Pods is a hard prerequisite for RDS sharedWith): %w", err)
	}
	return nil
}

// CleanupPodNetworkIdentity tears down everything EnsurePodNetworkIdentity
// creates, once consumes is empty or the CR is being deleted. A no-op if
// nothing was ever set up. Holds no data, so unlike the RDS instance
// itself there's no deletion-safety guard needed here.
func CleanupPodNetworkIdentity(
	ctx context.Context,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, serviceAccountName string,
	ledger []depsv1alpha1.ManagedResource,
) ([]depsv1alpha1.ManagedResource, error) {
	entry := status.FindManagedResource(ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	if entry == nil {
		return ledger, nil
	}

	groupID := SecurityGroupIDFromARN(entry.ARN)
	if _, err := ec2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: &groupID}); err != nil && !isGroupNotFound(err) {
		return ledger, wrapEC2Error(err, "deleting pod network identity security group")
	}

	policy := securityGroupPolicyObject(namespace, crName)
	if err := k8sClient.Delete(ctx, policy); err != nil && !apierrors.IsNotFound(err) {
		return ledger, fmt.Errorf("deleting SecurityGroupPolicy: %w", err)
	}

	if err := serviceaccount.ReleaseNetworkIdentityLabel(ctx, k8sClient, namespace, serviceAccountName); err != nil {
		return ledger, fmt.Errorf("releasing network identity label on ServiceAccount %q: %w", serviceAccountName, err)
	}

	status.RemoveManagedResource(&ledger, podNetworkIdentityResourceType, podNetworkIdentityLedgerName)
	return ledger, nil
}

func isGroupNotFound(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidGroup.NotFound"
}
