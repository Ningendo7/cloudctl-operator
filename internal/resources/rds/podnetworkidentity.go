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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
	dbSubnetGroupName, ok := firstConsumedSubnetGroupName(ctx, k8sClient, consumes)
	if !ok {
		// No consumed producer has resolved yet - self-resolves on a
		// later pass once at least one does, same forward-reference
		// tolerance as every other cross-CR reference in this project.
		return ledger, nil
	}

	vpcID, err := resolveVPCID(ctx, rdsClient, dbSubnetGroupName)
	if err != nil {
		return ledger, err
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

	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type: podNetworkIdentityResourceType,
		Name: podNetworkIdentityLedgerName,
		ARN:  arn,
		// Holds no data and is purely derived - trivially reversible,
		// same reasoning as alarms' own Delete-only lifecycle.
		DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		CreatedAt:      metav1.Now(),
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

// ensureSecurityGroupPolicy creates (via server-side apply, so re-running
// this is always idempotent) the vpcresources.k8s.aws/v1beta1
// SecurityGroupPolicy object selecting serviceAccountName's own label and
// attaching groupID - the two things EKS's Security Groups for Pods needs
// to actually associate this CR's pods with the dedicated security group
// above.
func ensureSecurityGroupPolicy(ctx context.Context, k8sClient client.Client, namespace, crName, serviceAccountName, groupID string) error {
	policy := &unstructured.Unstructured{}
	policy.SetAPIVersion("vpcresources.k8s.aws/v1beta1")
	policy.SetKind("SecurityGroupPolicy")
	policy.SetNamespace(namespace)
	policy.SetName(cloudctlaws.ResourceName(namespace, crName, "rds-pod-sg-policy", "network-identity", 255))

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
