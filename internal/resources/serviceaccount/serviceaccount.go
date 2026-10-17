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

// Package serviceaccount wires this CR's derived IAM role onto the
// Kubernetes ServiceAccount its workload actually runs as — the other half
// of IRSA that internal/resources/iam doesn't cover (that package only
// manages the AWS-side role). See docs/architecture.md's "Resource
// identity delivery to workloads" section for the two ownership modes this
// implements.
package serviceaccount

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// RoleARNAnnotation is EKS's own well-known annotation: the AWS SDK inside
// a pod running as this ServiceAccount reads it to auto-assume the role
// via a projected OIDC token, with no credential material shipped any
// other way.
const RoleARNAnnotation = "eks.amazonaws.com/role-arn"

// fieldOwner names this package's claim on RoleARNAnnotation under
// server-side apply. The API server itself then tracks exactly that field
// as ours — a ServiceAccount we merely merge into (rather than create) can
// carry annotations from a human or another tool, and releasing our claim
// (an Apply that omits the field) only ever removes what we own, never
// touching theirs. This replaces what would otherwise be a hand-rolled
// "which keys did we add" marker with the mechanism SSA already exists to
// provide.
const fieldOwner = client.FieldOwner("cloudctl-operator-serviceaccount")

// NetworkIdentityLabelKey marks a ServiceAccount as selected by this
// operator's own SecurityGroupPolicy objects (see
// internal/resources/rds) - its value is the ServiceAccount's own name,
// unique enough within one namespace to let a SecurityGroupPolicy target
// exactly one ServiceAccount even when several coexist in it.
const NetworkIdentityLabelKey = "cloudctl.io/network-identity"

// legacyNetworkIdentityFieldOwner is the one manager every CR used for the
// label before each got its own; releases also drop it, so a label claimed
// before an upgrade doesn't outlive the last CR using it.
const legacyNetworkIdentityFieldOwner = client.FieldOwner("cloudctl-operator-network-identity")

// networkIdentityFieldOwner is one CR's claim on the label. It is separate
// from fieldOwner, and per CR, so CRs sharing a ServiceAccount each hold
// the label and one releasing it doesn't take it from the others.
func networkIdentityFieldOwner(crUID string) client.FieldOwner {
	return client.FieldOwner("cloudctl-network-identity-" + crUID)
}

// AllowClaimAnnotation, set on a ServiceAccount to a CR's name, lets that CR
// take over a ServiceAccount whose IAM role was set outside this operator.
// Adding it needs write access to the ServiceAccount, which is what proves
// consent; nothing in the CR itself can, since the CR's author is exactly
// who may lack that access.
const AllowClaimAnnotation = "cloudctl.io/allow-claim"

// Reasons a ServiceAccount can't be used.
const (
	ReasonServiceAccountNotFound     = "ServiceAccountNotFound"
	ReasonServiceAccountNotClaimable = "ServiceAccountNotClaimable"
	ReasonServiceAccountInUse        = "ServiceAccountInUse"
)

// claimError says why the CR may not modify an existing ServiceAccount, or
// nil. One already claimed by another CR is refused, and so is one carrying
// an IAM role set outside this operator, unless it consents through
// AllowClaimAnnotation: the operator can edit any ServiceAccount, and must
// not do for a CR's author what their own access wouldn't allow.
func claimError(sa *corev1.ServiceAccount, namespace, crName, crUID string) error {
	if cloudctlaws.IsOwnedBy(sa.Annotations, namespace, crName, crUID) {
		return nil
	}
	if owner := sa.Annotations[cloudctlaws.OwnerTagKey]; owner != "" {
		return &cloudctlaws.ReconcileError{
			Err:    fmt.Errorf("ServiceAccount %q is already claimed by a different AppDependencies CR (%s)", sa.Name, owner),
			Reason: ReasonServiceAccountInUse,
		}
	}
	if role := sa.Annotations[RoleARNAnnotation]; role != "" && sa.Annotations[AllowClaimAnnotation] != crName {
		return &cloudctlaws.ReconcileError{
			Err: fmt.Errorf("ServiceAccount %q already has IAM role %s, set outside this operator; to hand it to this CR, annotate it with %s=%s",
				sa.Name, role, AllowClaimAnnotation, crName),
			Reason: ReasonServiceAccountNotClaimable,
		}
	}
	return nil
}

func notFoundError(name string) error {
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("ServiceAccount %q does not exist; create it, or leave the name unset to have one created for this CR", name),
		Retryable: true,
		Reason:    ReasonServiceAccountNotFound,
	}
}

// ownerReference makes the CR the controller owner of a ServiceAccount the
// operator creates for it, so garbage collection removes the two together.
func ownerReference(crName, crUID string) *applymetav1.OwnerReferenceApplyConfiguration {
	return applymetav1.OwnerReference().
		WithAPIVersion(depsv1alpha1.GroupVersion.String()).
		WithKind("AppDependencies").
		WithName(crName).
		WithUID(types.UID(crUID)).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

// EnsureNetworkIdentityLabel puts NetworkIdentityLabelKey on the
// ServiceAccount a CR's pods run as, so EKS's Security Groups for Pods can
// select exactly those pods. A ServiceAccount named after the CR is created
// when missing, owned by the CR; any other name has to exist already, and
// pass claimError, since the label decides which pods the database
// security group replaces the groups of.
func EnsureNetworkIdentityLabel(ctx context.Context, k8sClient client.Client, namespace, crName, crUID, name string) error {
	existing, exists, err := getServiceAccount(ctx, k8sClient, namespace, name)
	if err != nil {
		return err
	}
	apply := applycorev1.ServiceAccount(name, namespace).
		WithLabels(map[string]string{NetworkIdentityLabelKey: name})
	switch {
	case !exists && name != crName:
		return notFoundError(name)
	case !exists:
		apply = apply.WithOwnerReferences(ownerReference(crName, crUID))
	default:
		if err := claimError(existing, namespace, crName, crUID); err != nil {
			return err
		}
		if existing.Labels[NetworkIdentityLabelKey] == name && status.AppliedBy(existing, string(networkIdentityFieldOwner(crUID))) {
			return nil
		}
		if ownedBy(existing, crUID) {
			apply = apply.WithOwnerReferences(ownerReference(crName, crUID))
		}
	}
	return k8sClient.Apply(ctx, apply, networkIdentityFieldOwner(crUID), client.ForceOwnership)
}

// ReleaseNetworkIdentityLabel gives up this CR's claim on the label, the
// same re-apply-without-the-field mechanism release() uses for the role
// annotation; the label stays while another CR still claims it. Safe to
// call even if the ServiceAccount is already gone.
func ReleaseNetworkIdentityLabel(ctx context.Context, k8sClient client.Client, namespace, crName, crUID, name string) error {
	sa := &corev1.ServiceAccount{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sa); err != nil {
		return client.IgnoreNotFound(err)
	}
	apply := applycorev1.ServiceAccount(name, namespace)
	if owner := metav1.GetControllerOf(sa); owner != nil && string(owner.UID) == crUID {
		apply = apply.WithOwnerReferences(ownerReference(crName, crUID))
	}
	if err := k8sClient.Apply(ctx, apply, networkIdentityFieldOwner(crUID), client.ForceOwnership); err != nil {
		return err
	}
	return k8sClient.Apply(ctx, applycorev1.ServiceAccount(name, namespace), legacyNetworkIdentityFieldOwner, client.ForceOwnership)
}

// Ensure attaches roleARN to the ServiceAccount this CR's workload runs
// as: spec.ServiceAccountName if set, otherwise a CR-named ServiceAccount
// this operator owns. Returns the current status value unchanged if
// roleARN is empty — use Cleanup once no role exists, not this.
//
// An owner reference (native GC ties its lifecycle to the CR) is set only
// when this call creates the object at the default name — a name match
// alone is never adoption, same rule as every AWS-side resource here.
//
// Every claim also stamps cloudctlaws.OwnerTagKey/OwnerUIDTagKey as
// annotations, mirroring the AWS-side ownership tag. A target already
// claimed by a different CR is refused, not overwritten.
func Ensure(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies, roleARN string) (string, error) {
	if roleARN == "" {
		return cr.Status.ServiceAccountName, nil
	}

	target := cr.Spec.ServiceAccountName
	defaultedName := target == ""
	if defaultedName {
		target = cr.Name
	}

	existing, exists, err := getServiceAccount(ctx, k8sClient, cr.Namespace, target)
	if err != nil {
		return cr.Status.ServiceAccountName, err
	}
	// Checked before releasing the previous target, so a refused retarget
	// leaves the workload's current role in place.
	switch {
	case !exists && !defaultedName:
		return cr.Status.ServiceAccountName, notFoundError(target)
	case exists:
		if err := claimError(existing, cr.Namespace, cr.Name, string(cr.UID)); err != nil {
			return cr.Status.ServiceAccountName, err
		}
	}

	if prev := cr.Status.ServiceAccountName; prev != "" && prev != target {
		if err := release(ctx, k8sClient, cr, prev); err != nil {
			return prev, err
		}
	}

	isOwnedByUs := exists && cloudctlaws.IsOwnedBy(existing.Annotations, cr.Namespace, cr.Name, string(cr.UID))
	wantOwnerRef := defaultedName && (!exists || isOwnedByUs)
	annotations := map[string]string{
		RoleARNAnnotation:          roleARN,
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(cr.Namespace, cr.Name),
		cloudctlaws.OwnerUIDTagKey: string(cr.UID),
	}
	if exists && hasAnnotations(existing, annotations) && wantOwnerRef == ownedBy(existing, string(cr.UID)) &&
		status.AppliedBy(existing, string(fieldOwner)) {
		return target, nil
	}

	apply := applycorev1.ServiceAccount(target, cr.Namespace).WithAnnotations(annotations)
	if wantOwnerRef {
		gvk, err := apiutil.GVKForObject(cr, k8sClient.Scheme())
		if err != nil {
			return cr.Status.ServiceAccountName, err
		}
		apply = apply.WithOwnerReferences(applymetav1.OwnerReference().
			WithAPIVersion(gvk.GroupVersion().String()).
			WithKind(gvk.Kind).
			WithName(cr.Name).
			WithUID(cr.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))
	}

	if err := k8sClient.Apply(ctx, apply, fieldOwner, client.ForceOwnership); err != nil {
		return cr.Status.ServiceAccountName, err
	}
	return target, nil
}

// ownedBy reports whether crUID is the controller owner of sa.
func ownedBy(sa *corev1.ServiceAccount, crUID string) bool {
	owner := metav1.GetControllerOf(sa)
	return owner != nil && string(owner.UID) == crUID
}

func hasAnnotations(sa *corev1.ServiceAccount, want map[string]string) bool {
	for k, v := range want {
		if sa.Annotations[k] != v {
			return false
		}
	}
	return true
}

func getServiceAccount(ctx context.Context, k8sClient client.Client, namespace, name string) (sa *corev1.ServiceAccount, exists bool, err error) {
	sa = &corev1.ServiceAccount{}
	err = k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sa)
	if apierrors.IsNotFound(err) {
		return sa, false, nil
	}
	if err != nil {
		return sa, false, err
	}
	return sa, true, nil
}

// Cleanup releases this operator's claim on the ServiceAccount recorded in
// status — removing exactly the annotation it owns there, leaving
// everything else on the object untouched. A no-op if status has no
// ServiceAccount name recorded, or if that object is already gone.
func Cleanup(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) error {
	if cr.Status.ServiceAccountName == "" {
		return nil
	}
	return release(ctx, k8sClient, cr, cr.Status.ServiceAccountName)
}

// release gives up this field manager's claim by re-applying with the
// managed field omitted — server-side apply removes exactly the fields
// this manager previously owned and no longer requests, and does nothing
// else to the object (including not resurrecting it if something else
// deleted it in the meantime, checked for explicitly since an Apply
// against a name that doesn't exist would otherwise recreate a bare
// object with only an owner-reference-less identity, which we never want
// on a path whose whole purpose is tidying up).
func release(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies, name string) error {
	sa := &corev1.ServiceAccount{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: cr.Namespace, Name: name}, sa); err != nil {
		return client.IgnoreNotFound(err)
	}
	apply := applycorev1.ServiceAccount(name, cr.Namespace)
	// An owner reference this operator set must outlive the release: it is
	// what lets garbage collection remove a ServiceAccount it created once
	// the CR is gone. Omitting it here would strip it, orphaning the object.
	if owner := metav1.GetControllerOf(sa); owner != nil && owner.UID == cr.UID {
		apply = apply.WithOwnerReferences(applymetav1.OwnerReference().
			WithAPIVersion(owner.APIVersion).
			WithKind(owner.Kind).
			WithName(owner.Name).
			WithUID(owner.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))
	}
	return k8sClient.Apply(ctx, apply, fieldOwner, client.ForceOwnership)
}
