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
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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

	if prev := cr.Status.ServiceAccountName; prev != "" && prev != target {
		if err := release(ctx, k8sClient, cr.Namespace, prev); err != nil {
			return prev, err
		}
	}

	existing, exists, err := getServiceAccount(ctx, k8sClient, cr.Namespace, target)
	if err != nil {
		return cr.Status.ServiceAccountName, err
	}
	if exists && existing.Annotations[cloudctlaws.OwnerTagKey] != "" &&
		!cloudctlaws.IsOwnedBy(existing.Annotations, cr.Namespace, cr.Name, string(cr.UID)) {
		return cr.Status.ServiceAccountName, fmt.Errorf(
			"ServiceAccount %q is already claimed by a different AppDependencies CR (%s) — refusing to attach a different IAM role",
			target, existing.Annotations[cloudctlaws.OwnerTagKey],
		)
	}

	apply := applycorev1.ServiceAccount(target, cr.Namespace).
		WithAnnotations(map[string]string{
			RoleARNAnnotation:          roleARN,
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(cr.Namespace, cr.Name),
			cloudctlaws.OwnerUIDTagKey: string(cr.UID),
		})
	if defaultedName && !exists {
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
	return release(ctx, k8sClient, cr.Namespace, cr.Status.ServiceAccountName)
}

// release gives up this field manager's claim by re-applying with the
// managed field omitted — server-side apply removes exactly the fields
// this manager previously owned and no longer requests, and does nothing
// else to the object (including not resurrecting it if something else
// deleted it in the meantime, checked for explicitly since an Apply
// against a name that doesn't exist would otherwise recreate a bare
// object with only an owner-reference-less identity, which we never want
// on a path whose whole purpose is tidying up).
func release(ctx context.Context, k8sClient client.Client, namespace, name string) error {
	sa := &corev1.ServiceAccount{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sa); err != nil {
		return client.IgnoreNotFound(err)
	}
	apply := applycorev1.ServiceAccount(name, namespace)
	return k8sClient.Apply(ctx, apply, fieldOwner, client.ForceOwnership)
}
