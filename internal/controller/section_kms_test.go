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
	"slices"
	"testing"
	"time"

	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
)

func TestNeededKMSKeyNames_CoversEveryResourceTypeIncludingRDS(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Spec: depsv1alpha1.AppDependenciesSpec{
		SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
			{Name: "q", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
		}},
		RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
			{Name: "ledger-db", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}},
			{Name: "plain-db"},
		}},
	}}

	got := neededKMSKeyNames(cr)
	for _, want := range []string{
		kms.DedicatedKeyLedgerName(resourceTypeSQS, "q"),
		kms.DedicatedKeyLedgerName(resourceTypeRDS, "orders-db"),
		"shared",
	} {
		if !slices.Contains(got, want) {
			t.Errorf("neededKMSKeyNames() = %v, missing %q - kms.Cleanup would treat that key as unwanted", got, want)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected exactly 3 needed keys, got %v", got)
	}
}

func TestUndeclaredKMSKeyRefs(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Spec: depsv1alpha1.AppDependenciesSpec{
		KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "declared"}}},
		SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
			{Name: "a", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "declared"}},
			{Name: "b", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "typo"}},
			{Name: "c", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "typo"}},
		}},
	}}

	if got := undeclaredKMSKeyRefs(cr); !slices.Equal(got, []string{"typo"}) {
		t.Errorf("undeclaredKMSKeyRefs() = %v, want [typo] once", got)
	}
}

func TestFinalizeOrder_KeysOutliveTheResourcesEncryptedWithThem(t *testing.T) {
	names := func(sections []section) []string {
		out := make([]string, 0, len(sections))
		for _, s := range sections {
			out = append(out, s.name)
		}
		return out
	}
	r := &AppDependenciesReconciler{}

	reconcile := names(allSections(r, nil))
	if reconcile[0] != "KMSReady" {
		t.Errorf("expected kms to reconcile first so keys exist before the resources that reference them, got %v", reconcile)
	}

	finalize := names(finalizeOrder(allSections(r, nil)))
	if finalize[len(finalize)-1] != "KMSReady" {
		t.Errorf("expected kms to finalize last, got %v", finalize)
	}
	without := func(in []string) []string {
		return slices.DeleteFunc(slices.Clone(in), func(n string) bool { return n == "KMSReady" })
	}
	if !slices.Equal(without(reconcile), without(finalize)) {
		t.Errorf("only kms should move: reconcile %v, finalize %v", reconcile, finalize)
	}
}

// A key a resource still references must survive the cleanup pass even if
// the CR no longer declares it, and the mistake has to be visible.
func TestReconcileNormal_KeyStillReferencedButNotDeclared_IsKeptAndReported(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/11111111-1111-1111-1111-000000000001"
	longAgo := metav1.NewTime(metav1.Now().Add(-90 * 24 * time.Hour))
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "shared-keys", UID: "uid-1", Finalizers: []string{finalizerName}},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
			{Name: "orders", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "general"}},
		}}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{{
			Type: "kms", Name: "general", ARN: keyARN,
			State: depsv1alpha1.ManagedResourceStateVerified, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
			PendingDeletionSince: &longAgo,
		}}},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	kmsFake := r.AWSClients.KMS.(*fakeKMSClient)
	kmsFake.keys[keyARN] = &fakeKey{
		arn: keyARN, keyID: "11111111-1111-1111-1111-000000000001", keyState: kmstypes.KeyStateEnabled,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("platform", "shared-keys"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	if _, err := r.reconcileNormal(context.Background(), cr); err != nil {
		t.Fatalf("reconcileNormal() error = %v", err)
	}
	if kmsFake.keys[keyARN].keyState == kmstypes.KeyStatePendingDeletion {
		t.Error("a key a resource still references must not be scheduled for deletion")
	}
	cond := apimeta.FindStatusCondition(cr.Status.Conditions, "KMSReady")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "KMSKeyRefNotDeclared" {
		t.Errorf("expected KMSReady=False with reason KMSKeyRefNotDeclared, got %+v", cond)
	}
}
