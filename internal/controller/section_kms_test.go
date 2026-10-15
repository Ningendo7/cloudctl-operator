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
	"github.com/Ningendo7/cloudctl-operator/internal/status"
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

func kmsEntry(name, arn string, policy depsv1alpha1.DeletionPolicy) depsv1alpha1.ManagedResource {
	return depsv1alpha1.ManagedResource{Type: "kms", Name: name, ARN: arn, State: depsv1alpha1.ManagedResourceStateVerified, DeletionPolicy: policy}
}

// Which keys data still depends on comes from the ledger, not only spec:
// a resource that left spec but still holds data keeps its key.
func TestKMSKeysInUse_FromTheLedger(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
		kmsEntry("sqs#orders#key", "arn:key-a", depsv1alpha1.DeletionPolicyDelete),
		kmsEntry("shared", "arn:key-b", depsv1alpha1.DeletionPolicyDelete),
		kmsEntry("s3#receipts#key", "arn:key-c", depsv1alpha1.DeletionPolicyDelete),
		kmsEntry("rds#db#key", "arn:key-d", depsv1alpha1.DeletionPolicyDelete),
		kmsEntry("spare", "arn:key-e", depsv1alpha1.DeletionPolicyDelete),
		// Recorded before kmsKeyARN existed: found through its dedicated key.
		{Type: "sqs", Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		{Type: "sqs", Name: "orders#dlq", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
		// Retained, and encrypted with a referenced key rather than its own.
		{Type: "s3", Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, KMSKeyARN: "arn:key-b"},
		// A final snapshot is retained and uses its instance's key.
		{Type: "rds-snapshot", Name: "db", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
		{Type: "iam", Name: "role", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
	}}}

	got := kmsKeysInUse(cr, false)
	want := map[string]kms.KeyUse{
		"sqs#orders#key": {Retained: false},
		"shared":         {Retained: true},
		"rds#db#key":     {Retained: true},
	}
	if len(got) != len(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for name, use := range want {
		if g, ok := got[name]; !ok || g != use {
			t.Errorf("%s: got %+v (present=%v), want %+v", name, g, ok, use)
		}
	}
	if _, ok := got["s3#receipts#key"]; ok {
		t.Error("a resource that records its key ARN must not also pin its dedicated key")
	}
}

func TestKMSKeysInUse_IncludesWhatSpecNeeds(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: "orders", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
	}}}}
	if _, ok := kmsKeysInUse(cr, true)["sqs#orders#key"]; !ok {
		t.Error("expected the spec's dedicated key to be in use before its resource exists")
	}
	if len(kmsKeysInUse(cr, false)) != 0 {
		t.Error("expected nothing from spec when it is excluded")
	}
}

func TestRecordKMSKeyARNs_StampsDeclaredResourcesAndTheirDLQ(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", DLQ: true, Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
			}},
			S3: &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
				{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}},
				{Name: "plain"},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			kmsEntry("sqs#orders#key", "arn:key-a", depsv1alpha1.DeletionPolicyDelete),
			kmsEntry("shared", "arn:key-b", depsv1alpha1.DeletionPolicyDelete),
			{Type: "sqs", Name: "orders"},
			{Type: "sqs", Name: "orders#dlq"},
			{Type: "s3", Name: "receipts"},
			{Type: "s3", Name: "plain"},
		}},
	}

	recordKMSKeyARNs(cr)

	for _, c := range []struct{ typ, name, want string }{
		{"sqs", "orders", "arn:key-a"},
		{"sqs", "orders#dlq", "arn:key-a"},
		{"s3", "receipts", "arn:key-b"},
		{"s3", "plain", ""},
	} {
		entry, _ := status.FindManagedResource(cr.Status.ManagedResources, c.typ, c.name)
		if entry.KMSKeyARN != c.want {
			t.Errorf("%s/%s kmsKeyARN = %q, want %q", c.typ, c.name, entry.KMSKeyARN, c.want)
		}
	}
}

// A non-empty encrypted resource removed from spec is held by its own
// non-empty guard; its key must be held with it, not scheduled for deletion
// once the key's quiet window passes.
func TestReconcileNormal_KeyOfARemovedResourceStillHoldingDataIsKept(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/11111111-1111-1111-1111-000000000002"
	longAgo := metav1.NewTime(metav1.Now().Add(-90 * 24 * time.Hour))
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "app", UID: "uid-1", Finalizers: []string{finalizerName}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{
				Type: "kms", Name: "sqs#orders#key", ARN: keyARN,
				State: depsv1alpha1.ManagedResourceStateVerified, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				PendingDeletionSince: &longAgo,
			},
			{
				Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:platform-app-orders",
				State: depsv1alpha1.ManagedResourceStateVerified, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				PendingDeletionSince: &longAgo, KMSKeyARN: keyARN,
			},
		}},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	kmsFake := r.AWSClients.KMS.(*fakeKMSClient)
	kmsFake.keys[keyARN] = &fakeKey{
		arn: keyARN, keyID: "11111111-1111-1111-1111-000000000002", keyState: kmstypes.KeyStateEnabled,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("platform", "app"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	if _, err := r.reconcileNormal(context.Background(), cr); err != nil && !isRetryable(err) {
		t.Logf("reconcileNormal() error = %v (other sections' fakes are minimal)", err)
	}
	if kmsFake.keys[keyARN].keyState == kmstypes.KeyStatePendingDeletion {
		t.Fatal("the key of a resource still holding data must not be scheduled for deletion")
	}
}

// On CR deletion, a key protecting retained data outlives the CR.
func TestFinalize_KeyOfRetainedDataIsRelinquishedNotDeleted(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/11111111-1111-1111-1111-000000000003"
	longAgo := metav1.NewTime(metav1.Now().Add(-90 * 24 * time.Hour))
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "app", UID: "uid-1", Finalizers: []string{finalizerName}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{
				Type: "kms", Name: "shared", ARN: keyARN,
				State: depsv1alpha1.ManagedResourceStateVerified, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				PendingDeletionSince: &longAgo,
			},
			{Type: "s3", Name: "receipts", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, KMSKeyARN: keyARN},
		}},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	kmsFake := r.AWSClients.KMS.(*fakeKMSClient)
	kmsFake.keys[keyARN] = &fakeKey{
		arn: keyARN, keyID: "11111111-1111-1111-1111-000000000003", keyState: kmstypes.KeyStateEnabled,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("platform", "app"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	done, _, err := kmsSection(r, cr.DeepCopy()).finalize(context.Background(), cr)
	if err != nil {
		t.Fatalf("finalize() error = %v", err)
	}
	if kmsFake.keys[keyARN].keyState == kmstypes.KeyStatePendingDeletion {
		t.Fatal("a key protecting retained data must not be deleted with the CR")
	}
	if !done {
		t.Error("a retained key should not hold up the CR's deletion")
	}
}

// On CR deletion, a key whose data is still draining holds the
// finalizer until that data is gone.
func TestFinalize_KeyOfDrainingDataBlocksDeletion(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/11111111-1111-1111-1111-000000000004"
	longAgo := metav1.NewTime(metav1.Now().Add(-90 * 24 * time.Hour))
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "app", UID: "uid-1", Finalizers: []string{finalizerName}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{
				Type: "kms", Name: "dynamodb#sessions#key", ARN: keyARN,
				State: depsv1alpha1.ManagedResourceStateVerified, DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				PendingDeletionSince: &longAgo,
			},
			{Type: "dynamodb", Name: "sessions", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, PendingDeletionSince: &longAgo},
		}},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	kmsFake := r.AWSClients.KMS.(*fakeKMSClient)
	kmsFake.keys[keyARN] = &fakeKey{arn: keyARN, keyID: "11111111-1111-1111-1111-000000000004", keyState: kmstypes.KeyStateEnabled}

	done, blocked, err := kmsSection(r, cr.DeepCopy()).finalize(context.Background(), cr)
	if err != nil {
		t.Fatalf("finalize() error = %v", err)
	}
	if done || len(blocked) == 0 {
		t.Errorf("expected the finalizer to wait for the table, got done=%v blocked=%v", done, blocked)
	}
	if kmsFake.keys[keyARN].keyState == kmstypes.KeyStatePendingDeletion {
		t.Error("the key must not be scheduled while the table encrypted with it still exists")
	}
}
