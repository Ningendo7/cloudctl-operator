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

package iam

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func newFakeK8sClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func TestCollectGrants_OwnedResourceAlwaysGrantedRegardlessOfSharing(t *testing.T) {
	// Directly validates that owned resources are never gated behind
	// sharedWith/consumes - declaring a dependency as your own already
	// implies needing to use it.
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:default-checkout-service-orders"},
			},
		},
	}

	grants, skipped := collectGrants(context.Background(), newFakeK8sClient(), cr)
	if len(skipped) != 0 {
		t.Fatalf("expected no skipped entries, got %v", skipped)
	}
	if len(grants) != 1 || grants[0].resourceType != "sqs" || !grants[0].readWrite {
		t.Errorf("expected one full-access sqs grant, got %+v", grants)
	}
}

func TestCollectGrants_OwnedResourceWithoutLedgerEntryIsSkippedWithReason(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		// No ManagedResources - as if creation hasn't succeeded yet.
	}

	grants, skipped := collectGrants(context.Background(), newFakeK8sClient(), cr)
	if len(grants) != 0 {
		t.Errorf("expected no grants for an unreconciled owned resource, got %+v", grants)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0], "orders") {
		t.Errorf("expected a skip reason naming the unreconciled resource, got %v", skipped)
	}
}

func producerCR(namespace, name string, sharedWith []depsv1alpha1.SharedWithEntry, arn string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", SharedWith: sharedWith},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: arn},
			},
		},
	}
}

func TestCollectGrants_ConsumedResourceGrantedWhenSharedWithMatches(t *testing.T) {
	producer := producerCR("default", "checkout-service", []depsv1alpha1.SharedWithEntry{
		{Namespace: "default", Name: "fulfillment-service", Access: depsv1alpha1.AccessLevelReadWrite},
	}, "arn:aws:sqs:us-east-1:123456789012:default-checkout-service-orders")

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	grants, skipped := collectGrants(context.Background(), newFakeK8sClient(producer), consumer)
	if len(skipped) != 0 {
		t.Fatalf("expected no skipped entries, got %v", skipped)
	}
	if len(grants) != 1 || !grants[0].readWrite {
		t.Errorf("expected one ReadWrite sqs grant, got %+v", grants)
	}
}

func TestCollectGrants_ConsumedResourceDefaultsToReadOnly(t *testing.T) {
	producer := producerCR("default", "checkout-service", []depsv1alpha1.SharedWithEntry{
		{Namespace: "default", Name: "fulfillment-service"}, // Access left unset
	}, "arn:aws:sqs:us-east-1:123456789012:default-checkout-service-orders")

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	grants, _ := collectGrants(context.Background(), newFakeK8sClient(producer), consumer)
	if len(grants) != 1 || grants[0].readWrite {
		t.Errorf("expected an unset Access to default to ReadOnly, got %+v", grants)
	}
}

func TestCollectGrants_SkipsWhenProducerNotFound(t *testing.T) {
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	grants, skipped := collectGrants(context.Background(), newFakeK8sClient(), consumer)
	if len(grants) != 0 {
		t.Errorf("expected no grants when the producer doesn't exist, got %+v", grants)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0], "not found") {
		t.Errorf("expected a 'producer CR not found' reason, got %v", skipped)
	}
}

func TestCollectGrants_SkipsWhenResourceNotDeclaredOnProducer(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec:       depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{}}, // no "orders" declared
	}
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	_, skipped := collectGrants(context.Background(), newFakeK8sClient(producer), consumer)
	if len(skipped) != 1 || !strings.Contains(skipped[0], "not declared") {
		t.Errorf("expected a 'resource not declared' reason, got %v", skipped)
	}
}

func TestCollectGrants_SkipsWhenNotAuthorized(t *testing.T) {
	producer := producerCR("default", "checkout-service", nil, "arn:aws:sqs:us-east-1:123456789012:default-checkout-service-orders")
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	_, skipped := collectGrants(context.Background(), newFakeK8sClient(producer), consumer)
	if len(skipped) != 1 || !strings.Contains(skipped[0], "not authorized") {
		t.Errorf("expected a 'not authorized' reason, got %v", skipped)
	}
}

func TestCollectGrants_SkipsWhenProducerResourceNotYetReconciled(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "default", Name: "fulfillment-service"}}},
			}},
		},
		// No ManagedResources yet.
	}
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fulfillment-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "default", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	_, skipped := collectGrants(context.Background(), newFakeK8sClient(producer), consumer)
	if len(skipped) != 1 || !strings.Contains(skipped[0], "not reconciled") {
		t.Errorf("expected a 'not reconciled' reason, got %v", skipped)
	}
}

func TestBuildPolicyDocument_S3ProducesBucketAndObjectStatements(t *testing.T) {
	doc, err := buildPolicyDocument([]grant{{resourceType: "s3", arn: "arn:aws:s3:::my-bucket", readWrite: false}})
	if err != nil {
		t.Fatalf("buildPolicyDocument() error = %v", err)
	}

	var parsed policyDocument
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("failed to parse generated policy: %v", err)
	}
	if len(parsed.Statement) != 2 {
		t.Fatalf("expected two statements (bucket-level + object-level) for one s3 grant, got %d", len(parsed.Statement))
	}

	var bucketStmt, objectStmt *policyStatement
	for i := range parsed.Statement {
		if parsed.Statement[i].Resource[0] == "arn:aws:s3:::my-bucket" {
			bucketStmt = &parsed.Statement[i]
		} else if parsed.Statement[i].Resource[0] == "arn:aws:s3:::my-bucket/*" {
			objectStmt = &parsed.Statement[i]
		}
	}
	if bucketStmt == nil || objectStmt == nil {
		t.Fatalf("expected one bucket-scoped and one object-scoped statement, got %+v", parsed.Statement)
	}
	if !containsAction(bucketStmt.Action, "s3:ListBucket") {
		t.Error("expected ListBucket on the bucket-level statement")
	}
	if containsAction(objectStmt.Action, "s3:ListBucket") {
		t.Error("expected ListBucket to NOT appear on the object-level statement")
	}
	if !containsAction(objectStmt.Action, "s3:GetObject") {
		t.Error("expected GetObject on the object-level statement")
	}
	if containsAction(objectStmt.Action, "s3:PutObject") {
		t.Error("expected PutObject to be excluded for a ReadOnly grant")
	}
}

func TestBuildPolicyDocument_ReadWriteAddsWriteActions(t *testing.T) {
	doc, err := buildPolicyDocument([]grant{{resourceType: "sqs", arn: "arn:aws:sqs:us-east-1:123456789012:orders", readWrite: true}})
	if err != nil {
		t.Fatalf("buildPolicyDocument() error = %v", err)
	}
	var parsed policyDocument
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("failed to parse generated policy: %v", err)
	}
	if len(parsed.Statement) != 1 {
		t.Fatalf("expected one statement for one sqs grant, got %d", len(parsed.Statement))
	}
	if !containsAction(parsed.Statement[0].Action, "sqs:SendMessage") {
		t.Error("expected SendMessage to be included for a ReadWrite grant")
	}
	if !containsAction(parsed.Statement[0].Action, "sqs:DeleteMessage") {
		t.Error("expected DeleteMessage (baseline) to still be included alongside ReadWrite actions")
	}
}

func TestBuildPolicyDocument_ReadOnlyExcludesWriteActions(t *testing.T) {
	doc, err := buildPolicyDocument([]grant{{resourceType: "sqs", arn: "arn:aws:sqs:us-east-1:123456789012:orders", readWrite: false}})
	if err != nil {
		t.Fatalf("buildPolicyDocument() error = %v", err)
	}
	var parsed policyDocument
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("failed to parse generated policy: %v", err)
	}
	if containsAction(parsed.Statement[0].Action, "sqs:SendMessage") {
		t.Error("expected SendMessage to be excluded for a ReadOnly grant")
	}
}

func containsAction(actions []string, want string) bool {
	for _, a := range actions {
		if a == want {
			return true
		}
	}
	return false
}
