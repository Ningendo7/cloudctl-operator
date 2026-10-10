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

package kms

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func mustListAllCRs(t *testing.T, k8sClient client.Client) []depsv1alpha1.AppDependencies {
	t.Helper()
	crs, err := listAllCRs(context.Background(), k8sClient)
	if err != nil {
		t.Fatalf("listAllCRs() error = %v", err)
	}
	return crs
}

func consumerWithKeyRef(namespace, crName string, ref *depsv1alpha1.ConsumeRef) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName},
		Spec: depsv1alpha1.AppDependenciesSpec{
			RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
				{Name: "orders-db", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true, KMSKeyRef: ref}},
			}},
		},
	}
}

func TestHasActiveConsumer_FindsRDSConsumer(t *testing.T) {
	consumer := consumerWithKeyRef("team-b", "fulfillment-service", &depsv1alpha1.ConsumeRef{
		Namespace: "team-a", Name: "checkout-service", ResourceName: "primary",
	})
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).WithObjects(consumer).Build()

	if !HasActiveConsumer(mustListAllCRs(t, k8sClient), "team-a", "checkout-service", "primary") {
		t.Error("expected a consumer whose encryption.kmsKeyRef points at this key to be found")
	}
}

func TestHasActiveConsumer_IgnoresUnrelatedKeyRef(t *testing.T) {
	consumer := consumerWithKeyRef("team-b", "fulfillment-service", &depsv1alpha1.ConsumeRef{
		Namespace: "team-a", Name: "checkout-service", ResourceName: "some-other-key",
	})
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).WithObjects(consumer).Build()

	if HasActiveConsumer(mustListAllCRs(t, k8sClient), "team-a", "checkout-service", "primary") {
		t.Error("expected no match for a kmsKeyRef pointing at a different key")
	}
}

func TestHasActiveConsumer_IgnoresNilEncryptionAndNilKeyRef(t *testing.T) {
	noEncryption := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "no-encryption"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db"},
		}}},
	}
	dedicatedKeyOnly := consumerWithKeyRef("team-b", "dedicated-only", nil)
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).
		WithObjects(noEncryption, dedicatedKeyOnly).Build()

	if HasActiveConsumer(mustListAllCRs(t, k8sClient), "team-a", "checkout-service", "primary") {
		t.Error("expected no match when nothing declares a kmsKeyRef at all")
	}
}

func TestHasActiveConsumer_ChecksEveryResourceType(t *testing.T) {
	ref := &depsv1alpha1.ConsumeRef{Namespace: "team-a", Name: "checkout-service", ResourceName: "primary"}
	tests := []struct {
		name string
		cr   *depsv1alpha1.AppDependencies
	}{
		{name: "sqs", cr: &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "consumer"},
			Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true, KMSKeyRef: ref}},
			}}},
		}},
		{name: "sns", cr: &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "consumer"},
			Spec: depsv1alpha1.AppDependenciesSpec{SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{
				{Name: "events", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true, KMSKeyRef: ref}},
			}}},
		}},
		{name: "dynamodb", cr: &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "consumer"},
			Spec: depsv1alpha1.AppDependenciesSpec{DynamoDB: &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
				{Name: "sessions", PartitionKey: "id", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true, KMSKeyRef: ref}},
			}}},
		}},
		{name: "s3", cr: &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "consumer"},
			Spec: depsv1alpha1.AppDependenciesSpec{S3: &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
				{Name: "receipts", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true, KMSKeyRef: ref}},
			}}},
		}},
		{name: "rds", cr: consumerWithKeyRef("team-b", "consumer", ref)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).WithObjects(tc.cr).Build()
			if !HasActiveConsumer(mustListAllCRs(t, k8sClient), "team-a", "checkout-service", "primary") {
				t.Errorf("expected a %s resource's kmsKeyRef to be found", tc.name)
			}
		})
	}
}

func TestHasActiveConsumer_NoCRsAtAll(t *testing.T) {
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).Build()

	if HasActiveConsumer(mustListAllCRs(t, k8sClient), "team-a", "checkout-service", "primary") {
		t.Error("expected no match with no CRs in the cluster at all")
	}
}
