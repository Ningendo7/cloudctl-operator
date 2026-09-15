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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// These specs exercise the CRD's CEL validation rules against a real
// envtest API server (not just Go-level struct checks), since CEL rules are
// enforced by the apiserver at admission time.
var _ = Describe("AppDependencies CRD validation", func() {

	Context("DynamoDB partitionKey immutability", func() {
		It("rejects changing partitionKey on an existing table entry", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-dynamodb-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					DynamoDB: &depsv1alpha1.DynamoDBSpec{
						Resources: []depsv1alpha1.DynamoDBTableSpec{
							{Name: "sessions", PartitionKey: "userId"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.DynamoDB.Resources[0].PartitionKey = "accountId"
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("partitionKey is immutable"))
		})

		It("rejects changing sortKey on an existing table entry", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-dynamodb-sortkey-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					DynamoDB: &depsv1alpha1.DynamoDBSpec{
						Resources: []depsv1alpha1.DynamoDBTableSpec{
							{Name: "sessions", PartitionKey: "userId", SortKey: "createdAt"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.DynamoDB.Resources[0].SortKey = "updatedAt"
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sortKey is immutable"))
		})

		It("allows changing fields other than the key schema", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-dynamodb-mutable-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					DynamoDB: &depsv1alpha1.DynamoDBSpec{
						Resources: []depsv1alpha1.DynamoDBTableSpec{
							{Name: "sessions", PartitionKey: "userId"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.DynamoDB.Resources[0].Backup = &depsv1alpha1.DynamoDBBackupSpec{Enabled: true}
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		})
	})

	Context("S3 replication region requirement", func() {
		It("rejects replication enabled with no region set", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-s3-noregion-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					S3: &depsv1alpha1.S3Spec{
						Resources: []depsv1alpha1.S3BucketSpec{
							{
								Name:        "receipts",
								Replication: &depsv1alpha1.S3ReplicationSpec{Enabled: true},
							},
						},
					},
				},
			}
			err := k8sClient.Create(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("region is required when replication is enabled"))
		})

		It("accepts replication enabled with a region set", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-s3-withregion-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					S3: &depsv1alpha1.S3Spec{
						Resources: []depsv1alpha1.S3BucketSpec{
							{
								Name: "receipts",
								Replication: &depsv1alpha1.S3ReplicationSpec{
									Enabled: true,
									Region:  "us-west-2",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})

		It("accepts replication disabled with no region set", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-s3-disabled-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					S3: &depsv1alpha1.S3Spec{
						Resources: []depsv1alpha1.S3BucketSpec{
							{
								Name:        "receipts",
								Replication: &depsv1alpha1.S3ReplicationSpec{Enabled: false},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})
	})

	Context("deletionPolicy default", func() {
		It("defaults an SQS queue's deletionPolicy to Retain when omitted", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sqs-default-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			Expect(obj.Spec.SQS.Resources[0].DeletionPolicy).To(Equal(depsv1alpha1.DeletionPolicyRetain))
		})

		It("rejects an invalid deletionPolicy value", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sqs-badpolicy-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders", DeletionPolicy: "Purge"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})
	})

	Context("resource name uniqueness within a section", func() {
		It("rejects two sqs resources with the same name", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sqs-duplicate-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders"},
							{Name: "orders"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})
	})
})
