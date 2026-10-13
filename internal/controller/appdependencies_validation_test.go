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
	"fmt"
	"strings"

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

	Context("SQS fifo immutability", func() {
		It("rejects changing fifo on an existing queue entry", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sqs-fifo-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders", FIFO: true},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.SQS.Resources[0].FIFO = false
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("fifo is immutable"))
		})

		It("defaults fifo to false when omitted", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sqs-fifo-default-",
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
			Expect(obj.Spec.SQS.Resources[0].FIFO).To(BeFalse())
		})
	})

	Context("SNS fifo immutability", func() {
		It("rejects changing fifo on an existing topic entry", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sns-fifo-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SNS: &depsv1alpha1.SNSSpec{
						Resources: []depsv1alpha1.SNSTopicSpec{
							{Name: "events", FIFO: true},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.SNS.Resources[0].FIFO = false
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("fifo is immutable"))
		})

		It("defaults fifo to false when omitted", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-sns-fifo-default-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SNS: &depsv1alpha1.SNSSpec{
						Resources: []depsv1alpha1.SNSTopicSpec{
							{Name: "events"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			Expect(obj.Spec.SNS.Resources[0].FIFO).To(BeFalse())
		})
	})

	Context("EncryptionSpec mutual exclusivity", func() {
		It("rejects enabled and kmsKeyRef set together", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-encryption-exclusive-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{
								Name: "orders",
								Encryption: &depsv1alpha1.EncryptionSpec{
									Enabled:   true,
									KMSKeyRef: "primary",
								},
							},
						},
					},
				},
			}
			err := k8sClient.Create(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("enabled and kmsKeyRef are mutually exclusive"))
		})

		It("accepts enabled alone", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-encryption-enabled-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})

		It("accepts kmsKeyRef alone", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-encryption-ref-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{
								Name: "orders",
								Encryption: &depsv1alpha1.EncryptionSpec{
									KMSKeyRef: "primary",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})
	})

	Context("S3 lifecycle rule transition fields", func() {
		It("rejects transitionStorageClass set without transitionAfterDays", func() {
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-s3-lifecycle-notransition-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					S3: &depsv1alpha1.S3Spec{
						Resources: []depsv1alpha1.S3BucketSpec{
							{
								Name: "receipts",
								Overrides: &depsv1alpha1.S3Overrides{
									LifecycleRules: []depsv1alpha1.S3LifecycleRule{
										{ID: "archive", TransitionStorageClass: "GLACIER"},
									},
								},
							},
						},
					},
				},
			}
			err := k8sClient.Create(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("transitionAfterDays is required when transitionStorageClass is set"))
		})

		It("accepts transitionStorageClass paired with transitionAfterDays", func() {
			days := int32(30)
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-s3-lifecycle-withtransition-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					S3: &depsv1alpha1.S3Spec{
						Resources: []depsv1alpha1.S3BucketSpec{
							{
								Name: "receipts",
								Overrides: &depsv1alpha1.S3Overrides{
									LifecycleRules: []depsv1alpha1.S3LifecycleRule{
										{ID: "archive", TransitionStorageClass: "GLACIER", TransitionAfterDays: &days},
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})

		It("accepts a rule with neither transition field set", func() {
			days := int32(90)
			obj := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "validation-s3-lifecycle-expironly-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					S3: &depsv1alpha1.S3Spec{
						Resources: []depsv1alpha1.S3BucketSpec{
							{
								Name: "receipts",
								Overrides: &depsv1alpha1.S3Overrides{
									LifecycleRules: []depsv1alpha1.S3LifecycleRule{
										{ID: "expire", ExpirationAfterDays: &days},
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})
	})

	Context("RDS immutable fields", func() {
		newRDS := func(generateName string, mutate func(*depsv1alpha1.RDSInstanceSpec)) *depsv1alpha1.AppDependencies {
			inst := depsv1alpha1.RDSInstanceSpec{
				Name:              "orders-db",
				DBSubnetGroupName: "prod-private-data-tier",
				Engine:            "postgres",
				EngineVersion:     "16.3",
				InstanceClass:     "db.t4g.micro",
				AllocatedStorage:  20,
			}
			if mutate != nil {
				mutate(&inst)
			}
			return &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: generateName, Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{
					RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{inst}},
				},
			}
		}

		It("rejects changing the engine", func() {
			obj := newRDS("validation-rds-engine-", nil)
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].Engine = "mysql"
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("engine is immutable"))
		})

		It("rejects changing the DB subnet group", func() {
			obj := newRDS("validation-rds-subnet-", nil)
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].DBSubnetGroupName = "other-tier"
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("dbSubnetGroupName is immutable"))
		})

		It("rejects adding encryption to an unencrypted instance", func() {
			obj := newRDS("validation-rds-enc-add-", nil)
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].Encryption = &depsv1alpha1.EncryptionSpec{Enabled: true}
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("encryption is immutable"))
		})

		It("rejects removing encryption", func() {
			obj := newRDS("validation-rds-enc-remove-", func(i *depsv1alpha1.RDSInstanceSpec) {
				i.Encryption = &depsv1alpha1.EncryptionSpec{Enabled: true}
			})
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].Encryption = nil
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("encryption is immutable"))
		})

		It("rejects switching the key reference", func() {
			obj := newRDS("validation-rds-enc-key-", func(i *depsv1alpha1.RDSInstanceSpec) {
				i.Encryption = &depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}
			})
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].Encryption = &depsv1alpha1.EncryptionSpec{KMSKeyRef: "other"}
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("encryption is immutable"))
		})

		It("still allows changing mutable fields", func() {
			obj := newRDS("validation-rds-mutable-", func(i *depsv1alpha1.RDSInstanceSpec) {
				i.Encryption = &depsv1alpha1.EncryptionSpec{Enabled: true}
			})
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].InstanceClass = "db.t4g.small"
			obj.Spec.RDS.Resources[0].AllocatedStorage = 40
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		})

		It("rejects decreasing allocatedStorage", func() {
			obj := newRDS("validation-rds-storage-", func(i *depsv1alpha1.RDSInstanceSpec) { i.AllocatedStorage = 40 })
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			obj.Spec.RDS.Resources[0].AllocatedStorage = 20
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("allocatedStorage cannot be decreased"))
		})
	})

	Context("invalid specs are rejected at admission", func() {
		validRDS := func() depsv1alpha1.RDSInstanceSpec {
			return depsv1alpha1.RDSInstanceSpec{
				Name: "orders-db", DBSubnetGroupName: "prod-private-data-tier",
				Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
			}
		}
		rdsCR := func(mutate func(*depsv1alpha1.RDSInstanceSpec)) *depsv1alpha1.AppDependencies {
			inst := validRDS()
			mutate(&inst)
			return &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "validation-invalid-rds-", Namespace: "default"},
				Spec:       depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{inst}}},
			}
		}
		sqsCR := func(mutate func(*depsv1alpha1.SQSQueueSpec)) *depsv1alpha1.AppDependencies {
			q := depsv1alpha1.SQSQueueSpec{Name: "orders"}
			mutate(&q)
			return &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "validation-invalid-sqs-", Namespace: "default"},
				Spec:       depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{q}}},
			}
		}
		s3CR := func(mutate func(*depsv1alpha1.S3BucketSpec)) *depsv1alpha1.AppDependencies {
			b := depsv1alpha1.S3BucketSpec{Name: "receipts"}
			mutate(&b)
			return &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "validation-invalid-s3-", Namespace: "default"},
				Spec:       depsv1alpha1.AppDependenciesSpec{S3: &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{b}}},
			}
		}

		DescribeTable("rejects",
			func(obj *depsv1alpha1.AppDependencies, wantMessage string) {
				err := k8sClient.Create(ctx, obj)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(wantMessage))
			},
			Entry("an unsupported RDS engine", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.Engine = "oracle" }), "Unsupported value"),
			Entry("a non-numeric engine version", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.EngineVersion = "16.x" }), "should match"),
			Entry("an instance class that isn't a db.* class", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.InstanceClass = "t4g.micro" }), "should match"),
			Entry("storage below the minimum", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.AllocatedStorage = 10 }), "greater than or equal to 20"),
			Entry("storage above the maximum", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.AllocatedStorage = 70000 }), "less than or equal to 65536"),
			Entry("an RDS name with an underscore", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.Name = "orders_db" }), "should match"),
			Entry("an RDS name over 63 characters", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.Name = strings.Repeat("a", 64) }), "Too long"),
			Entry("a dedicated key together with a key reference", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) {
				i.Encryption = &depsv1alpha1.EncryptionSpec{Enabled: true, KMSKeyRef: "shared"}
			}), "mutually exclusive"),
			Entry("an RDS key reference with a path traversal", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) {
				i.Encryption = &depsv1alpha1.EncryptionSpec{KMSKeyRef: "../shared key"}
			}), "should match"),
			Entry("more sharedWith entries than allowed", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) {
				for n := range 21 {
					i.SharedWith = append(i.SharedWith, depsv1alpha1.SharedWithEntry{Namespace: "team", Name: fmt.Sprintf("consumer-%d", n)})
				}
			}), "Too many"),
			Entry("an SQS name with a space", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) { q.Name = "my orders" }), "should match"),
			Entry("an SQS name over 80 characters", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) { q.Name = strings.Repeat("a", 81) }), "Too long"),
			Entry("an unknown deletion policy", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) { q.DeletionPolicy = "Purge" }), "Unsupported value"),
			Entry("an uppercase S3 bucket name", s3CR(func(b *depsv1alpha1.S3BucketSpec) { b.Name = "Receipts" }), "should match"),
			Entry("S3 replication without a region", s3CR(func(b *depsv1alpha1.S3BucketSpec) {
				b.Replication = &depsv1alpha1.S3ReplicationSpec{Enabled: true}
			}), "region is required"),
		)

		It("accepts the same specs once they're valid, so the table is rejecting the right thing", func() {
			Expect(k8sClient.Create(ctx, rdsCR(func(*depsv1alpha1.RDSInstanceSpec) {}))).To(Succeed())
			Expect(k8sClient.Create(ctx, sqsCR(func(*depsv1alpha1.SQSQueueSpec) {}))).To(Succeed())
			Expect(k8sClient.Create(ctx, s3CR(func(*depsv1alpha1.S3BucketSpec) {}))).To(Succeed())
		})
	})
})
