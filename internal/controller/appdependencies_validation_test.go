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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
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

		It("rejects replication enabled even with a region set, since replication does not exist yet", func() {
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
			err := k8sClient.Create(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replication is not implemented yet"))
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

		ddbCR := func(mutate func(*depsv1alpha1.DynamoDBTableSpec)) *depsv1alpha1.AppDependencies {
			tbl := depsv1alpha1.DynamoDBTableSpec{Name: "sessions", PartitionKey: "userId"}
			mutate(&tbl)
			return &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "validation-invalid-ddb-", Namespace: "default"},
				Spec:       depsv1alpha1.AppDependenciesSpec{DynamoDB: &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{tbl}}},
			}
		}

		saCR := func(mutate func(*depsv1alpha1.AppDependencies)) *depsv1alpha1.AppDependencies {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "validation-invalid-sa-", Namespace: "default"},
				Spec:       depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}},
			}
			mutate(cr)
			return cr
		}

		It("accepts a ServiceAccount name with dots and hyphens, and a consumed resource name up to 256 characters", func() {
			obj := saCR(func(c *depsv1alpha1.AppDependencies) {
				c.Spec.ServiceAccountName = "team-a.worker-1"
				c.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{{Namespace: "team", Name: "producer", ResourceName: strings.Repeat("a", 256)}}
			})
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		})

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
			Entry("more subscriptions than a queue policy has room for", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) {
				for n := range 11 {
					q.SubscribesTo = append(q.SubscribesTo, depsv1alpha1.ConsumeRef{Namespace: "team", Name: "producer", ResourceName: fmt.Sprintf("topic-%d", n)})
				}
			}), "Too many"),
			Entry("an SQS name with a space", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) { q.Name = "my orders" }), "should match"),
			Entry("an SQS name over 80 characters", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) { q.Name = strings.Repeat("a", 81) }), "Too long"),
			Entry("an unknown deletion policy", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) { q.DeletionPolicy = "Purge" }), "Unsupported value"),
			Entry("an uppercase S3 bucket name", s3CR(func(b *depsv1alpha1.S3BucketSpec) { b.Name = "Receipts" }), "should match"),
			Entry("an uppercase ServiceAccount name", saCR(func(c *depsv1alpha1.AppDependencies) { c.Spec.ServiceAccountName = "My-SA" }), "should match"),
			Entry("a ServiceAccount name with an underscore", saCR(func(c *depsv1alpha1.AppDependencies) { c.Spec.ServiceAccountName = "team_sa" }), "should match"),
			Entry("a ServiceAccount name ending in a hyphen", saCR(func(c *depsv1alpha1.AppDependencies) { c.Spec.ServiceAccountName = "team-" }), "should match"),
			Entry("an uppercase network ServiceAccount name", saCR(func(c *depsv1alpha1.AppDependencies) { c.Spec.NetworkServiceAccountName = "DB-Client" }), "should match"),
			Entry("S3 replication that would have a region", s3CR(func(b *depsv1alpha1.S3BucketSpec) {
				b.Replication = &depsv1alpha1.S3ReplicationSpec{Enabled: true, Region: "us-west-2"}
			}), "replication is not implemented yet"),
			Entry("RDS replication that would have a region", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) {
				i.Replication = &depsv1alpha1.RDSReplicationSpec{Enabled: true, Region: "us-west-2"}
			}), "replication is not implemented yet"),
			Entry("a lifecycle rule with an empty id", s3CR(func(b *depsv1alpha1.S3BucketSpec) {
				b.Overrides = &depsv1alpha1.S3Overrides{LifecycleRules: []depsv1alpha1.S3LifecycleRule{{ID: "", ExpirationAfterDays: func() *int32 { d := int32(30); return &d }()}}}
			}), "at least 1 chars long"),
			Entry("a sharedWith namespace that cannot be a namespace", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) {
				q.SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: "Team_A", Name: "consumer"}}
			}), "should match"),
			Entry("a consumes namespace that cannot be a namespace", saCR(func(c *depsv1alpha1.AppDependencies) {
				c.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{{Namespace: "-team", Name: "producer", ResourceName: "orders"}}
			}), "should match"),
			Entry("a CR name so long that the names generated from it are refused", saCR(func(c *depsv1alpha1.AppDependencies) {
				c.GenerateName = ""
				c.Name = strings.Repeat("a", 238)
			}), "limited to 237 characters"),
			Entry("S3 replication without a region", s3CR(func(b *depsv1alpha1.S3BucketSpec) {
				b.Replication = &depsv1alpha1.S3ReplicationSpec{Enabled: true}
			}), "region is required"),
			Entry("a sharedWith entry with an empty namespace", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) {
				q.SharedWith = []depsv1alpha1.SharedWithEntry{{Name: "consumer"}}
			}), "at least 1 chars long"),
			Entry("a sharedWith entry with an empty name", sqsCR(func(q *depsv1alpha1.SQSQueueSpec) {
				q.SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: "team"}}
			}), "at least 1 chars long"),
			Entry("a consumes entry with an empty namespace", saCR(func(c *depsv1alpha1.AppDependencies) {
				c.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{{Name: "producer", ResourceName: "orders"}}
			}), "at least 1 chars long"),
			Entry("a consumes entry with an empty name", saCR(func(c *depsv1alpha1.AppDependencies) {
				c.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{{Namespace: "team", ResourceName: "orders"}}
			}), "at least 1 chars long"),
			Entry("a consumes entry with an empty resourceName", saCR(func(c *depsv1alpha1.AppDependencies) {
				c.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{{Namespace: "team", Name: "producer"}}
			}), "at least 1 chars long"),
			Entry("an empty RDS subnet group name", rdsCR(func(i *depsv1alpha1.RDSInstanceSpec) { i.DBSubnetGroupName = "" }), "at least 1 chars long"),
			Entry("an empty DynamoDB partition key", ddbCR(func(t *depsv1alpha1.DynamoDBTableSpec) { t.PartitionKey = "" }), "at least 1 chars long"),
		)

		It("accepts a CR name at the limit, a DynamoDB deletionProtection setting and a real namespace in a reference", func() {
			Expect(k8sClient.Create(ctx, saCR(func(c *depsv1alpha1.AppDependencies) {
				c.GenerateName = ""
				c.Name = strings.Repeat("b", 237)
			}))).To(Succeed())
			Expect(k8sClient.Create(ctx, ddbCR(func(t *depsv1alpha1.DynamoDBTableSpec) {
				t.DeletionProtection = func() *bool { off := false; return &off }()
			}))).To(Succeed())
			Expect(k8sClient.Create(ctx, saCR(func(c *depsv1alpha1.AppDependencies) {
				c.Spec.SQS.Consumes = []depsv1alpha1.ConsumeRef{{Namespace: "team-a1", Name: "producer", ResourceName: "orders"}}
			}))).To(Succeed())
		})

		It("accepts the same specs once they're valid, so the table is rejecting the right thing", func() {
			Expect(k8sClient.Create(ctx, rdsCR(func(*depsv1alpha1.RDSInstanceSpec) {}))).To(Succeed())
			Expect(k8sClient.Create(ctx, sqsCR(func(*depsv1alpha1.SQSQueueSpec) {}))).To(Succeed())
			Expect(k8sClient.Create(ctx, s3CR(func(*depsv1alpha1.S3BucketSpec) {}))).To(Succeed())
			Expect(k8sClient.Create(ctx, ddbCR(func(*depsv1alpha1.DynamoDBTableSpec) {}))).To(Succeed())
		})

		It("rejects a subnet group grant with an empty subnet group name", func() {
			grant := &depsv1alpha1.RDSSubnetGroupGrant{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "validation-invalid-grant-"},
				Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{AllowedNamespaces: []string{"default"}},
			}
			err := k8sClient.Create(ctx, grant)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least 1 chars long"))
		})
	})
})

// A v0.2.0 CR can hold the cross-CR kmsKeyRef object and KMS sharedWith/
// consumes fields, all removed in v0.3.0. Upgrading the CRD leaves such
// objects stored, so this runs a separate API server through the real
// sequence: install the v0.2.0 CRD, store old-shaped CRs, install the
// current CRD, then do what the manager does.
var _ = Describe("Upgrading the CRD from v0.2.0", func() {
	It("still lists every CR, and refuses only the one with the removed kmsKeyRef form", func() {
		env := &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join("testdata", "upgrade")},
			ErrorIfCRDPathMissing: true,
		}
		if dir := getFirstFoundEnvTestBinaryDir(); dir != "" {
			env.BinaryAssetsDirectory = dir
		}
		upgradeCfg, err := env.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(env.Stop)

		raw, err := client.New(upgradeCfg, client.Options{Scheme: runtime.NewScheme()})
		Expect(err).NotTo(HaveOccurred())
		create := func(name string, spec map[string]any) {
			Expect(raw.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "deps.cloudctl.io/v1alpha1", "kind": "AppDependencies",
				"metadata": map[string]any{"name": name, "namespace": "default"},
				"spec":     spec,
			}})).To(Succeed())
		}
		create("plain", map[string]any{"sqs": map[string]any{"resources": []any{map[string]any{"name": "orders"}}}})
		create("old-producer", map[string]any{"kms": map[string]any{"resources": []any{
			map[string]any{"name": "shared", "sharedWith": []any{map[string]any{"namespace": "other", "name": "consumer"}}},
		}}})
		create("old-consumer", map[string]any{
			"kms": map[string]any{"consumes": []any{map[string]any{"namespace": "default", "name": "old-producer", "resourceName": "shared"}}},
			"sqs": map[string]any{"resources": []any{map[string]any{"name": "events", "encryption": map[string]any{
				"kmsKeyRef": map[string]any{"namespace": "default", "name": "old-producer", "resourceName": "shared"},
			}}}},
		})

		_, err = envtest.InstallCRDs(upgradeCfg, envtest.CRDInstallOptions{
			Paths:              []string{filepath.Join("..", "..", "config", "crd", "bases")},
			ErrorIfPathMissing: true,
		})
		Expect(err).NotTo(HaveOccurred())

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(depsv1alpha1.AddToScheme(scheme)).To(Succeed())
		typed, err := client.New(upgradeCfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		By("listing, which is what the manager's cache does for every CR in the cluster")
		var list depsv1alpha1.AppDependenciesList
		Expect(typed.List(ctx, &list)).To(Succeed(), "one old-shaped CR must not stop the whole list from decoding")
		Expect(list.Items).To(HaveLen(3))

		sqsClient := newFakeSQSClient()
		reconciler := &AppDependenciesReconciler{
			Client: typed, APIReader: typed, Scheme: scheme,
			AWSClients: &cloudctlaws.Clients{
				SQS: sqsClient, SNS: newFakeSNSClient(), IAM: newFakeIAMClient(), KMS: newFakeKMSClient(),
				CloudWatch: newFakeCloudWatchClient(), Region: "us-east-1", AccountID: "123456789012",
			},
			Recorder:        events.NewFakeRecorder(50),
			OIDCProviderARN: "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
			OIDCProviderURL: "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
		}
		reconcileOnce := func(name string) depsv1alpha1.AppDependencies {
			key := types.NamespacedName{Namespace: "default", Name: name}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			var got depsv1alpha1.AppDependencies
			Expect(typed.Get(ctx, key, &got)).To(Succeed())
			return got
		}

		By("refusing the CR that still has the removed form, without creating anything for it")
		consumer := reconcileOnce("old-consumer")
		kmsReady := apimeta.FindStatusCondition(consumer.Status.Conditions, conditionTypeKMSReady)
		Expect(kmsReady).NotTo(BeNil())
		Expect(kmsReady.Status).To(Equal(metav1.ConditionFalse))
		Expect(kmsReady.Reason).To(Equal(reasons.LegacyKMSKeyRef))
		Expect(kmsReady.Message).To(And(ContainSubstring(`sqs resource "events"`), ContainSubstring("kms.resources")))
		Expect(sqsClient.queues).To(BeEmpty(), "an encrypted resource must not be created unencrypted because its key reference was lost")

		By("reconciling a CR that never used the removed fields exactly as before")
		plain := reconcileOnce("plain")
		Expect(apimeta.FindStatusCondition(plain.Status.Conditions, "SQSReady")).NotTo(BeNil())
		Expect(apimeta.FindStatusCondition(plain.Status.Conditions, "SQSReady").Status).To(Equal(metav1.ConditionTrue))
	})

	It("decodes the removed object form of kmsKeyRef without losing the string form", func() {
		var legacy, current depsv1alpha1.EncryptionSpec
		Expect(json.Unmarshal([]byte(`{"enabled":false,"kmsKeyRef":{}}`), &legacy)).To(Succeed())
		Expect(legacy.LegacyKMSKeyRef).To(BeTrue())
		Expect(legacy.KMSKeyRef).To(BeEmpty())

		Expect(json.Unmarshal([]byte(`{"kmsKeyRef":"shared"}`), &current)).To(Succeed())
		Expect(current.LegacyKMSKeyRef).To(BeFalse())
		Expect(current.KMSKeyRef).To(Equal("shared"))

		out, err := json.Marshal(legacy)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(out)).NotTo(ContainSubstring("Legacy"), "the marker is not part of the API")
	})
})
