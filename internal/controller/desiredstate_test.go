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
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/observability"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

var _ = Describe("AppDependencies Controller", func() {
	var (
		fakeSQS        *fakeSQSClient
		fakeSNS        *fakeSNSClient
		fakeIAM        *fakeIAMClient
		fakeKMS        *fakeKMSClient
		fakeCloudWatch *fakeCloudWatchClient
		fakeRDS        *fakeRDSClient
		fakeEC2        *fakeEC2Client
		fakeRecorder   *events.FakeRecorder
		reconciler     *AppDependenciesReconciler
	)

	BeforeEach(func() {
		fakeSQS = newFakeSQSClient()
		fakeSNS = newFakeSNSClient()
		fakeIAM = newFakeIAMClient()
		fakeKMS = newFakeKMSClient()
		fakeCloudWatch = newFakeCloudWatchClient()
		fakeRDS = newFakeRDSClient()
		fakeEC2 = newFakeEC2Client()
		fakeRecorder = events.NewFakeRecorder(20)
		reconciler = &AppDependenciesReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			AWSClients: &cloudctlaws.Clients{
				SQS: fakeSQS, SNS: fakeSNS, IAM: fakeIAM, KMS: fakeKMS, CloudWatch: fakeCloudWatch,
				RDS: fakeRDS, EC2: fakeEC2,
				Region: "us-east-1", AccountID: "123456789012",
			},
			Recorder:        fakeRecorder,
			OIDCProviderARN: "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
			OIDCProviderURL: "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
		}
	})

	Context("reconciling a new CR with a declared queue", func() {
		It("adds the finalizer, creates the queue, and reports Ready", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-create-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())
			Expect(updated.Finalizers).To(ContainElement(finalizerName))

			queueName := cloudctlaws.ResourceName(updated.Namespace, updated.Name, "sqs", "orders", 80)
			Expect(fakeSQS.queues).To(HaveKey(queueName))

			ready := apimeta.FindStatusCondition(updated.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))

			sqsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "SQSReady")
			Expect(sqsReady).NotTo(BeNil())
			Expect(sqsReady.Status).To(Equal(metav1.ConditionTrue))
		})

		It("traces a reconcile as one tree: root, finalizer, every section, connection info, status patch", func() {
			recorder := useSpanRecorder(GinkgoT())
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "controller-traced-", Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			spans := recorder.Ended()
			root := spanNamed(GinkgoT(), spans, "Reconcile")
			Expect(root.Parent().IsValid()).To(BeFalse(), "the reconcile is the root of its trace")

			names := []string{
				"ensure finalizer", "patch status", "reconcile connectioninfo",
				"reconcile kms", "reconcile sqs", "reconcile sns", "reconcile dynamodb",
				"reconcile s3", "reconcile rds", "reconcile alarms", "reconcile iam",
			}
			for _, name := range names {
				child := spanNamed(GinkgoT(), spans, name)
				Expect(child.SpanContext().TraceID()).To(Equal(root.SpanContext().TraceID()), "%s should be in the reconcile's trace", name)
				Expect(child.Parent().SpanID()).To(Equal(root.SpanContext().SpanID()), "%s should be a direct child of the reconcile", name)
			}

			// The section's span has to be what its AWS calls run under, or
			// they would float outside the trace.
			sqsSection := spanNamed(GinkgoT(), spans, "reconcile sqs")
			Expect(fakeSQS.createQueueSpan.SpanID()).To(Equal(sqsSection.SpanContext().SpanID()),
				"CreateQueue should run under the sqs section's span")
		})

		It("counts and times every section it runs", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "controller-metrics-", Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			sections := []string{"kms", "sqs", "sns", "dynamodb", "s3", "rds", "alarms", "iam"}
			durations := map[string]uint64{}
			for _, section := range sections {
				durations[section] = histogramCount(GinkgoT(), observability.SectionReconcileDuration, section)
			}
			sqsReady := counterValue(GinkgoT(), observability.SectionReconciles, "sqs", observability.SectionReady)

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			for _, section := range sections {
				Expect(histogramCount(GinkgoT(), observability.SectionReconcileDuration, section)-durations[section]).
					To(Equal(uint64(1)), "section %s should be timed exactly once per reconcile pass", section)
			}
			Expect(counterValue(GinkgoT(), observability.SectionReconciles, "sqs", observability.SectionReady) - sqsReady).
				To(Equal(1.0))
		})
	})

	Context("reconciling a new CR with a declared KMS key", func() {
		It("creates the key, aliases it, enables rotation, and reports Ready", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-kms-create-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					KMS: &depsv1alpha1.KMSSpec{
						Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())

			alias := "alias/" + cloudctlaws.ResourceName(updated.Namespace, updated.Name, "kms", "primary", 256-len("alias/"))
			Expect(fakeKMS.aliases).To(HaveKey(alias))

			entry, _ := status.FindManagedResource(updated.Status.ManagedResources, "kms", "primary")
			Expect(entry).NotTo(BeNil())
			Expect(entry.State).To(Equal(depsv1alpha1.ManagedResourceStateVerified))
			Expect(fakeKMS.aliases[alias]).To(Equal(entry.ARN))

			kmsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "KMSReady")
			Expect(kmsReady).NotTo(BeNil())
			Expect(kmsReady.Status).To(Equal(metav1.ConditionTrue))

			ready := apimeta.FindStatusCondition(updated.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))

			Expect(fakeRecorder.Events).To(Receive(ContainSubstring("KeyCreated")))
		})
	})

	Context("reconciling with an AWS permission error", func() {
		It("sets SQSReady=False with reason PermissionDenied", func() {
			fakeSQS.createQueueErr = &fakeAWSError{code: "AccessDenied"}

			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-permdenied-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).To(HaveOccurred())

			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())

			sqsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "SQSReady")
			Expect(sqsReady).NotTo(BeNil())
			Expect(sqsReady.Status).To(Equal(metav1.ConditionFalse))
			Expect(sqsReady.Reason).To(Equal("PermissionDenied"))
		})
	})

	Context("deleting a CR", func() {
		It("removes the finalizer once the queue is empty and deletionPolicy is Delete", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-delete-clean-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var created depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &created)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &created)).To(Succeed())

			// First reconcile after deletion only enters the mandatory
			// quiet window (see sqs.deletionQuietWindow) - even an empty
			// queue isn't deleted on the very first pass it comes up for
			// deletion, so the finalizer must still be present here.
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var pending depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &pending)).To(Succeed())
			Expect(pending.Finalizers).To(ContainElement(finalizerName))

			// Push the ledger entry's PendingDeletionSince into the past so
			// the next reconcile evaluates the real emptiness check instead
			// of just holding through the quiet window.
			entry, _ := status.FindManagedResource(pending.Status.ManagedResources, "sqs", "orders")
			Expect(entry).NotTo(BeNil())
			past := metav1.NewTime(time.Now().Add(-1 * time.Hour))
			entry.PendingDeletionSince = &past
			status.UpsertManagedResource(&pending.Status.ManagedResources, entry)
			Expect(k8sClient.Status().Update(ctx, &pending)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, req.NamespacedName, &depsv1alpha1.AppDependencies{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			queueName := cloudctlaws.ResourceName(cr.Namespace, cr.Name, "sqs", "orders", 80)
			Expect(fakeSQS.queues).NotTo(HaveKey(queueName))
		})

		It("keeps the finalizer when a Delete-policy queue is still non-empty", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-delete-pending-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{
						Resources: []depsv1alpha1.SQSQueueSpec{
							{Name: "orders", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			queueName := cloudctlaws.ResourceName(cr.Namespace, cr.Name, "sqs", "orders", 80)
			fakeSQS.queues[queueName].approxMessages = "5"

			var created depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &created)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &created)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var stillThere depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &stillThere)).To(Succeed())
			Expect(stillThere.Finalizers).To(ContainElement(finalizerName))
			Expect(fakeSQS.queues).To(HaveKey(queueName))
		})
	})

	Context("reconciling a new CR with a declared topic", func() {
		It("creates the topic and reports Ready via SNSReady and the aggregate condition", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-sns-create-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SNS: &depsv1alpha1.SNSSpec{
						Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())

			topicArn := cloudctlaws.TopicARN("us-east-1", "123456789012", cloudctlaws.ResourceName(updated.Namespace, updated.Name, "sns", "events", 256))
			Expect(fakeSNS.topics).To(HaveKey(topicArn))

			snsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "SNSReady")
			Expect(snsReady).NotTo(BeNil())
			Expect(snsReady.Status).To(Equal(metav1.ConditionTrue))

			ready := apimeta.FindStatusCondition(updated.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("deleting a CR with a topic that has active subscriptions", func() {
		It("keeps the finalizer until the topic is safe to delete", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "controller-sns-delete-pending-",
					Namespace:    "default",
				},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SNS: &depsv1alpha1.SNSSpec{
						Resources: []depsv1alpha1.SNSTopicSpec{
							{Name: "events", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			topicArn := cloudctlaws.TopicARN("us-east-1", "123456789012", cloudctlaws.ResourceName(cr.Namespace, cr.Name, "sns", "events", 256))
			fakeSNS.topics[topicArn].subscriptions = 1

			var created depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &created)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &created)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var stillThere depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &stillThere)).To(Succeed())
			Expect(stillThere.Finalizers).To(ContainElement(finalizerName))
			Expect(fakeSNS.topics).To(HaveKey(topicArn))
		})
	})

	Context("reconciling a new CR with a declared RDS instance", func() {
		It("creates the instance and its security group, and reports Ready", func() {
			grant := &depsv1alpha1.RDSSubnetGroupGrant{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "rds-grant-happy-"},
				Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "happy-path-subnet-group", AllowedNamespaces: []string{"default"}},
			}
			Expect(k8sClient.Create(ctx, grant)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, grant) })

			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "controller-rds-create-", Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
					Name: "orders-db", DBSubnetGroupName: "happy-path-subnet-group",
					Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
				}}}},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			instanceID := cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, cr.Namespace, cr.Name, "rds", "orders-db", 63)
			Expect(fakeRDS.instances).To(HaveKey(instanceID))
			Expect(fakeEC2.groupCount()).NotTo(BeZero())

			// First pass only ever creates and marks Creating - same
			// two-pass create-then-verify behavior as every other
			// resource type here. A second pass is needed to see Verified.
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())

			entry, _ := status.FindManagedResource(updated.Status.ManagedResources, resourceTypeRDS, "orders-db")
			Expect(entry).NotTo(BeNil())
			Expect(entry.State).To(Equal(depsv1alpha1.ManagedResourceStateVerified))

			rdsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "RDSReady")
			Expect(rdsReady).NotTo(BeNil())
			Expect(rdsReady.Status).To(Equal(metav1.ConditionTrue))

			ready := apimeta.FindStatusCondition(updated.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		})

		// Within one reconcile pass, the same instance's live state is read
		// from three independent call sites (rdsSection's own status
		// check, the generated ConfigMap's connection-info delivery, and
		// the mirrored credentials Secret's delivery) - all three
		// ultimately resolve the exact same instance. See
		// rds.WithDescribeCache.
		It("shares one DescribeDBInstances call per reconcile pass instead of one per call site", func() {
			grant := &depsv1alpha1.RDSSubnetGroupGrant{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "rds-grant-describecache-"},
				Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "describecache-subnet-group", AllowedNamespaces: []string{"default"}},
			}
			Expect(k8sClient.Create(ctx, grant)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, grant) })

			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "controller-rds-describecache-", Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
					Name: "orders-db", DBSubnetGroupName: "describecache-subnet-group",
					Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
				}}}},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			// First two passes create, then verify ownership - get past
			// both before measuring the steady-state pass below, so this
			// isn't conflated with that unrelated two-pass shape.
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			instanceID := cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, cr.Namespace, cr.Name, "rds", "orders-db", 63)
			callsBefore := fakeRDS.describeDBInstancesCalls[instanceID]

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			callsDuringThisPass := fakeRDS.describeDBInstancesCalls[instanceID] - callsBefore
			Expect(callsDuringThisPass).To(Equal(1), "expected rdsSection's own status check, the ConfigMap's connection-info delivery, and the Secret's credentials delivery to share one DescribeDBInstances call")
		})
	})

	Context("reconciling a declared RDS instance with no authorizing grant", func() {
		It("reports RDSReady=False without creating anything in AWS", func() {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "controller-rds-unauthorized-", Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
					Name: "orders-db", DBSubnetGroupName: "no-grant-subnet-group",
					Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
				}}}},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			// A retryable ReconcileError, so Reconcile itself reports no Go
			// error (it requeues internally) - only the condition reflects
			// the problem.
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())
			Expect(fakeRDS.instances).To(BeEmpty())

			rdsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "RDSReady")
			Expect(rdsReady).NotTo(BeNil())
			Expect(rdsReady.Status).To(Equal(metav1.ConditionFalse))
		})
	})

	Context("an RDS instance removed from spec and left stuck past the grace period", func() {
		It("surfaces InstanceStuckPendingDeletion without Reconcile itself returning an error", func() {
			grant := &depsv1alpha1.RDSSubnetGroupGrant{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "rds-grant-stuck-"},
				Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "stuck-subnet-group", AllowedNamespaces: []string{"default"}},
			}
			Expect(k8sClient.Create(ctx, grant)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, grant) })

			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "controller-rds-stuck-", Namespace: "default"},
				Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
					Name: "orders-db", DBSubnetGroupName: "stuck-subnet-group",
					Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
					DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				}}}},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Remove the instance from spec entirely - the next reconcile
			// marks it pending deletion.
			var updated depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())
			updated.Spec.RDS.Resources = nil
			Expect(k8sClient.Update(ctx, &updated)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())
			entry, _ := status.FindManagedResource(updated.Status.ManagedResources, resourceTypeRDS, "orders-db")
			Expect(entry).NotTo(BeNil())
			Expect(entry.PendingDeletionSince).NotTo(BeNil())

			// Backdate it directly in status, well past both
			// deletionQuietWindow and PendingDeletionGracePeriod, exactly
			// as a human would observe after enough real time passed.
			past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
			entry.PendingDeletionSince = &past
			status.UpsertManagedResource(&updated.Status.ManagedResources, entry)
			Expect(k8sClient.Status().Update(ctx, &updated)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, req.NamespacedName, &updated)).To(Succeed())
			rdsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "RDSReady")
			Expect(rdsReady).NotTo(BeNil())
			Expect(rdsReady.Status).To(Equal(metav1.ConditionFalse))
			Expect(rdsReady.Reason).To(Equal("InstanceStuckPendingDeletion"))

			// The instance itself is never deleted - this operator never
			// calls DeleteDBInstance.
			instanceID := cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, updated.Namespace, updated.Name, "rds", "orders-db", 63)
			Expect(fakeRDS.instances).To(HaveKey(instanceID))
		})
	})
})

// newCountingReconciler builds a reconciler backed by a fake client whose
// status-subresource Patch calls are counted in patchCount, plus the same
// fake AWS backends the Ginkgo suite above uses - reused here because the
// tests above need envtest (a real API server, unwrappable), while the
// tests below need to count Patch calls, which only the fake client's
// interceptor supports.
func newCountingReconciler(t *testing.T, patchCount *int, objs ...client.Object) *AppDependenciesReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme (core): %v", err)
	}
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme (deps): %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&depsv1alpha1.AppDependencies{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if subResourceName == "status" {
					*patchCount = *patchCount + 1
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	return &AppDependenciesReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		AWSClients: &cloudctlaws.Clients{
			SQS: newFakeSQSClient(), SNS: newFakeSNSClient(), IAM: newFakeIAMClient(),
			KMS: newFakeKMSClient(), CloudWatch: newFakeCloudWatchClient(),
			Region: "us-east-1", AccountID: "123456789012",
		},
		OIDCProviderARN: "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
		OIDCProviderURL: "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
	}
}

// A section only checkpoints when it actually changed cr.Status - this is
// what keeps a large, mostly-idle fleet's drift-detection reconciles (every
// CR, every 5 minutes, by default) from costing up to 8 status writes
// apiece instead of a handful. This test pins that behavior down directly,
// since nothing else would fail loudly if it silently regressed back to
// checkpointing unconditionally.
func TestEnsureDesiredState_UnchangedReconcile_SkipsRedundantCheckpoints(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	firstReconcileCount := patchCount
	if firstReconcileCount == 0 {
		t.Fatal("expected the first reconcile (creating a new queue) to checkpoint at least once")
	}

	// Nothing changed, so nothing is written.
	patchCount = 0
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}

	if patchCount != 0 {
		t.Errorf("expected no status Patch call on a no-op reconcile, got %d", patchCount)
	}
}

// The mirror image of the test above: a section that actually creates
// something new must still checkpoint promptly, not get accidentally
// swallowed by whatever the "skip if unchanged" logic is comparing against.
func TestEnsureDesiredState_ChangedSection_StillCheckpoints(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	if patchCount < 2 {
		t.Errorf("expected at least 2 status Patch calls (the SQS section's checkpoint plus the trailing write), got %d", patchCount)
	}
}

// A checkpoint fires mid-reconcile, well before this pass's own status write
// would naturally happen — leaving a real window for something else (a
// GitOps tool reapplying spec, a human edit) to update the same CR first.
// Since a checkpoint's whole point is to survive a crash, it must also
// survive this: it must not itself be the thing that gets rejected and lose
// the ledger entry it was trying to save.
func TestCheckpointFor_SurvivesConcurrentSpecEdit(t *testing.T) {
	scheme := newSharedWithScheme(t)
	seed := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(seed).
		WithStatusSubresource(&depsv1alpha1.AppDependencies{}).
		Build()

	// The reconciler's own in-memory copy, as of the start of this pass.
	var reconcilerCR depsv1alpha1.AppDependencies
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(seed), &reconcilerCR); err != nil {
		t.Fatalf("Get: %v", err)
	}
	r := &AppDependenciesReconciler{Client: c, Scheme: scheme}
	original := reconcilerCR.DeepCopy()
	checkpoint := checkpointFor(r, &reconcilerCR, original)

	// Concurrently, something else edits spec and saves - moving the
	// object's ResourceVersion past what the reconciler's copy holds, same
	// as a GitOps reconciliation landing mid-reconcile.
	var editor depsv1alpha1.AppDependencies
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(seed), &editor); err != nil {
		t.Fatalf("Get (editor): %v", err)
	}
	editor.Spec.SQS = &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	if err := c.Update(context.Background(), &editor); err != nil {
		t.Fatalf("concurrent spec edit: %v", err)
	}

	ledger := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:111111111111:orders"},
	}
	if err := checkpoint(context.Background(), ledger); err != nil {
		t.Fatalf("checkpoint() rejected by an unrelated concurrent spec edit: %v", err)
	}

	var final depsv1alpha1.AppDependencies
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(seed), &final); err != nil {
		t.Fatalf("Get (final): %v", err)
	}
	if len(final.Status.ManagedResources) != 1 {
		t.Errorf("expected the checkpointed ledger entry to be persisted, got %+v", final.Status.ManagedResources)
	}
	if final.Spec.SQS == nil {
		t.Error("expected the concurrent spec edit to survive the checkpoint, not be clobbered by it")
	}
}

func TestJitteredDriftRequeue_WithinBoundsAndVaries(t *testing.T) {
	lo := time.Duration(float64(DriftDetectionInterval) * (1 - driftRequeueJitterFraction))
	hi := time.Duration(float64(DriftDetectionInterval) * (1 + driftRequeueJitterFraction))

	seen := map[time.Duration]bool{}
	for range 50 {
		d := jitteredDriftRequeue()
		if d < lo || d > hi {
			t.Fatalf("jitteredDriftRequeue() = %v, want within [%v, %v]", d, lo, hi)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Error("expected jitteredDriftRequeue() to vary across calls, not return a fixed value")
	}
}

func TestStatusPatch_DoesNotCarryResourceVersion(t *testing.T) {
	original := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", ResourceVersion: "1"}}
	cr := original.DeepCopy()
	cr.ResourceVersion = "7"
	cr.Status.IAMRoleARN = "arn:aws:iam::123456789012:role/x"

	data, err := statusPatch(original, cr).Data(cr)
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if strings.Contains(string(data), "resourceVersion") {
		t.Errorf("patch must not carry resourceVersion as an implicit precondition, got %s", data)
	}
	if !strings.Contains(string(data), "iamRoleARN") {
		t.Errorf("expected the status change to be in the patch, got %s", data)
	}
}

func TestCheckpointFor_LaterCheckpointSurvivesAConcurrentSpecEdit(t *testing.T) {
	scheme := newSharedWithScheme(t)
	seed := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(seed).
		WithStatusSubresource(&depsv1alpha1.AppDependencies{}).
		Build()

	var reconcilerCR depsv1alpha1.AppDependencies
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(seed), &reconcilerCR); err != nil {
		t.Fatalf("Get: %v", err)
	}
	r := &AppDependenciesReconciler{Client: c, Scheme: scheme}
	checkpoint := checkpointFor(r, &reconcilerCR, reconcilerCR.DeepCopy())

	first := []depsv1alpha1.ManagedResource{{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:111111111111:orders"}}
	if err := checkpoint(context.Background(), first); err != nil {
		t.Fatalf("first checkpoint() error = %v", err)
	}

	var editor depsv1alpha1.AppDependencies
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(seed), &editor); err != nil {
		t.Fatalf("Get (editor): %v", err)
	}
	editor.Spec.SQS = &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	if err := c.Update(context.Background(), &editor); err != nil {
		t.Fatalf("concurrent spec edit: %v", err)
	}

	second := []depsv1alpha1.ManagedResource{first[0], {Type: "sns", Name: "events", ARN: "arn:aws:sns:us-east-1:111111111111:events"}}
	if err := checkpoint(context.Background(), second); err != nil {
		t.Fatalf("second checkpoint() rejected by a spec edit made after the first: %v", err)
	}
}

// A second pass over an RDS instance the operator just created compares it
// with the spec, so the fake has to report what a real instance does.
func TestReconcile_AnRDSInstanceSettlesAcrossTwoPasses(t *testing.T) {
	grant := &depsv1alpha1.RDSSubnetGroupGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "rds-grant"},
		Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "happy-path-subnet-group", AllowedNamespaces: []string{"default"}},
	}
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "rds-cr", UID: "uid-1", Finalizers: []string{finalizerName}},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
			Name: "orders-db", DBSubnetGroupName: "happy-path-subnet-group",
			Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
		}}}},
	}
	patches := 0
	r := newCountingReconciler(t, &patches, cr, grant)
	r.AWSClients.RDS = newFakeRDSClient()
	r.AWSClients.EC2 = newFakeEC2Client()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	for pass := 1; pass <= 2; pass++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("pass %d: Reconcile() error = %v", pass, err)
		}
	}
	var updated depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), req.NamespacedName, &updated); err != nil {
		t.Fatal(err)
	}
	entry, found := status.FindManagedResource(updated.Status.ManagedResources, resourceTypeRDS, "orders-db")
	if !found || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected a Verified instance entry after two passes, got %+v (found=%v)", entry, found)
	}
}

func TestWarnIfLedgerLarge(t *testing.T) {
	ledgerOf := func(n int) *depsv1alpha1.AppDependencies {
		cr := &depsv1alpha1.AppDependencies{}
		for i := range n {
			cr.Status.ManagedResources = append(cr.Status.ManagedResources, depsv1alpha1.ManagedResource{Type: "sqs", Name: fmt.Sprintf("q%d", i)})
		}
		return cr
	}
	var recorded []string
	record := func(eventType, reason, _ string) { recorded = append(recorded, eventType+"/"+reason) }

	warnIfLedgerLarge(ledgerOf(largeLedgerEntries), record)
	if len(recorded) != 0 {
		t.Fatalf("no warning expected at the threshold, got %v", recorded)
	}
	warnIfLedgerLarge(ledgerOf(largeLedgerEntries+1), record)
	if len(recorded) != 1 || recorded[0] != "Warning/LedgerLarge" {
		t.Errorf("expected one Warning/LedgerLarge past the threshold, got %v", recorded)
	}
	warnIfLedgerLarge(ledgerOf(largeLedgerEntries+1), nil) // no recorder wired: must not panic
}

func TestStatusNeedsPatch(t *testing.T) {
	base := depsv1alpha1.AppDependenciesStatus{IAMRoleARN: "arn:aws:iam::123456789012:role/r"}
	other := depsv1alpha1.AppDependenciesStatus{IAMRoleARN: "arn:aws:iam::123456789012:role/other"}
	cr := func(version string, s depsv1alpha1.AppDependenciesStatus) *depsv1alpha1.AppDependencies {
		return &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{ResourceVersion: version}, Status: s}
	}
	tests := []struct {
		name string
		got  *depsv1alpha1.AppDependencies
		want bool
	}{
		{"nothing changed", cr("7", base), false},
		{"status changed", cr("7", other), true},
		{"a checkpoint wrote and the status ended where it began", cr("8", base), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusNeedsPatch("7", cr("7", base), tc.got); got != tc.want {
				t.Errorf("statusNeedsPatch() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestArnRegion(t *testing.T) {
	tests := map[string]string{
		"arn:aws:sqs:us-east-1:123456789012:orders":                            "us-east-1",
		"arn:aws:dynamodb:eu-west-1:123456789012:table/sessions":               "eu-west-1",
		"arn:aws:sns:us-west-2:123456789012:events:1a2b3c4d-0000-1111-2222-33": "us-west-2",
		"arn:aws:s3:::receipts":                                                "",
		"arn:aws:iam::123456789012:role/checkout":                              "",
		"checkout-service-final-snapshot-1700000000":                           "",
		"": "",
	}
	for arn, want := range tests {
		if got := arnRegion(arn); got != want {
			t.Errorf("arnRegion(%q) = %q, want %q", arn, got, want)
		}
	}
}

// Resources do not move between regions. An operator in another region would
// find queues missing and recreate them empty, or drop the ledger of a CR
// being deleted, so it refuses and touches nothing.
func TestReconcile_ACRWithResourcesInAnotherRegionIsRefused(t *testing.T) {
	inOtherRegion := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-west-2:123456789012:default-checkout-service-orders-1a2b3c4d"},
		{Type: "s3", Name: "receipts", ARN: "arn:aws:s3:::receipts"},
	}
	newCR := func(deleting bool) *depsv1alpha1.AppDependencies {
		cr := transientCR()
		cr.Status.ManagedResources = slices.Clone(inOtherRegion)
		if deleting {
			now := metav1.Now()
			cr.DeletionTimestamp = &now
		}
		return cr
	}

	for name, deleting := range map[string]bool{"a normal pass": false, "a deletion": true} {
		t.Run(name, func(t *testing.T) {
			cr := newCR(deleting)
			r, sqsFake := throttledReconciler(t, cr)
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

			res, err := r.Reconcile(context.Background(), req)
			if err != nil {
				t.Fatalf("Reconcile() error = %v, want a refusal recorded on the CR", err)
			}
			if !withinJitter(res.RequeueAfter, DriftDetectionInterval) {
				t.Errorf("RequeueAfter = %v, want about %v", res.RequeueAfter, DriftDetectionInterval)
			}
			if len(sqsFake.queues) != 0 || sqsFake.getQueueUrlCalls != 0 {
				t.Error("a refused CR must not be looked up or recreated in this region")
			}
			var updated depsv1alpha1.AppDependencies
			if err := r.Get(context.Background(), req.NamespacedName, &updated); err != nil {
				t.Fatal(err)
			}
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, conditionTypeSQSReady)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasons.RegionChanged {
				t.Errorf("SQSReady = %+v, want False with reason %s", cond, reasons.RegionChanged)
			}
			if len(updated.Status.ManagedResources) != len(inOtherRegion) {
				t.Errorf("the ledger must be left alone, has %d entries", len(updated.Status.ManagedResources))
			}
		})
	}
}

func TestSetSectionCondition_AnOwnershipRefusalNamesItsReasonOnTheSectionTheEventAndReady(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{}
	cr.Generation = 1
	var recorded []string
	record := func(eventType, reason, _ string) { recorded = append(recorded, eventType+"/"+reason) }

	_, err := cloudctlaws.CheckExistingOwnership(context.Background(), "queue", "orders", nil, "ns", "app", "uid", false)
	setSectionCondition(context.Background(), cr, conditionTypeSQSReady, err, record)

	section := apimeta.FindStatusCondition(cr.Status.Conditions, conditionTypeSQSReady)
	if section == nil || section.Reason != reasons.AdoptRequired {
		t.Fatalf("the section should carry %s, got %+v", reasons.AdoptRequired, section)
	}
	ready := apimeta.FindStatusCondition(cr.Status.Conditions, status.ConditionTypeReady)
	if ready == nil || ready.Reason != reasons.AdoptRequired || !strings.HasPrefix(ready.Message, "SQSReady: ") {
		t.Errorf("Ready should carry the section's reason and name it, got %+v", ready)
	}
	if want := "Warning/AdoptRequired"; !slices.Contains(recorded, want) {
		t.Errorf("expected the event %s, got %v", want, recorded)
	}
}

func TestSetSectionCondition_NoSecondEventForAReasonThatRecordsItsOwn(t *testing.T) {
	var recorded []string
	record := func(eventType, reason, _ string) { recorded = append(recorded, eventType+"/"+reason) }

	own := &cloudctlaws.ReconcileError{Err: errors.New("gone"), Reason: reasons.InstanceMissing}
	setSectionCondition(context.Background(), &depsv1alpha1.AppDependencies{}, conditionTypeRDSReady, own, record)
	if len(recorded) != 0 {
		t.Errorf("InstanceMissing is recorded where it is raised, not again here, got %v", recorded)
	}

	other := &cloudctlaws.ReconcileError{Err: errors.New("exists"), Reason: reasons.NameTaken}
	setSectionCondition(context.Background(), &depsv1alpha1.AppDependencies{}, conditionTypeRDSReady, other, record)
	if !slices.Equal(recorded, []string{"Warning/NameTaken"}) {
		t.Errorf("a reason without an event of its own is recorded here, got %v", recorded)
	}
}
