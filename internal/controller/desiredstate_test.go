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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

var _ = Describe("AppDependencies Controller", func() {
	var (
		fakeSQS        *fakeSQSClient
		fakeSNS        *fakeSNSClient
		fakeIAM        *fakeIAMClient
		fakeKMS        *fakeKMSClient
		fakeCloudWatch *fakeCloudWatchClient
		reconciler     *AppDependenciesReconciler
	)

	BeforeEach(func() {
		fakeSQS = newFakeSQSClient()
		fakeSNS = newFakeSNSClient()
		fakeIAM = newFakeIAMClient()
		fakeKMS = newFakeKMSClient()
		fakeCloudWatch = newFakeCloudWatchClient()
		reconciler = &AppDependenciesReconciler{
			Client:          k8sClient,
			Scheme:          k8sClient.Scheme(),
			AWSClients:      &cloudctlaws.Clients{SQS: fakeSQS, SNS: fakeSNS, IAM: fakeIAM, KMS: fakeKMS, CloudWatch: fakeCloudWatch, Region: "us-east-1", AccountID: "123456789012"},
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

			queueName := cloudctlaws.ResourceName(updated.Namespace, updated.Name, "orders")
			Expect(fakeSQS.queues).To(HaveKey(queueName))

			ready := apimeta.FindStatusCondition(updated.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))

			sqsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "SQSReady")
			Expect(sqsReady).NotTo(BeNil())
			Expect(sqsReady.Status).To(Equal(metav1.ConditionTrue))
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

			alias := "alias/" + cloudctlaws.ResourceName(updated.Namespace, updated.Name, "primary")
			Expect(fakeKMS.aliases).To(HaveKey(alias))

			entry := status.FindManagedResource(updated.Status.ManagedResources, "kms", "primary")
			Expect(entry).NotTo(BeNil())
			Expect(entry.State).To(Equal(depsv1alpha1.ManagedResourceStateVerified))
			Expect(fakeKMS.aliases[alias]).To(Equal(entry.ARN))

			kmsReady := apimeta.FindStatusCondition(updated.Status.Conditions, "KMSReady")
			Expect(kmsReady).NotTo(BeNil())
			Expect(kmsReady.Status).To(Equal(metav1.ConditionTrue))

			ready := apimeta.FindStatusCondition(updated.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))
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
			entry := status.FindManagedResource(pending.Status.ManagedResources, "sqs", "orders")
			Expect(entry).NotTo(BeNil())
			past := metav1.NewTime(time.Now().Add(-1 * time.Hour))
			entry.PendingDeletionSince = &past
			status.UpsertManagedResource(&pending.Status.ManagedResources, *entry)
			Expect(k8sClient.Status().Update(ctx, &pending)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, req.NamespacedName, &depsv1alpha1.AppDependencies{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			queueName := cloudctlaws.ResourceName(cr.Namespace, cr.Name, "orders")
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

			queueName := cloudctlaws.ResourceName(cr.Namespace, cr.Name, "orders")
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

			topicArn := cloudctlaws.TopicARN("us-east-1", "123456789012", cloudctlaws.ResourceName(updated.Namespace, updated.Name, "events"))
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

			topicArn := cloudctlaws.TopicARN("us-east-1", "123456789012", cloudctlaws.ResourceName(cr.Namespace, cr.Name, "events"))
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
})
