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
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
		fakeRecorder   *record.FakeRecorder
		reconciler     *AppDependenciesReconciler
	)

	BeforeEach(func() {
		fakeSQS = newFakeSQSClient()
		fakeSNS = newFakeSNSClient()
		fakeIAM = newFakeIAMClient()
		fakeKMS = newFakeKMSClient()
		fakeCloudWatch = newFakeCloudWatchClient()
		fakeRecorder = record.NewFakeRecorder(20)
		reconciler = &AppDependenciesReconciler{
			Client:          k8sClient,
			Scheme:          k8sClient.Scheme(),
			AWSClients:      &cloudctlaws.Clients{SQS: fakeSQS, SNS: fakeSNS, IAM: fakeIAM, KMS: fakeKMS, CloudWatch: fakeCloudWatch, Region: "us-east-1", AccountID: "123456789012"},
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
		Client: c,
		Scheme: scheme,
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

	// Second reconcile: same CR, nothing in spec changed. SQS's own ledger
	// entry is stable (inside its trust window, so its section produces no
	// diff at all) - but the derived IAM role has no such trust window and
	// always re-stamps LastVerifiedAt to metav1.Now() on every reconcile,
	// so the IAM section's own checkpoint still fires every time regardless
	// of this optimization (the same pre-existing gap KMS has). Expected
	// total is therefore 2, not 1: IAM's unavoidable-for-now checkpoint,
	// plus reconcileNormal's own trailing write, which is unconditional and
	// not part of this optimization at all.
	patchCount = 0
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}

	if patchCount != 2 {
		t.Errorf("expected exactly 2 status Patch calls on a no-op reconcile (IAM's re-verify checkpoint + the trailing write), got %d", patchCount)
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
