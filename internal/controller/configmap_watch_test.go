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
	"encoding/json"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/configmap"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/serviceaccount"
)

// These specs exist to catch a real gap found during manual day-2 testing
// against a live cluster: SetupWithManager registered no watch on this
// operator's own generated ConfigMap, so deleting it (or corrupting its
// data) was only ever repaired incidentally, if some unrelated reconcile
// happened to already be in flight - never as a deliberate, bounded-time
// guarantee. Unlike every other test in this package, these drive a real
// running controller (SetupWithManager + mgr.Start) rather than calling
// Reconcile directly, since calling Reconcile by hand would prove nothing
// about whether a watch exists to trigger it in the first place.
var _ = Describe("AppDependencies watches on its generated ConfigMap", Ordered, func() {
	var (
		mgrCancel context.CancelFunc
		mgrDone   chan struct{}
		testNS    string
	)

	// Started once for this whole Describe block, not per It: SetupWithManager
	// registers a controller named "appdependencies" in controller-runtime's
	// process-global metrics registry, which rejects a second registration
	// under the same name - a fresh manager per It would collide with itself.
	BeforeAll(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-test-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		testNS = ns.Name

		// Scoped to testNS alone: every other test file in this package
		// creates AppDependencies CRs in "default" and never deletes them,
		// which was always safe before since nothing else ran a live
		// reconcile loop against that shared state. This suite's manager is
		// the first thing here that does - an unscoped cache would pick up
		// those leftover CRs on its initial sync and try to reconcile them
		// too, with a reconciler whose AWSClients was only ever configured
		// for what these specs need (no DynamoDB/S3 fakes), causing a real
		// nil-pointer panic rather than a clean, isolated test failure.
		// SkipNameValidation: section_rds_integration_test.go's own watch
		// spec also runs a live manager for this same reconciler type in
		// this process - Ginkgo randomizes top-level spec order by
		// default, so either one could register the "appdependencies"
		// controller name first; both must tolerate the other already
		// holding it.
		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Controller:             config.Controller{SkipNameValidation: &skipNameValidation},
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{testNS: {}},
				ByObject:          ManagedObjectCache(),
			},
		})
		Expect(err).NotTo(HaveOccurred())

		reconciler := &AppDependenciesReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
			AWSClients: &cloudctlaws.Clients{
				SQS: newFakeSQSClient(), SNS: newFakeSNSClient(), IAM: newFakeIAMClient(),
				KMS: newFakeKMSClient(), CloudWatch: newFakeCloudWatchClient(),
				Region: "us-east-1", AccountID: "123456789012",
			},
			Recorder:        events.NewFakeRecorder(20),
			OIDCProviderARN: "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
			OIDCProviderURL: "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
		}
		Expect(reconciler.SetupWithManager(mgr)).To(Succeed())

		var mgrCtx context.Context
		mgrCtx, mgrCancel = context.WithCancel(ctx)
		mgrDone = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(mgrDone)
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
	})

	AfterAll(func() {
		mgrCancel()
		Eventually(mgrDone, 10*time.Second).Should(BeClosed())
	})

	// Every CR here declares one owned SQS queue: an empty ConfigMap is
	// deliberately deleted rather than created (see configmap.Ensure), so
	// there must be real connection data for there to be anything to watch.
	newWatchTestCR := func(generateName string) *depsv1alpha1.AppDependencies {
		return &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: generateName,
				Namespace:    testNS,
			},
			Spec: depsv1alpha1.AppDependenciesSpec{
				SQS: &depsv1alpha1.SQSSpec{
					Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}},
				},
			},
		}
	}

	It("recreates the connection ConfigMap after it's deleted out-of-band", func() {
		cr := newWatchTestCR("watch-configmap-")
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		cmKey := types.NamespacedName{Namespace: cr.Namespace, Name: configmap.ConfigMapName(cr.Name)}
		Eventually(func() error {
			return k8sClient.Get(ctx, cmKey, &corev1.ConfigMap{})
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, cmKey, &cm)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &cm)).To(Succeed())

		// No manual Reconcile call from here on - only the controller's own
		// watch on ConfigMap can make this pass, and well under
		// DriftDetectionInterval.
		Eventually(func() error {
			return k8sClient.Get(ctx, cmKey, &corev1.ConfigMap{})
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
	})

	It("repairs the connection ConfigMap's data after it's corrupted in place, not just deleted", func() {
		// Delete and Update are different event types for this watch -
		// corrupting data in place, rather than deleting the object, is
		// the only way to actually exercise the Update path.
		cr := newWatchTestCR("watch-configmap-update-")
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		cmKey := types.NamespacedName{Namespace: cr.Namespace, Name: configmap.ConfigMapName(cr.Name)}
		var cm corev1.ConfigMap
		Eventually(func() error {
			return k8sClient.Get(ctx, cmKey, &cm)
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		Expect(cm.Data).NotTo(BeEmpty())

		original := map[string]string{}
		maps.Copy(original, cm.Data)
		for k := range cm.Data {
			cm.Data[k] = "corrupted"
		}
		Expect(k8sClient.Update(ctx, &cm)).To(Succeed())

		Eventually(func() map[string]string {
			var cm corev1.ConfigMap
			if err := k8sClient.Get(ctx, cmKey, &cm); err != nil {
				return nil
			}
			return cm.Data
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(original))
	})

})

// chaosSQS wraps the in-memory SQS fake with failure injection that is safe
// to flip from the test goroutine while a live manager is calling it.
type chaosSQS struct {
	*fakeSQSClient
	mu          sync.Mutex
	createErr   error
	untagErr    error
	createCalls int
}

func (c *chaosSQS) setCreateErr(err error) { c.mu.Lock(); c.createErr = err; c.mu.Unlock() }
func (c *chaosSQS) setUntagErr(err error)  { c.mu.Lock(); c.untagErr = err; c.mu.Unlock() }
func (c *chaosSQS) createCallCount() int   { c.mu.Lock(); defer c.mu.Unlock(); return c.createCalls }

func (c *chaosSQS) CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	c.mu.Lock()
	c.createCalls++
	err := c.createErr
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return c.fakeSQSClient.CreateQueue(ctx, in, optFns...)
}

func (c *chaosSQS) UntagQueue(ctx context.Context, in *sqs.UntagQueueInput, optFns ...func(*sqs.Options)) (*sqs.UntagQueueOutput, error) {
	c.mu.Lock()
	err := c.untagErr
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return c.fakeSQSClient.UntagQueue(ctx, in, optFns...)
}

// These specs run a live manager against a real API server and push it
// through failures and hostile in-cluster state, not the happy path:
// partial AWS failures, persistent errors (no hot loop), a delete that
// cannot finish yet, objects it must not take over, a user-owned
// ServiceAccount, and cross-namespace sharing being revoked.
var _ = Describe("AppDependencies under failure and hostile state", Ordered, func() {
	const timeout, poll = 30 * time.Second, 100 * time.Millisecond

	var (
		mgrCancel context.CancelFunc
		mgrDone   chan struct{}
		chaos     *chaosSQS
		nsA, nsB  string
	)

	BeforeAll(func() {
		for _, target := range []*string{&nsA, &nsB} {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "resilience-"}}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			*target = ns.Name
		}

		chaos = &chaosSQS{fakeSQSClient: newFakeSQSClient()}

		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Controller:             config.Controller{SkipNameValidation: &skipNameValidation},
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{nsA: {}, nsB: {}},
				ByObject:          ManagedObjectCache(),
			},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect((&AppDependenciesReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
			AWSClients: &cloudctlaws.Clients{
				SQS: chaos, SNS: newFakeSNSClient(), IAM: newFakeIAMClient(),
				KMS: newFakeKMSClient(), CloudWatch: newFakeCloudWatchClient(),
				Region: "us-east-1", AccountID: "123456789012",
			},
			Recorder:        events.NewFakeRecorder(1000),
			OIDCProviderARN: "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
			OIDCProviderURL: "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
		}).SetupWithManager(mgr)).To(Succeed())

		var mgrCtx context.Context
		mgrCtx, mgrCancel = context.WithCancel(ctx)
		mgrDone = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(mgrDone)
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
	})

	AfterAll(func() {
		mgrCancel()
		Eventually(mgrDone, 10*time.Second).Should(BeClosed())
	})

	// Every spec starts from a healthy AWS and no CRs: failures are
	// cleared first so leftover retries can finish, then each CR is deleted
	// and waited out.
	AfterEach(func() {
		chaos.setCreateErr(nil)
		chaos.setUntagErr(nil)
		for _, ns := range []string{nsA, nsB} {
			var list depsv1alpha1.AppDependenciesList
			Expect(k8sClient.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
			for i := range list.Items {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &list.Items[i]))).To(Succeed())
			}
			Eventually(func() int {
				var l depsv1alpha1.AppDependenciesList
				_ = k8sClient.List(ctx, &l, client.InNamespace(ns))
				return len(l.Items)
			}, 60*time.Second, poll).Should(BeZero())
		}
	})

	queueCR := func(ns, name string, mutate func(*depsv1alpha1.AppDependencies)) *depsv1alpha1.AppDependencies {
		cr := &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: depsv1alpha1.AppDependenciesSpec{
				SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
			},
		}
		if mutate != nil {
			mutate(cr)
		}
		return cr
	}

	condition := func(ns, name, condType string) *metav1.Condition {
		var cr depsv1alpha1.AppDependencies
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cr); err != nil {
			return nil
		}
		return apimeta.FindStatusCondition(cr.Status.Conditions, condType)
	}

	conditionStatus := func(ns, name, condType string) metav1.ConditionStatus {
		if c := condition(ns, name, condType); c != nil {
			return c.Status
		}
		return ""
	}

	Context("an AWS failure in one section", func() {
		It("does not stop the other sections, and recovers by itself once AWS does", func() {
			chaos.setCreateErr(&fakeAWSError{code: "AccessDenied"})
			cr := queueCR(nsA, "partial-failure", func(cr *depsv1alpha1.AppDependencies) {
				cr.Spec.SNS = &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			Eventually(func() metav1.ConditionStatus { return conditionStatus(nsA, cr.Name, "SNSReady") }, timeout, poll).
				Should(Equal(metav1.ConditionTrue), "an SQS failure must not block the SNS section")
			Eventually(func() string {
				if c := condition(nsA, cr.Name, "SQSReady"); c != nil {
					return c.Reason
				}
				return ""
			}, timeout, poll).Should(Equal("PermissionDenied"))
			Expect(conditionStatus(nsA, cr.Name, "Ready")).To(Equal(metav1.ConditionFalse))

			chaos.setCreateErr(nil)
			Eventually(func() metav1.ConditionStatus { return conditionStatus(nsA, cr.Name, "Ready") }, timeout, poll).
				Should(Equal(metav1.ConditionTrue), "no manual trigger: the failed reconcile must retry on its own")
		})

		It("backs off on a persistent failure instead of hammering AWS and the API server", func() {
			chaos.setCreateErr(&fakeAWSError{code: "AccessDenied"})
			cr := queueCR(nsA, "persistent-failure", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			// Status is written section by section, so wait for the last
			// section's condition: only then is one pass fully recorded.
			Eventually(func() metav1.ConditionStatus { return conditionStatus(nsA, cr.Name, "IAMReady") }, timeout, poll).
				Should(Equal(metav1.ConditionFalse))
			time.Sleep(500 * time.Millisecond)

			callsBefore := chaos.createCallCount()
			var before depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &before)).To(Succeed())

			time.Sleep(3 * time.Second)

			// Exponential backoff from 5ms reaches only about 10 attempts in
			// the first 5 seconds; a hot loop would make thousands.
			Expect(chaos.createCallCount() - callsBefore).To(BeNumerically("<", 25))
			var after depsv1alpha1.AppDependencies
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &after)).To(Succeed())
			beforeStatus, _ := json.Marshal(before.Status)
			afterStatus, _ := json.Marshal(after.Status)
			Expect(after.ResourceVersion).To(Equal(before.ResourceVersion),
				"an unchanged failure must not rewrite status on every attempt\nbefore: %s\nafter:  %s", beforeStatus, afterStatus)
		})
	})

	Context("deleting a CR whose cleanup cannot finish yet", func() {
		It("keeps the finalizer, then completes by itself once the cause clears", func() {
			cr := queueCR(nsA, "delete-blocked", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			Eventually(func() metav1.ConditionStatus { return conditionStatus(nsA, cr.Name, "Ready") }, timeout, poll).
				Should(Equal(metav1.ConditionTrue))

			chaos.setUntagErr(&fakeAWSError{code: "AccessDenied"})
			Expect(k8sClient.Delete(ctx, cr)).To(Succeed())

			Consistently(func() []string {
				var got depsv1alpha1.AppDependencies
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &got); err != nil {
					return nil
				}
				return got.Finalizers
			}, 3*time.Second, poll).Should(ContainElement(finalizerName), "the CR must not vanish while cleanup is failing")

			chaos.setUntagErr(nil)
			Eventually(func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &depsv1alpha1.AppDependencies{})
				return apierrors.IsNotFound(err)
			}, timeout, poll).Should(BeTrue())
		})
	})

	Context("an object the operator must not take over", func() {
		It("refuses a pre-existing ConfigMap it doesn't own, leaves it untouched, and takes over the name once it's gone", func() {
			foreign := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: configmap.ConfigMapName("foreign-cm")},
				Data:       map[string]string{"MINE": "keep"},
			}
			Expect(k8sClient.Create(ctx, foreign)).To(Succeed())

			Expect(k8sClient.Create(ctx, queueCR(nsA, "foreign-cm", nil))).To(Succeed())

			Eventually(func() string {
				if c := condition(nsA, "foreign-cm", "ConnectionInfoReady"); c != nil && c.Status == metav1.ConditionFalse {
					return c.Message
				}
				return ""
			}, timeout, poll).Should(ContainSubstring("not owned by this CR"))

			Consistently(func() map[string]string {
				var got corev1.ConfigMap
				_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), &got)
				return got.Data
			}, 2*time.Second, poll).Should(Equal(map[string]string{"MINE": "keep"}))

			Expect(k8sClient.Delete(ctx, foreign)).To(Succeed())
			Eventually(func() map[string]string {
				var got corev1.ConfigMap
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), &got); err != nil {
					return nil
				}
				if got.Labels[depsv1alpha1.ManagedByLabelKey] != depsv1alpha1.ManagedByLabelValue || len(got.OwnerReferences) != 1 {
					return nil
				}
				return got.Data
			}, timeout, poll).ShouldNot(BeEmpty(), "the operator's own labelled, owned ConfigMap should take the name")
		})
	})

	Context("the connection ConfigMap's own metadata being tampered with", func() {
		cmKey := func(crName string) types.NamespacedName {
			return types.NamespacedName{Namespace: nsA, Name: configmap.ConfigMapName(crName)}
		}

		It("gets its managed-by label back after the label is stripped", func() {
			Expect(k8sClient.Create(ctx, queueCR(nsA, "cm-label", nil))).To(Succeed())
			Eventually(func() int {
				var cm corev1.ConfigMap
				_ = k8sClient.Get(ctx, cmKey("cm-label"), &cm)
				return len(cm.Data)
			}, timeout, poll).Should(BeNumerically(">", 0))

			// Without the label the object drops out of the operator's
			// filtered cache, so this exercises the path where it can only
			// be found through the API reader.
			var cm corev1.ConfigMap
			Expect(k8sClient.Get(ctx, cmKey("cm-label"), &cm)).To(Succeed())
			delete(cm.Labels, depsv1alpha1.ManagedByLabelKey)
			Expect(k8sClient.Update(ctx, &cm)).To(Succeed())

			Eventually(func() string {
				var got corev1.ConfigMap
				_ = k8sClient.Get(ctx, cmKey("cm-label"), &got)
				return got.Labels[depsv1alpha1.ManagedByLabelKey]
			}, timeout, poll).Should(Equal(depsv1alpha1.ManagedByLabelValue))
		})

		It("is treated as foreign once its owner reference is stripped, and the operator backs off instead of overwriting it", func() {
			Expect(k8sClient.Create(ctx, queueCR(nsA, "cm-owner", nil))).To(Succeed())
			Eventually(func() int {
				var cm corev1.ConfigMap
				_ = k8sClient.Get(ctx, cmKey("cm-owner"), &cm)
				return len(cm.OwnerReferences)
			}, timeout, poll).Should(Equal(1))

			var cm corev1.ConfigMap
			Expect(k8sClient.Get(ctx, cmKey("cm-owner"), &cm)).To(Succeed())
			cm.OwnerReferences = nil
			cm.Data = map[string]string{"HAND_EDITED": "true"}
			Expect(k8sClient.Update(ctx, &cm)).To(Succeed())

			// The owner reference is the only proof the object is ours, so
			// without it the operator must not touch the data. The user
			// recovers by deleting the ConfigMap.
			Eventually(func() string {
				if c := condition(nsA, "cm-owner", "ConnectionInfoReady"); c != nil && c.Status == metav1.ConditionFalse {
					return c.Message
				}
				return ""
			}, timeout, poll).Should(ContainSubstring("not owned by this CR"))
			Consistently(func() map[string]string {
				var got corev1.ConfigMap
				_ = k8sClient.Get(ctx, cmKey("cm-owner"), &got)
				return got.Data
			}, 2*time.Second, poll).Should(Equal(map[string]string{"HAND_EDITED": "true"}))

			Expect(k8sClient.Delete(ctx, &cm)).To(Succeed())
			Eventually(func() int {
				var got corev1.ConfigMap
				_ = k8sClient.Get(ctx, cmKey("cm-owner"), &got)
				return len(got.OwnerReferences)
			}, timeout, poll).Should(Equal(1))
		})
	})

	Context("a ServiceAccount the user owns", func() {
		It("gets the role annotation merged in, keeps everything else, and is released (not deleted) with the CR", func() {
			sa := &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   nsA,
					Name:        "team-sa",
					Labels:      map[string]string{"team": "payments"},
					Annotations: map[string]string{"example.com/keep": "yes"},
				},
			}
			Expect(k8sClient.Create(ctx, sa)).To(Succeed())

			cr := queueCR(nsA, "user-sa", func(cr *depsv1alpha1.AppDependencies) { cr.Spec.ServiceAccountName = "team-sa" })
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			saKey := client.ObjectKeyFromObject(sa)
			Eventually(func() string {
				var got corev1.ServiceAccount
				_ = k8sClient.Get(ctx, saKey, &got)
				return got.Annotations[serviceaccount.RoleARNAnnotation]
			}, timeout, poll).ShouldNot(BeEmpty())

			var merged corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, saKey, &merged)).To(Succeed())
			Expect(merged.Annotations).To(HaveKeyWithValue("example.com/keep", "yes"))
			Expect(merged.Labels).To(HaveKeyWithValue("team", "payments"))
			Expect(merged.OwnerReferences).To(BeEmpty(), "a ServiceAccount the user created must never be owned, or garbage-collected with the CR")

			Expect(k8sClient.Delete(ctx, cr)).To(Succeed())
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &depsv1alpha1.AppDependencies{}))
			}, timeout, poll).Should(BeTrue())

			var released corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, saKey, &released)).To(Succeed(), "the user's ServiceAccount must survive the CR")
			Expect(released.Annotations).NotTo(HaveKey(serviceaccount.RoleARNAnnotation))
			Expect(released.Annotations).To(HaveKeyWithValue("example.com/keep", "yes"))
		})

		It("is refused when another CR already claimed it", func() {
			sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: "shared-sa"}}
			Expect(k8sClient.Create(ctx, sa)).To(Succeed())

			first := queueCR(nsA, "sa-first", func(cr *depsv1alpha1.AppDependencies) { cr.Spec.ServiceAccountName = "shared-sa" })
			Expect(k8sClient.Create(ctx, first)).To(Succeed())
			Eventually(func() string {
				var got corev1.ServiceAccount
				_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(sa), &got)
				return got.Annotations[serviceaccount.RoleARNAnnotation]
			}, timeout, poll).ShouldNot(BeEmpty())
			var claimed corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sa), &claimed)).To(Succeed())

			second := queueCR(nsA, "sa-second", func(cr *depsv1alpha1.AppDependencies) { cr.Spec.ServiceAccountName = "shared-sa" })
			Expect(k8sClient.Create(ctx, second)).To(Succeed())
			Eventually(func() string {
				if c := condition(nsA, "sa-second", "IAMReady"); c != nil && c.Status == metav1.ConditionFalse {
					return c.Message
				}
				return ""
			}, timeout, poll).Should(ContainSubstring("already claimed"))

			var still corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sa), &still)).To(Succeed())
			Expect(still.Annotations[serviceaccount.RoleARNAnnotation]).To(Equal(claimed.Annotations[serviceaccount.RoleARNAnnotation]),
				"the second CR must not repoint the first one's role")
		})
	})

	Context("the operator's own ServiceAccount", func() {
		It("is recreated when deleted and re-annotated when stripped", func() {
			cr := queueCR(nsA, "own-sa", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			saKey := types.NamespacedName{Namespace: nsA, Name: cr.Name}

			roleARN := func() string {
				var got corev1.ServiceAccount
				_ = k8sClient.Get(ctx, saKey, &got)
				return got.Annotations[serviceaccount.RoleARNAnnotation]
			}
			Eventually(roleARN, timeout, poll).ShouldNot(BeEmpty())

			var sa corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, saKey, &sa)).To(Succeed())
			Expect(sa.OwnerReferences).To(HaveLen(1))
			delete(sa.Annotations, serviceaccount.RoleARNAnnotation)
			Expect(k8sClient.Update(ctx, &sa)).To(Succeed())
			Eventually(roleARN, timeout, poll).ShouldNot(BeEmpty(), "a stripped annotation must be restored")

			Expect(k8sClient.Get(ctx, saKey, &sa)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &sa)).To(Succeed())
			Eventually(roleARN, timeout, poll).ShouldNot(BeEmpty(), "a deleted ServiceAccount must be recreated")
		})
	})

	Context("sharing across namespaces", func() {
		It("delivers the shared resource to the consumer and takes it away again when the grant is revoked", func() {
			producer := queueCR(nsA, "producer", func(cr *depsv1alpha1.AppDependencies) {
				cr.Spec.SQS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: nsB, Name: "consumer"}}
			})
			Expect(k8sClient.Create(ctx, producer)).To(Succeed())

			consumer := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsB, Name: "consumer"},
				Spec: depsv1alpha1.AppDependenciesSpec{
					SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{{Namespace: nsA, Name: "producer", ResourceName: "orders"}}},
				},
			}
			Expect(k8sClient.Create(ctx, consumer)).To(Succeed())

			consumerKeys := func() []string {
				var cm corev1.ConfigMap
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: nsB, Name: configmap.ConfigMapName("consumer")}, &cm); err != nil {
					return nil
				}
				var keys []string
				for k := range cm.Data {
					keys = append(keys, k)
				}
				return keys
			}
			Eventually(consumerKeys, timeout, poll).Should(HaveLen(1))
			Expect(strings.Join(consumerKeys(), ",")).To(And(ContainSubstring("ORDERS"), HaveSuffix("_URL")))

			By("revoking the grant on the producer, without touching the consumer")
			updateCR(client.ObjectKeyFromObject(producer), func(cr *depsv1alpha1.AppDependencies) {
				cr.Spec.SQS.Resources[0].SharedWith = nil
			})
			Eventually(consumerKeys, timeout, poll).Should(BeEmpty(), "revoked access must disappear from the consumer's ConfigMap")

			By("granting it again")
			updateCR(client.ObjectKeyFromObject(producer), func(cr *depsv1alpha1.AppDependencies) {
				cr.Spec.SQS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: nsB, Name: "consumer"}}
			})
			Eventually(consumerKeys, timeout, poll).Should(HaveLen(1))
		})
	})
})

// updateCR applies mutate to the latest copy of a CR, retrying when the
// running controller's own status writes make the update conflict.
func updateCR(key client.ObjectKey, mutate func(*depsv1alpha1.AppDependencies)) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cr depsv1alpha1.AppDependencies
		if err := k8sClient.Get(ctx, key, &cr); err != nil {
			return err
		}
		mutate(&cr)
		return k8sClient.Update(ctx, &cr)
	})).To(Succeed())
}
