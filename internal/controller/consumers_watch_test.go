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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/configmap"
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
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{testNS: {}},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		reconciler := &AppDependenciesReconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
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
		// Delete and Update are different event types - a Delete-only test
		// previously passed even while a real bug silently dropped every
		// Update event on this watch (see SetupWithManager history: a global
		// WithEventFilter was unintentionally ANDed onto this Owns() call
		// too, and Create/Delete pass that filter trivially while Update
		// does not). Corrupting data in place, rather than deleting the
		// object, is the only way to actually exercise the Update path.
		cr := newWatchTestCR("watch-configmap-update-")
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		cmKey := types.NamespacedName{Namespace: cr.Namespace, Name: configmap.ConfigMapName(cr.Name)}
		var cm corev1.ConfigMap
		Eventually(func() error {
			return k8sClient.Get(ctx, cmKey, &cm)
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		Expect(cm.Data).NotTo(BeEmpty())

		original := map[string]string{}
		for k, v := range cm.Data {
			original[k] = v
		}
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
