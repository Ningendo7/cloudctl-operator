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

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func TestStuckInstanceNames(t *testing.T) {
	results := []rds.CleanupResult{
		{Name: "orders-db", Reason: rds.CleanupReasonStuckPendingDeletion},
		{Name: "invoices-db", Reason: rds.CleanupReasonPendingDeletion},
		{Name: "reports-db", Reason: rds.CleanupReasonRetained},
		{Name: "archive-db", Reason: rds.CleanupReasonStuckPendingDeletion},
	}
	got := stuckInstanceNames(results)
	if len(got) != 2 || got[0] != "orders-db" || got[1] != "archive-db" {
		t.Errorf("stuckInstanceNames() = %v, want [orders-db archive-db]", got)
	}
}

func TestStuckInstanceNames_NoneStuck(t *testing.T) {
	results := []rds.CleanupResult{
		{Name: "invoices-db", Reason: rds.CleanupReasonPendingDeletion},
		{Name: "reports-db", Reason: rds.CleanupReasonRetained},
	}
	if got := stuckInstanceNames(results); len(got) != 0 {
		t.Errorf("expected no stuck names, got %v", got)
	}
}

// This Describe drives a real running controller (SetupWithManager +
// mgr.Start), mirroring configmap_watch_test.go's own live-manager
// pattern - calling Reconcile directly would prove nothing about whether
// the RDSSubnetGroupGrant watch actually exists. Kept here, alongside
// stuckInstanceNames above, rather than in its own file or in
// desiredstate_test.go: both are genuinely section_rds.go's own concern
// (the watch only ever matters for RDS), unlike configmap_watch_test.go's
// own watch, which is generic to every resource type.
var _ = Describe("AppDependencies watches on RDSSubnetGroupGrant", Ordered, func() {
	var (
		mgrCancel context.CancelFunc
		mgrDone   chan struct{}
		testNS    string
		fakeRDS   *fakeRDSClient
		fakeEC2   *fakeEC2Client
	)

	BeforeAll(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "rds-watch-test-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		testNS = ns.Name

		fakeRDS = newFakeRDSClient()
		fakeEC2 = newFakeEC2Client()

		// RDSSubnetGroupGrant is cluster-scoped, so this namespace scoping
		// only isolates the AppDependencies CRs this suite creates from
		// every other test file's own leftover state - the grant itself
		// is visible cluster-wide regardless, which is exactly what this
		// spec needs to prove.
		// SkipNameValidation: this process already has other live
		// managers (configmap_watch_test.go's own) that registered a
		// controller named "appdependencies" in controller-runtime's
		// process-global metrics registry - Ginkgo randomizes spec order,
		// so whichever one runs first claims it; this one must tolerate
		// that.
		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Controller:             config.Controller{SkipNameValidation: &skipNameValidation},
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{testNS: {}},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		reconciler := &AppDependenciesReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme: mgr.GetScheme(),
			AWSClients: &cloudctlaws.Clients{
				RDS: fakeRDS, EC2: fakeEC2,
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

	It("re-reconciles a blocked CR the moment an authorizing grant is created", func() {
		cr := &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-rds-", Namespace: testNS},
			Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
				Name:              "orders-db",
				DBSubnetGroupName: "watch-test-subnet-group",
				Engine:            "postgres",
				EngineVersion:     "16.3",
				InstanceClass:     "db.t4g.micro",
				AllocatedStorage:  20,
			}}}},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
		Eventually(func() bool {
			var current depsv1alpha1.AppDependencies
			if err := k8sClient.Get(ctx, crKey, &current); err != nil {
				return false
			}
			rdsReady := apimeta.FindStatusCondition(current.Status.Conditions, "RDSReady")
			return rdsReady != nil && rdsReady.Status == metav1.ConditionFalse
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue(), "expected RDSReady=False while unauthorized")

		grant := &depsv1alpha1.RDSSubnetGroupGrant{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-rds-grant-"},
			Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "watch-test-subnet-group", AllowedNamespaces: []string{testNS}},
		}
		Expect(k8sClient.Create(ctx, grant)).To(Succeed())

		// No manual Reconcile call from here on - only the watch on
		// RDSSubnetGroupGrant can make this pass.
		Eventually(func() bool {
			var current depsv1alpha1.AppDependencies
			if err := k8sClient.Get(ctx, crKey, &current); err != nil {
				return false
			}
			rdsReady := apimeta.FindStatusCondition(current.Status.Conditions, "RDSReady")
			return rdsReady != nil && rdsReady.Status == metav1.ConditionTrue
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue(), "expected the grant's creation to immediately unblock the CR")

		instanceID := cloudctlaws.ResourceName(cr.Namespace, cr.Name, "rds", "orders-db", 63)
		Expect(fakeRDS.instances).To(HaveKey(instanceID))
	})

	It("grants ingress to a consumer's security group the moment it publishes its pod identity, with no manual producer reconcile", func() {
		grant := &depsv1alpha1.RDSSubnetGroupGrant{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-identity-grant-"},
			Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "identity-watch-subnet-group", AllowedNamespaces: []string{testNS}},
		}
		Expect(k8sClient.Create(ctx, grant)).To(Succeed())

		producer := &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-identity-producer-", Namespace: testNS},
			Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
				Name:              "orders-db",
				DBSubnetGroupName: "identity-watch-subnet-group",
				Engine:            "postgres",
				EngineVersion:     "16.3",
				InstanceClass:     "db.t4g.micro",
				AllocatedStorage:  20,
			}}}},
		}
		Expect(k8sClient.Create(ctx, producer)).To(Succeed())

		consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{GenerateName: "watch-identity-consumer-", Namespace: testNS}}
		Expect(k8sClient.Create(ctx, consumer)).To(Succeed())

		// Wait for the producer's own security group to exist, then share
		// it with the consumer - still no ingress yet, since the consumer
		// hasn't published any pod identity. Looked up by its own
		// deterministic name rather than "whatever's in fakeEC2.groups",
		// since this Describe is Ordered and the prior It's own producer
		// security group is still sitting in the same fake's map.
		producerSGName := cloudctlaws.ResourceName(testNS, producer.Name, "rds-sg", "orders-db", 255)
		var producerSGID string
		Eventually(func() bool {
			var current depsv1alpha1.AppDependencies
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(producer), &current); err != nil {
				return false
			}
			entry := status.FindManagedResource(current.Status.ManagedResources, resourceTypeRDS, "orders-db")
			return entry != nil && entry.State == depsv1alpha1.ManagedResourceStateVerified
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue(), "expected the producer's instance to become Verified")

		for id, g := range fakeEC2.groups {
			if g.name == producerSGName {
				producerSGID = id
				break
			}
		}
		Expect(producerSGID).NotTo(BeEmpty())
		Expect(fakeEC2.groups[producerSGID].ingress).To(BeEmpty(), "expected no ingress yet - the consumer hasn't published an identity")

		var updatedProducer depsv1alpha1.AppDependencies
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(producer), &updatedProducer)).To(Succeed())
		updatedProducer.Spec.RDS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: testNS, Name: consumer.Name}}
		Expect(k8sClient.Update(ctx, &updatedProducer)).To(Succeed())

		// Now the consumer declares it consumes this instance - its own
		// reconcile publishes a pod identity, which (via the new watch)
		// must immediately wake the producer back up, with no manual
		// Reconcile call on the producer from this test.
		var updatedConsumer depsv1alpha1.AppDependencies
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(consumer), &updatedConsumer)).To(Succeed())
		updatedConsumer.Spec.RDS = &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: testNS, Name: producer.Name, ResourceName: "orders-db"},
		}}
		Expect(k8sClient.Update(ctx, &updatedConsumer)).To(Succeed())

		Eventually(func() []fakeIngressRule {
			return fakeEC2.groups[producerSGID].ingress
		}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty(), "expected the producer to grant ingress without any manual reconcile")
	})
})
