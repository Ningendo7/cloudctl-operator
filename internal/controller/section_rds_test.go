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
	"strings"
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
	"github.com/Ningendo7/cloudctl-operator/internal/controller/predicates"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/configmap"
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
				ByObject:          ManagedObjectCache(),
			},
		})
		Expect(err).NotTo(HaveOccurred())

		reconciler := &AppDependenciesReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
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
		Expect(fakeRDS.hasInstance(instanceID)).To(BeTrue())
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
			entry, entryFound := status.FindManagedResource(current.Status.ManagedResources, resourceTypeRDS, "orders-db")
			return entryFound && entry.State == depsv1alpha1.ManagedResourceStateVerified
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue(), "expected the producer's instance to become Verified")

		producerSGID = fakeEC2.groupIDByName(producerSGName)
		Expect(producerSGID).NotTo(BeEmpty())
		Expect(fakeEC2.ingressOf(producerSGID)).To(BeEmpty(), "expected no ingress yet - the consumer hasn't published an identity")

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
			return fakeEC2.ingressOf(producerSGID)
		}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty(), "expected the producer to grant ingress without any manual reconcile")
	})
})

// The credentials Secret holds a database password, so it gets the same
// out-of-band abuse as the ConfigMap, against a real API server and the
// production cache configuration (only operator-labelled Secrets are
// cached; anything else is only visible through the API reader).
var _ = Describe("RDS credentials Secret on a real cluster", Ordered, func() {
	const timeout, poll = 30 * time.Second, 100 * time.Millisecond

	var (
		mgrCancel context.CancelFunc
		mgrDone   chan struct{}
		secrets   *fakeSecretsManager
		nsA, nsB  string
		grant     *depsv1alpha1.RDSSubnetGroupGrant
	)

	BeforeAll(func() {
		for _, target := range []*string{&nsA, &nsB} {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "rds-secret-"}}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			*target = ns.Name
		}
		grant = &depsv1alpha1.RDSSubnetGroupGrant{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "rds-secret-grant-"},
			Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: "secret-spec-sg", AllowedNamespaces: []string{nsA, nsB}},
		}
		Expect(k8sClient.Create(ctx, grant)).To(Succeed())

		fakeRDS := newFakeRDSClient()
		fakeRDS.setExposeMasterSecret(true)
		secrets = newFakeSecretsManager("pw-1")

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
				RDS: fakeRDS, EC2: newFakeEC2Client(), SecretsManager: secrets,
				SQS: newFakeSQSClient(), SNS: newFakeSNSClient(), IAM: newFakeIAMClient(),
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
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, grant))).To(Succeed())
	})

	AfterEach(func() {
		secrets.rotate("pw-1")
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

	dbCR := func(ns, name string, sharedWith ...depsv1alpha1.SharedWithEntry) *depsv1alpha1.AppDependencies {
		return &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
				Name: "orders-db", DBSubnetGroupName: "secret-spec-sg",
				Engine: "postgres", EngineVersion: "16.3", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
				SharedWith: sharedWith,
			}}}},
		}
	}

	secretKey := func(ns, crName string) types.NamespacedName {
		return types.NamespacedName{Namespace: ns, Name: rds.CredentialsSecretName(crName)}
	}

	password := func(ns, crName string) string {
		var s corev1.Secret
		if err := k8sClient.Get(ctx, secretKey(ns, crName), &s); err != nil {
			return ""
		}
		return string(s.Data[credentialsDataKey("PASSWORD")])
	}

	It("mirrors the managed credentials into a labelled Secret owned by the CR", func() {
		cr := dbCR(nsA, "mirror")
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		Eventually(func() string { return password(nsA, "mirror") }, timeout, poll).Should(Equal("pw-1"))

		var s corev1.Secret
		Expect(k8sClient.Get(ctx, secretKey(nsA, "mirror"), &s)).To(Succeed())
		Expect(s.Labels).To(HaveKeyWithValue(depsv1alpha1.ManagedByLabelKey, depsv1alpha1.ManagedByLabelValue))
		Expect(s.OwnerReferences).To(HaveLen(1))
		Expect(s.OwnerReferences[0].Name).To(Equal(cr.Name))
		Expect(s.OwnerReferences[0].Controller).To(HaveValue(BeTrue()))
		Expect(string(s.Data[credentialsDataKey("USERNAME")])).To(Equal("cloudctl_admin"))
	})

	It("restores the Secret after it is deleted, corrupted, or loses its managed-by label", func() {
		cr := dbCR(nsA, "tamper")
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		Eventually(func() string { return password(nsA, "tamper") }, timeout, poll).Should(Equal("pw-1"))

		By("corrupting the password in place")
		var s corev1.Secret
		Expect(k8sClient.Get(ctx, secretKey(nsA, "tamper"), &s)).To(Succeed())
		s.Data[credentialsDataKey("PASSWORD")] = []byte("hacked")
		Expect(k8sClient.Update(ctx, &s)).To(Succeed())
		Eventually(func() string { return password(nsA, "tamper") }, timeout, poll).Should(Equal("pw-1"))

		By("stripping the managed-by label, which also drops it out of the operator's cache")
		Expect(k8sClient.Get(ctx, secretKey(nsA, "tamper"), &s)).To(Succeed())
		delete(s.Labels, depsv1alpha1.ManagedByLabelKey)
		Expect(k8sClient.Update(ctx, &s)).To(Succeed())
		Eventually(func() string {
			var got corev1.Secret
			_ = k8sClient.Get(ctx, secretKey(nsA, "tamper"), &got)
			return got.Labels[depsv1alpha1.ManagedByLabelKey]
		}, timeout, poll).Should(Equal(depsv1alpha1.ManagedByLabelValue))
		Expect(password(nsA, "tamper")).To(Equal("pw-1"))

		By("deleting it outright")
		Expect(k8sClient.Get(ctx, secretKey(nsA, "tamper"), &s)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &s)).To(Succeed())
		Eventually(func() string { return password(nsA, "tamper") }, timeout, poll).Should(Equal("pw-1"))
	})

	It("picks up a rotated password on the next reconcile", func() {
		cr := dbCR(nsA, "rotate")
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		Eventually(func() string { return password(nsA, "rotate") }, timeout, poll).Should(Equal("pw-1"))

		secrets.rotate("pw-2")
		updateCR(client.ObjectKeyFromObject(cr), func(current *depsv1alpha1.AppDependencies) {
			if current.Annotations == nil {
				current.Annotations = map[string]string{}
			}
			current.Annotations[predicates.ForceReconcileAnnotation] = "now"
		})

		Eventually(func() string { return password(nsA, "rotate") }, timeout, poll).Should(Equal("pw-2"))
	})

	It("refuses a pre-existing Secret it doesn't own, leaves it untouched, and takes the name once it's gone", func() {
		foreign := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: rds.CredentialsSecretName("foreign")},
			Data:       map[string][]byte{"MINE": []byte("keep")},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		Expect(k8sClient.Create(ctx, dbCR(nsA, "foreign"))).To(Succeed())

		Eventually(func() string {
			var cr depsv1alpha1.AppDependencies
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: nsA, Name: "foreign"}, &cr); err != nil {
				return ""
			}
			if c := apimeta.FindStatusCondition(cr.Status.Conditions, "ConnectionInfoReady"); c != nil && c.Status == metav1.ConditionFalse {
				return c.Message
			}
			return ""
		}, timeout, poll).Should(ContainSubstring("not owned by this CR"))

		Consistently(func() map[string][]byte {
			var got corev1.Secret
			_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), &got)
			return got.Data
		}, 2*time.Second, poll).Should(Equal(map[string][]byte{"MINE": []byte("keep")}))

		Expect(k8sClient.Delete(ctx, foreign)).To(Succeed())
		Eventually(func() string { return password(nsA, "foreign") }, timeout, poll).Should(Equal("pw-1"))
	})

	It("gives a consumer the host and port but never a credentials Secret", func() {
		Expect(k8sClient.Create(ctx, dbCR(nsA, "producer-db", depsv1alpha1.SharedWithEntry{Namespace: nsB, Name: "consumer-db"}))).To(Succeed())
		Eventually(func() string { return password(nsA, "producer-db") }, timeout, poll).Should(Equal("pw-1"))

		consumer := &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: nsB, Name: "consumer-db"},
			Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{
				Consumes: []depsv1alpha1.ConsumeRef{{Namespace: nsA, Name: "producer-db", ResourceName: "orders-db"}},
			}},
		}
		Expect(k8sClient.Create(ctx, consumer)).To(Succeed())

		Eventually(func() []string {
			var cm corev1.ConfigMap
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: nsB, Name: configmap.ConfigMapName("consumer-db")}, &cm); err != nil {
				return nil
			}
			var keys []string
			for k := range cm.Data {
				keys = append(keys, k)
			}
			return keys
		}, timeout, poll).Should(ContainElements(
			ContainSubstring("_HOST"), ContainSubstring("_PORT"),
		))

		Consistently(func() []string {
			var list corev1.SecretList
			_ = k8sClient.List(ctx, &list, client.InNamespace(nsB))
			var leaked []string
			for _, s := range list.Items {
				for k := range s.Data {
					if strings.Contains(k, "PASSWORD") || strings.Contains(k, "USERNAME") {
						leaked = append(leaked, s.Name+"/"+k)
					}
				}
			}
			return leaked
		}, 2*time.Second, poll).Should(BeEmpty(), "credentials must only ever reach the owner's namespace")
	})
})

// credentialsDataKey is the key rds writes a field of orders-db's
// credentials under.
func credentialsDataKey(field string) string {
	return "RDS_ORDERS_DB_" + field
}
