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
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/controller/predicates"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

var testIdentity = cloudctlaws.NewIdentity("us-east-1", "123456789012")

// ledgerTestCR declares one of each kind the controller's fakes can create,
// encrypted, with a DLQ and FIFO, so its ledger holds every naming path those
// kinds have.
func ledgerTestCR() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		Namespace: "default", Name: "app",
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "orders", DLQ: true, Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
				{Name: "jobs", FIFO: true},
			}},
			SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}, {Name: "ordered", FIFO: true}}},
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "shared"}}},
		},
	}
}

// What the operator creates must pass its own check, or upgrading would stop
// every CR.
func TestLedgerCheck_EverythingTheOperatorCreatesPasses(t *testing.T) {
	cr := ledgerTestCR()
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	request := reconcile.Request{Namespace: cr.Namespace, Name: cr.Name}
	for range 2 {
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
	}

	var stored depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	if err := checkLedger(testIdentity, &stored); err != nil {
		t.Fatalf("the operator's own ledger fails the check: %v", err)
	}
	seen := map[string]bool{}
	for _, e := range stored.Status.ManagedResources {
		seen[e.Type] = true
	}
	for _, want := range []string{resourceTypeSQS, resourceTypeSNS, resourceTypeKMS, resourceTypeIAM} {
		if !seen[want] {
			t.Errorf("no %s entry in the ledger %+v; the test no longer covers it", want, stored.Status.ManagedResources)
		}
	}
	for _, c := range stored.Status.Conditions {
		if c.Reason == reasons.LedgerTampered {
			t.Errorf("condition %s is %s", c.Type, c.Reason)
		}
	}
}

// A forged entry stops the pass before it makes a single AWS call.
func TestLedgerCheck_AForgedEntryStopsThePassBeforeAWS(t *testing.T) {
	cr := ledgerTestCR()
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	calls := countAWSCalls(r.AWSClients)
	request := reconcile.Request{Namespace: cr.Namespace, Name: cr.Name}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var stored depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	for i, e := range stored.Status.ManagedResources {
		if e.Type == resourceTypeSQS && e.Name == "jobs" {
			stored.Status.ManagedResources[i].ARN = testIdentity.RegionalARN("sqs", cloudctlaws.QueueName("payments", "billing", true, "ledger"))
		}
	}
	if err := r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}

	clear(calls)
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("a pass with a forged entry called AWS: %v", calls)
	}
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	section := apimeta.FindStatusCondition(stored.Status.Conditions, conditionTypeSQSReady)
	ready := apimeta.FindStatusCondition(stored.Status.Conditions, status.ConditionTypeReady)
	if section == nil || section.Reason != reasons.LedgerTampered || ready == nil || ready.Reason != reasons.LedgerTampered {
		t.Errorf("SQSReady = %+v, Ready = %+v; want both %s", section, ready, reasons.LedgerTampered)
	}
}

func TestCheckLedger(t *testing.T) {
	const ns, cr = "default", "app"
	other := cloudctlaws.NewIdentity("us-east-1", "999999999999")
	queue := cloudctlaws.QueueName(ns, cr, false, "orders")
	dlq := cloudctlaws.QueueName(ns, cr, false, "orders", "dlq")
	topic := cloudctlaws.TopicName(ns, cr, false, "events")
	table := cloudctlaws.TableName(ns, cr, "sessions")
	instance := cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, ns, cr, "rds", "orders-db", 63)
	role := cloudctlaws.ResourceName(ns, cr, "iam", "role", 64)
	producerTopic := cloudctlaws.TopicName("shop", "checkout", false, "orders")

	for name, tc := range map[string]struct {
		entry depsv1alpha1.ManagedResource
		ok    bool
	}{
		"a queue":                  {depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: testIdentity.RegionalARN("sqs", queue)}, true},
		"a DLQ":                    {depsv1alpha1.ManagedResource{Type: "sqs", Name: cloudctlaws.DerivedKey("orders", "dlq"), ARN: testIdentity.RegionalARN("sqs", dlq)}, true},
		"another CR's queue":       {depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: testIdentity.RegionalARN("sqs", cloudctlaws.QueueName("shop", "checkout", false, "orders"))}, false},
		"a queue in another acct":  {depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: other.RegionalARN("sqs", queue)}, false},
		"a DLQ named as the queue": {depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: testIdentity.RegionalARN("sqs", dlq)}, false},
		"a topic":                  {depsv1alpha1.ManagedResource{Type: "sns", Name: "events", ARN: testIdentity.RegionalARN("sns", topic)}, true},
		"another CR's topic":       {depsv1alpha1.ManagedResource{Type: "sns", Name: "events", ARN: testIdentity.RegionalARN("sns", producerTopic)}, false},
		"a table":                  {depsv1alpha1.ManagedResource{Type: "dynamodb", Name: "sessions", ARN: testIdentity.RegionalARN("dynamodb", "table/"+table)}, true},
		"another table":            {depsv1alpha1.ManagedResource{Type: "dynamodb", Name: "sessions", ARN: testIdentity.RegionalARN("dynamodb", "table/payments")}, false},
		"an instance":              {depsv1alpha1.ManagedResource{Type: "rds", Name: "orders-db", ARN: testIdentity.RegionalARN("rds", "db:"+instance)}, true},
		"another team's instance":  {depsv1alpha1.ManagedResource{Type: "rds", Name: "orders-db", ARN: testIdentity.RegionalARN("rds", "db:billing-prod")}, false},
		"the role":                 {depsv1alpha1.ManagedResource{Type: "iam", Name: "role", ARN: testIdentity.GlobalARN("iam", "role/"+role)}, true},
		"the role under a path":    {depsv1alpha1.ManagedResource{Type: "iam", Name: "role", ARN: testIdentity.GlobalARN("iam", "role/cloudctl/"+role)}, true},
		"an admin role":            {depsv1alpha1.ManagedResource{Type: "iam", Name: "role", ARN: testIdentity.GlobalARN("iam", "role/Admin")}, false},
		"a key":                    {depsv1alpha1.ManagedResource{Type: "kms", Name: "shared", ARN: testIdentity.RegionalARN("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab")}, true},
		"a key in another account": {depsv1alpha1.ManagedResource{Type: "kms", Name: "shared", ARN: other.RegionalARN("kms", "key/1234abcd-12ab-34cd-56ef-1234567890ab")}, false},
		"a security group":         {depsv1alpha1.ManagedResource{Type: "ec2-securitygroup", Name: "pod-network-identity", ARN: testIdentity.RegionalARN("ec2", "security-group/sg-0123456789abcdef0")}, true},
		"a group in another acct":  {depsv1alpha1.ManagedResource{Type: "ec2-securitygroup", Name: "pod-network-identity", ARN: other.RegionalARN("ec2", "security-group/sg-0123456789abcdef0")}, false},
		"a subscription":           {depsv1alpha1.ManagedResource{Type: "sqssubscription", Name: "orders/subscribes-to/shop/checkout/orders", ARN: testIdentity.RegionalARN("sns", producerTopic) + ":0b1c2d3e"}, true},
		"a subscription elsewhere": {depsv1alpha1.ManagedResource{Type: "sqssubscription", Name: "orders/subscribes-to/shop/checkout/orders", ARN: testIdentity.RegionalARN("sns", "billing-alerts") + ":0b1c2d3e"}, false},
		"an unknown type":          {depsv1alpha1.ManagedResource{Type: "future-kind", Name: "x", ARN: "anything"}, true},
		"no ARN yet":               {depsv1alpha1.ManagedResource{Type: "rds", Name: "orders-db"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			c := &depsv1alpha1.AppDependencies{Namespace: ns, Name: cr}
			c.Status.ManagedResources = []depsv1alpha1.ManagedResource{tc.entry}
			err := checkLedger(testIdentity, c)
			if tc.ok != (err == nil) {
				t.Fatalf("checkLedger() = %v, want ok %v", err, tc.ok)
			}
			if !tc.ok {
				if _, isMismatch := errors.AsType[*cloudctlaws.LedgerMismatchError](err); !isMismatch {
					t.Errorf("checkLedger() = %T, want a LedgerMismatchError", err)
				}
			}
		})
	}
}

// A key entry pointing at another CR's real key, in this account and region,
// passes the ARN check; its tags stop it.
func TestLedgerCheck_AForgedKeyEntryIsCaughtByItsTags(t *testing.T) {
	cr := ledgerTestCR()
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	kmsFake := r.AWSClients.KMS.(*fakeKMSClient)
	calls := countAWSCalls(r.AWSClients)
	request := reconcile.Request{Namespace: cr.Namespace, Name: cr.Name}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	foreign := testIdentity.RegionalARN("kms", "key/0f0f0f0f-1111-2222-3333-444444444444")
	kmsFake.keys[foreign] = &fakeKey{
		arn: foreign, keyID: "0f0f0f0f-1111-2222-3333-444444444444",
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("payments", "billing"), cloudctlaws.OwnerUIDTagKey: "uid-billing"},
	}
	var stored depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	for i, e := range stored.Status.ManagedResources {
		if e.Type == resourceTypeKMS && e.Name == "shared" {
			stored.Status.ManagedResources[i].ARN = foreign
		}
	}
	if err := r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}

	clear(calls)
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("a pass with a forged key wrote to AWS: %v", writes)
	}
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	if c := apimeta.FindStatusCondition(stored.Status.Conditions, conditionTypeKMSReady); c == nil || c.Reason != reasons.LedgerTampered {
		t.Errorf("KMSReady = %+v, want %s", c, reasons.LedgerTampered)
	}
}

func TestVerifyChosenID(t *testing.T) {
	const ns, name = "default", "app"
	keyARN := testIdentity.RegionalARN("kms", "key/0a0a0a0a-1111-2222-3333-444444444444")
	setup := func(tags map[string]string) (*AppDependenciesReconciler, *depsv1alpha1.AppDependencies, awsCalls) {
		kmsClient := newFakeKMSClient()
		kmsClient.keys[keyARN] = &fakeKey{arn: keyARN, keyID: "0a0a0a0a-1111-2222-3333-444444444444", tags: tags}
		r := &AppDependenciesReconciler{AWSClients: &cloudctlaws.Clients{KMS: kmsClient}}
		calls := countAWSCalls(r.AWSClients)
		cr := &depsv1alpha1.AppDependencies{Namespace: ns, Name: name, UID: "uid-now"}
		cr.Status.ManagedResources = []depsv1alpha1.ManagedResource{{Type: resourceTypeKMS, Name: "shared", ARN: keyARN}}
		return r, cr, calls
	}
	owner := func(uid string) map[string]string {
		return map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue(ns, name), cloudctlaws.OwnerUIDTagKey: uid}
	}

	t.Run("its own key is read once per process", func(t *testing.T) {
		r, cr, calls := setup(owner("uid-now"))
		for range 3 {
			if err := r.verifyChosenID(context.Background(), cr, cr.Status.ManagedResources[0]); err != nil {
				t.Fatalf("verifyChosenID() = %v", err)
			}
		}
		if n := calls["kms.ListResourceTags"]; n != 1 {
			t.Errorf("ListResourceTags called %d times, want 1", n)
		}
	})
	t.Run("a key from an earlier CR is refused", func(t *testing.T) {
		r, cr, _ := setup(owner("uid-before"))
		if err := r.verifyChosenID(context.Background(), cr, cr.Status.ManagedResources[0]); err == nil {
			t.Fatal("verifyChosenID() accepted a key tagged with another UID")
		}
	})
	t.Run("unless the CR was told to take over from it", func(t *testing.T) {
		r, cr, _ := setup(owner("uid-before"))
		ctx := cloudctlaws.WithAdoptFromUID(context.Background(), "uid-before")
		if err := r.verifyChosenID(ctx, cr, cr.Status.ManagedResources[0]); err != nil {
			t.Fatalf("verifyChosenID() = %v, want the takeover allowed", err)
		}
	})
	t.Run("a relinquished key is not read", func(t *testing.T) {
		r, cr, calls := setup(nil)
		cr.Status.ManagedResources[0].State = depsv1alpha1.ManagedResourceStateRelinquished
		if err := r.verifyChosenID(context.Background(), cr, cr.Status.ManagedResources[0]); err != nil || calls["kms.ListResourceTags"] != 0 {
			t.Fatalf("verifyChosenID() = %v after %d reads, want nil and none", err, calls["kms.ListResourceTags"])
		}
	})
	t.Run("a key that is gone is left to the KMS section", func(t *testing.T) {
		r, cr, _ := setup(nil)
		cr.Status.ManagedResources[0].ARN = testIdentity.RegionalARN("kms", "key/ffffffff-1111-2222-3333-444444444444")
		if err := r.verifyChosenID(context.Background(), cr, cr.Status.ManagedResources[0]); err != nil {
			t.Fatalf("verifyChosenID() = %v, want nil", err)
		}
	})
}

// A producer's ledger is read for the consumer's grants. An entry in it the
// operator would not have written grants nothing, and the consumer waits for
// it like an unresolved reference rather than being stopped.
func TestLedgerCheck_AForgedProducerEntryGrantsTheConsumerNothing(t *testing.T) {
	r := sharingReconciler(t)
	ctx := context.Background()
	role := cloudctlaws.ResourceName("default", "consumer", "iam", "role", 64)
	policy := func() string {
		return r.AWSClients.IAM.(*fakeIAMClient).roles[role].policies["cloudctl-derived-policy"]
	}
	queueARN, keyARN := producerARNs(t, r)
	if !strings.Contains(policy(), queueARN) || !strings.Contains(policy(), keyARN) {
		t.Fatalf("setup: the consumer's policy %s lacks the queue %s or key %s", policy(), queueARN, keyARN)
	}

	foreignKey := testIdentity.RegionalARN("kms", "key/0f0f0f0f-1111-2222-3333-444444444444")
	r.AWSClients.KMS.(*fakeKMSClient).keys[foreignKey] = &fakeKey{
		arn: foreignKey, keyID: "0f0f0f0f-1111-2222-3333-444444444444",
		tags: map[string]string{cloudctlaws.OwnerTagKey: cloudctlaws.OwnerTagValue("payments", "billing"), cloudctlaws.OwnerUIDTagKey: "uid-billing"},
	}
	for name, forged := range map[string]struct{ entryType, arn string }{
		"another CR's queue": {resourceTypeSQS, testIdentity.RegionalARN("sqs", cloudctlaws.QueueName("payments", "billing", false, "ledger"))},
		"another CR's key":   {resourceTypeKMS, foreignKey},
	} {
		t.Run(name, func(t *testing.T) {
			forgeProducer(t, r, forged.entryType, forged.arn)

			// An unresolved consumes is a wait, as for a producer not found yet.
			_, err := r.Reconcile(ctx, reconcile.Request{Namespace: "default", Name: "consumer"})
			if wait, ok := errors.AsType[*cloudctlaws.ReconcileError](err); !ok || !wait.WakesOnChange || !strings.Contains(err.Error(), "not one the operator wrote") {
				t.Fatalf("Reconcile(consumer) = %v; want the wait for an unresolved consumes", err)
			}
			if got := policy(); strings.Contains(got, forged.arn) {
				t.Errorf("the consumer's policy grants the forged entry: %s", got)
			}
		})
	}
}

// sharingReconciler reconciles a producer sharing an encrypted queue and the
// consumer of it.
func sharingReconciler(t *testing.T) *AppDependenciesReconciler {
	t.Helper()
	producer := &depsv1alpha1.AppDependencies{
		Namespace: "default", Name: "producer",
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{
			Name:       "orders",
			Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true},
			SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "default", Name: "consumer", Access: depsv1alpha1.AccessLevelReadWrite}},
		}}}},
	}
	consumer := &depsv1alpha1.AppDependencies{
		Namespace: "default", Name: "consumer",
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "producer", ResourceName: "orders"},
		}}},
	}
	patches := 0
	r := newCountingReconciler(t, &patches, producer, consumer)
	for _, name := range []string{"producer", "consumer"} {
		if _, err := r.Reconcile(context.Background(), reconcile.Request{Namespace: "default", Name: name}); err != nil {
			t.Fatalf("Reconcile(%s) error = %v", name, err)
		}
	}
	return r
}

func producerARNs(t *testing.T, r *AppDependenciesReconciler) (queueARN, keyARN string) {
	t.Helper()
	var p depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "producer"}, &p); err != nil {
		t.Fatal(err)
	}
	for _, e := range p.Status.ManagedResources {
		switch e.Type {
		case resourceTypeSQS:
			queueARN = e.ARN
		case resourceTypeKMS:
			keyARN = e.ARN
		}
	}
	return queueARN, keyARN
}

// forgeProducer sets the ARN of the producer's entries of one type, and puts
// them back when the test ends.
func forgeProducer(t *testing.T, r *AppDependenciesReconciler, entryType, arn string) {
	t.Helper()
	ctx := context.Background()
	key := client.ObjectKey{Namespace: "default", Name: "producer"}
	var p depsv1alpha1.AppDependencies
	if err := r.Get(ctx, key, &p); err != nil {
		t.Fatal(err)
	}
	original := p.Status.DeepCopy().ManagedResources
	for i := range p.Status.ManagedResources {
		if p.Status.ManagedResources[i].Type == entryType {
			p.Status.ManagedResources[i].ARN = arn
		}
	}
	if err := r.Status().Update(ctx, &p); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var back depsv1alpha1.AppDependencies
		if err := r.Get(ctx, key, &back); err != nil {
			t.Fatal(err)
		}
		back.Status.ManagedResources = original
		if err := r.Status().Update(ctx, &back); err != nil {
			t.Fatal(err)
		}
	})
}

// withDeployment runs the test as an operator started with --cluster-name.
func withDeployment(t *testing.T, name string) {
	t.Helper()
	before := cloudctlaws.Deployment()
	cloudctlaws.SetDeployment(name)
	t.Cleanup(func() { cloudctlaws.SetDeployment(before) })
}

func TestDeployment_ACRLabelledForAnotherDeploymentIsLeftAlone(t *testing.T) {
	withDeployment(t, "prod")
	cr := ledgerTestCR()
	cr.Labels = map[string]string{predicates.DeploymentLabel: "staging"}
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	calls := countAWSCalls(r.AWSClients)

	if _, err := r.Reconcile(context.Background(), reconcile.Request{Namespace: cr.Namespace, Name: cr.Name}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(calls) != 0 || patches != 0 {
		t.Errorf("another deployment's CR got %d AWS calls and %d status writes", len(calls), patches)
	}
}

func TestDeployment_ANewCRTakesTheOperatorsNameAndKeepsIt(t *testing.T) {
	withDeployment(t, "prod")
	cr := ledgerTestCR()
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	request := reconcile.Request{Namespace: cr.Namespace, Name: cr.Name}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var stored depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Deployment != "prod" || len(stored.Status.ManagedResources) == 0 {
		t.Fatalf("status.deployment = %q with %d resources, want prod and some", stored.Status.Deployment, len(stored.Status.ManagedResources))
	}

	// The same CR seen by an operator with another name: nothing is touched.
	cloudctlaws.SetDeployment("staging")
	calls := countAWSCalls(r.AWSClients)
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("an operator with another deployment name called AWS: %v", calls)
	}
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(stored.Status.Conditions, status.ConditionTypeReady)
	if ready == nil || ready.Reason != reasons.DeploymentChanged {
		t.Errorf("Ready = %+v, want %s", ready, reasons.DeploymentChanged)
	}
}

// withCluster runs the rest of the test as the operator in the named cluster.
func withCluster(t *testing.T, name string) {
	t.Helper()
	cloudctlaws.SetCluster(name, "uid-"+name)
	t.Cleanup(func() { cloudctlaws.SetCluster("", "") })
}

// A cluster restored from another's etcd backup has the same CR, UID and
// status; it must not act on the other cluster's resources until handed them.
func TestCluster_ACRFromAnotherClusterIsStoppedUntilHandedOver(t *testing.T) {
	withCluster(t, "blue")
	cr := ledgerTestCR()
	cr.UID = "uid-1"
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	request := reconcile.Request{Namespace: cr.Namespace, Name: cr.Name}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var stored depsv1alpha1.AppDependencies
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cluster != "blue" {
		t.Fatalf("status.cluster = %q, want blue", stored.Status.Cluster)
	}

	withCluster(t, "green")
	fakeSQS := r.AWSClients.SQS.(*fakeSQSClient)
	calls := countAWSCalls(r.AWSClients)
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("green called AWS for blue's CR: %v", calls)
	}
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(stored.Status.Conditions, status.ConditionTypeReady)
	if ready == nil || ready.Reason != reasons.ClusterChanged || !strings.Contains(ready.Message, "adopt-from-uid=blue/uid-1") {
		t.Fatalf("Ready = %+v, want %s naming the handover", ready, reasons.ClusterChanged)
	}

	// Blue's role, already in the ledger, stays blue's; green's pods get
	// their own.
	blueRole, _ := status.FindManagedResource(stored.Status.ManagedResources, "iam", "role")
	if !strings.HasSuffix(blueRole.ARN, "/"+cloudctlaws.ClusterResourceName("blue", cr.Namespace, cr.Name, "iam", "role", 64)) {
		t.Fatalf("test setup broken: blue's ledger has role %q", blueRole.ARN)
	}
	stored.Annotations = map[string]string{cloudctlaws.AdoptFromUIDAnnotation: "blue/uid-1"}
	if err := r.Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() after the handover error = %v", err)
	}
	if err := r.Get(context.Background(), request.NamespacedName, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cluster != "green" {
		t.Errorf("status.cluster = %q after the handover, want green", stored.Status.Cluster)
	}
	greenRole := cloudctlaws.ClusterResourceName("green", cr.Namespace, cr.Name, "iam", "role", 64)
	if entry, _ := status.FindManagedResource(stored.Status.ManagedResources, "iam", "role"); !strings.HasSuffix(entry.ARN, "/"+greenRole) {
		t.Errorf("green's ledger has role %q, want green's own %q", entry.ARN, greenRole)
	}
	ordersQueue := fakeSQS.queues[cloudctlaws.QueueName(cr.Namespace, cr.Name, false, "orders")]
	if ordersQueue == nil || ordersQueue.tags[cloudctlaws.ClusterTagKey] != "green" {
		t.Errorf("the queue was not taken over by green: %+v", ordersQueue)
	}
}
