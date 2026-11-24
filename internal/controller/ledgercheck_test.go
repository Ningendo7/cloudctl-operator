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
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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
