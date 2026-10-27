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

package sqs

import (
	"context"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"
	"time"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
)

func ordersQueue(t *testing.T, client *fakeSQS) *fakeQueue {
	t.Helper()
	q, ok := client.queues[cloudctlaws.ResourceName("default", "checkout-service", "sqs", "orders", 80)]
	if !ok {
		t.Fatal("expected the queue to exist")
	}
	return q
}

func ensureOrders(t *testing.T, client *fakeSQS, q depsv1alpha1.SQSQueueSpec, recordEvent func(string, string, string)) {
	t.Helper()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{q}}
	if _, err := Ensure(context.Background(), client, nil, nil, nil, "default", "checkout-service", "uid-1", spec, nil, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
}

func TestEnsure_AQueueRefusesPlainHTTP(t *testing.T) {
	client := newFakeSQS()
	recordEvent, events := newEventCollector()

	ensureOrders(t, client, depsv1alpha1.SQSQueueSpec{Name: "orders"}, recordEvent)

	q := ordersQueue(t, client)
	for _, want := range []string{`"Effect":"Deny"`, `"Action":"sqs:*"`, `"aws:SecureTransport":"false"`, q.arn} {
		if !strings.Contains(q.policy, want) {
			t.Errorf("the queue policy should contain %s, got %s", want, q.policy)
		}
	}
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("expected one TransportSecurityEnforced event, got %+v", *events)
	}
}

func TestEnsure_ADeadLetterQueueRefusesPlainHTTPToo(t *testing.T) {
	client := newFakeSQS()

	ensureOrders(t, client, depsv1alpha1.SQSQueueSpec{Name: "orders", DLQ: true}, nil)

	dlq := client.queues[cloudctlaws.DerivedResourceName("default", "checkout-service", "sqs", 80, "orders", "dlq")]
	if dlq == nil || !strings.Contains(dlq.policy, awspolicy.InsecureTransportSid) {
		t.Errorf("the dead-letter queue holds the same data and needs the statement, policy: %v", dlq)
	}
}

func TestEnsure_TheTransportStatementIsNotRewrittenOnTheNextPass(t *testing.T) {
	client := newFakeSQS()
	recordEvent, events := newEventCollector()
	ensureOrders(t, client, depsv1alpha1.SQSQueueSpec{Name: "orders"}, recordEvent)
	before := ordersQueue(t, client).policy

	ensureOrders(t, client, depsv1alpha1.SQSQueueSpec{Name: "orders"}, recordEvent)

	if got := ordersQueue(t, client).policy; got != before {
		t.Errorf("the policy was rewritten with nothing to change:\n%s\n%s", before, got)
	}
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("the event belongs to adding the statement, got %+v", *events)
	}
}

func TestEnsure_TheTransportStatementKeepsOtherStatementsAndTheDeletionDeny(t *testing.T) {
	client := newFakeSQS()
	ensureOrders(t, client, depsv1alpha1.SQSQueueSpec{Name: "orders"}, nil)
	q := ordersQueue(t, client)
	theirs := `{"Version":"2012-10-17","Statement":[{"Sid":"theirs","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Action":"sqs:SendMessage","Resource":"` + q.arn + `"}]}`
	q.policy = theirs
	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}

	ensureOrders(t, client, depsv1alpha1.SQSQueueSpec{Name: "orders"}, nil)

	for _, sid := range []string{"theirs", pendingDeletionDenySid, awspolicy.InsecureTransportSid} {
		if !strings.Contains(q.policy, `"`+sid+`"`) {
			t.Errorf("statement %s is missing from %s", sid, q.policy)
		}
	}
}

// expireTrust makes every entry look last verified long enough ago that the
// next pass checks it against AWS again.
func expireTrust(ledger []depsv1alpha1.ManagedResource) {
	old := metav1.NewTime(time.Now().Add(-status.TrustWindow - time.Minute))
	for i := range ledger {
		ledger[i].LastVerifiedAt = &old
	}
}

// The statement is checked when the queue is verified, once per trust window,
// not with a read on every pass.
func TestEnsure_TheTransportStatementIsCheckedOncePerTrustWindow(t *testing.T) {
	client := newFakeSQS()
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
	ensure := func(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
		t.Helper()
		out, err := Ensure(context.Background(), client, nil, nil, nil, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
		return out
	}
	ledger := ensure(nil)
	q := ordersQueue(t, client)
	q.policy = "" // someone removes the statement

	ledger = ensure(ledger)
	if q.policy != "" {
		t.Fatalf("a pass trusted from the ledger read the policy and wrote it back: %s", q.policy)
	}

	expireTrust(ledger)
	ensure(ledger)
	if !strings.Contains(q.policy, awspolicy.InsecureTransportSid) {
		t.Errorf("the statement was not restored when the queue was verified again: %q", q.policy)
	}
}
