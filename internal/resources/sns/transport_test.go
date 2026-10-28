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

package sns

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func eventsTopic(t *testing.T, client *fakeSNS) *fakeTopic {
	t.Helper()
	name := cloudctlaws.ResourceName("default", "checkout-service", "sns", "events", 256)
	topic, ok := client.topics[cloudctlaws.TopicARN(testRegion, testAccountID, name)]
	if !ok {
		t.Fatal("expected the topic to exist")
	}
	return topic
}

func ensureEvents(t *testing.T, client *fakeSNS, recordEvent func(string, string, string)) {
	t.Helper()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	if _, err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
}

func TestEnsure_ATopicRefusesPlainHTTP(t *testing.T) {
	client := newFakeSNS()
	recordEvent, events := newEventCollector()

	ensureEvents(t, client, recordEvent)

	topic := eventsTopic(t, client)
	for _, want := range []string{`"Effect":"Deny"`, `"sns:Publish"`, `"sns:Subscribe"`, `"aws:SecureTransport":"false"`, topic.arn} {
		if !strings.Contains(topic.policy, want) {
			t.Errorf("the topic policy should contain %s, got %s", want, topic.policy)
		}
	}
	if strings.Contains(topic.policy, "sns:*") {
		t.Errorf("SNS refuses a wildcard action in a topic policy, got %s", topic.policy)
	}
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("expected one TransportSecurityEnforced event, got %+v", *events)
	}
}

func TestEnsure_TheTransportStatementIsNotRewrittenOnTheNextPass(t *testing.T) {
	client := newFakeSNS()
	recordEvent, events := newEventCollector()
	ensureEvents(t, client, recordEvent)
	before := eventsTopic(t, client).policy

	ensureEvents(t, client, recordEvent)

	if got := eventsTopic(t, client).policy; got != before {
		t.Errorf("the policy was rewritten with nothing to change:\n%s\n%s", before, got)
	}
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("the event belongs to adding the statement, got %+v", *events)
	}
}

// AWS gives every topic a default statement; it must survive.
func TestEnsure_TheTransportStatementKeepsAWSsDefaultStatementAndTheDeletionDeny(t *testing.T) {
	client := newFakeSNS()
	ensureEvents(t, client, nil)
	topic := eventsTopic(t, client)
	topic.policy = `{"Version":"2012-10-17","Statement":[{"Sid":"__default_statement_ID","Effect":"Allow","Principal":{"AWS":"*"},"Action":["SNS:Publish"],"Resource":"` + topic.arn + `","Condition":{"StringEquals":{"AWS:SourceOwner":"123456789012"}}}]}`
	if err := addPendingDeletionDeny(context.Background(), client, topic.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}

	ensureEvents(t, client, nil)

	for _, sid := range []string{"__default_statement_ID", pendingDeletionDenySid, awspolicy.InsecureTransportSid} {
		if !strings.Contains(topic.policy, `"`+sid+`"`) {
			t.Errorf("statement %s is missing from %s", sid, topic.policy)
		}
	}
}

func expireTrust(ledger []depsv1alpha1.ManagedResource) {
	old := metav1.NewTime(time.Now().Add(-status.TrustWindow - time.Minute))
	for i := range ledger {
		ledger[i].LastVerifiedAt = &old
	}
}

// The statement is checked when the topic is verified, once per trust window,
// not with a read on every pass.
func TestEnsure_TheTransportStatementIsCheckedOncePerTrustWindow(t *testing.T) {
	client := newFakeSNS()
	spec := &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}}
	ensure := func(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
		t.Helper()
		out, err := Ensure(context.Background(), client, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger, nil, nil)
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
		return out
	}
	ledger := ensure(nil)
	topic := eventsTopic(t, client)
	topic.policy = "" // someone removes the statement

	ledger = ensure(ledger)
	if topic.policy != "" {
		t.Fatalf("a pass trusted from the ledger read the policy and wrote it back: %s", topic.policy)
	}

	expireTrust(ledger)
	ensure(ledger)
	if !strings.Contains(topic.policy, awspolicy.InsecureTransportSid) {
		t.Errorf("the statement was not restored when the topic was verified again: %q", topic.policy)
	}
}
