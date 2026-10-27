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
	"slices"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
)

const keyARN = "arn:aws:kms:us-east-1:123456789012:key/abc"

// crWith builds a CR declaring one entry of each kind with the given
// encryption, and a ledger holding each entry stamped with a KMS key.
func crWith(encryption *depsv1alpha1.EncryptionSpec, stamped bool) *depsv1alpha1.AppDependencies {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "checkout", Generation: 1},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS:      &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", Encryption: encryption}}},
			SNS:      &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", Encryption: encryption}}},
			DynamoDB: &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{Name: "sessions", Encryption: encryption}}},
			S3:       &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", Encryption: encryption}}},
		},
	}
	key := ""
	if stamped {
		key = keyARN
	}
	for _, e := range []struct{ resourceType, name string }{
		{"sqs", "orders"}, {"sns", "events"}, {"dynamodb", "sessions"}, {"s3", "receipts"},
	} {
		cr.Status.ManagedResources = append(cr.Status.ManagedResources, depsv1alpha1.ManagedResource{
			Type: e.resourceType, Name: e.name, KMSKeyARN: key,
		})
	}
	return cr
}

func encryptionCondition(t *testing.T, cr *depsv1alpha1.AppDependencies) *metav1.Condition {
	t.Helper()
	cond := apimeta.FindStatusCondition(cr.Status.Conditions, conditionTypeEncryptionMatchesSpec)
	if cond == nil {
		t.Fatal("expected an EncryptionMatchesSpec condition")
	}
	return cond
}

func TestCheckEncryptionMatchesSpec_ADroppedEncryptionIsReportedForEveryResourceType(t *testing.T) {
	for name, encryption := range map[string]*depsv1alpha1.EncryptionSpec{
		"encryption removed from the entry": nil,
		"encryption turned off":             {Enabled: false},
	} {
		t.Run(name, func(t *testing.T) {
			cr := crWith(encryption, true)
			var events []string
			checkEncryptionMatchesSpec(cr, func(eventType, reason, _ string) { events = append(events, eventType+"/"+reason) })

			cond := encryptionCondition(t, cr)
			if cond.Status != metav1.ConditionFalse || cond.Reason != reasons.EncryptionRemovalIgnored {
				t.Fatalf("EncryptionMatchesSpec = %s/%s, want False/%s", cond.Status, cond.Reason, reasons.EncryptionRemovalIgnored)
			}
			for _, want := range []string{`sqs "orders"`, `sns "events"`, `dynamodb "sessions"`, `s3 "receipts"`} {
				if !strings.Contains(cond.Message, want) {
					t.Errorf("the message should name %s, got %q", want, cond.Message)
				}
			}
			if !slices.Equal(events, []string{"Warning/" + reasons.EncryptionRemovalIgnored}) {
				t.Errorf("events = %v, want one warning", events)
			}
		})
	}
}

func TestCheckEncryptionMatchesSpec_NothingToReport(t *testing.T) {
	for name, tc := range map[string]struct {
		encryption *depsv1alpha1.EncryptionSpec
		stamped    bool
	}{
		"still asks for a dedicated key": {&depsv1alpha1.EncryptionSpec{Enabled: true}, true},
		"asks for a named key":           {&depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}, true},
		"never had a key":                {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			cr := crWith(tc.encryption, tc.stamped)
			var events []string
			checkEncryptionMatchesSpec(cr, func(_, reason, _ string) { events = append(events, reason) })

			if cond := encryptionCondition(t, cr); cond.Status != metav1.ConditionTrue || cond.Reason != reasons.EncryptionAsDeclared {
				t.Errorf("EncryptionMatchesSpec = %s/%s, want True/%s", cond.Status, cond.Reason, reasons.EncryptionAsDeclared)
			}
			if len(events) != 0 {
				t.Errorf("no event expected, got %v", events)
			}
		})
	}
}

// Entries recorded before the key was stamped on them are found through the
// dedicated key's own ledger entry.
func TestCheckEncryptionMatchesSpec_FindsAnOlderEntryThroughItsDedicatedKey(t *testing.T) {
	cr := crWith(nil, false)
	cr.Status.ManagedResources = append(cr.Status.ManagedResources,
		depsv1alpha1.ManagedResource{Type: "kms", Name: kms.DedicatedKeyLedgerName("sqs", "orders"), ARN: keyARN})

	checkEncryptionMatchesSpec(cr, nil)

	cond := encryptionCondition(t, cr)
	if cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, `sqs "orders"`) {
		t.Errorf("EncryptionMatchesSpec = %s %q, want False naming the queue", cond.Status, cond.Message)
	}
}

// The condition is not part of Ready: the resource works as it did.
func TestCheckEncryptionMatchesSpec_DoesNotTouchReady(t *testing.T) {
	cr := crWith(nil, true)
	checkEncryptionMatchesSpec(cr, nil)
	if slices.Contains(sectionTypes, conditionTypeEncryptionMatchesSpec) {
		t.Error("EncryptionMatchesSpec must stay out of sectionTypes, or a mismatch would flip Ready")
	}
}
