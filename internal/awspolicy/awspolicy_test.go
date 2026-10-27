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

package awspolicy

import (
	"encoding/json"
	"strings"
	"testing"
)

func raw(t *testing.T, s string) json.RawMessage {
	t.Helper()
	if !json.Valid([]byte(s)) {
		t.Fatalf("not JSON: %s", s)
	}
	return json.RawMessage(s)
}

func TestUpsert_AddsAStatementAndLeavesTheOthersAlone(t *testing.T) {
	foreign := raw(t, `{"Sid":"theirs","Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject"],"Resource":"*"}`)

	got, changed, err := Upsert([]json.RawMessage{foreign}, InsecureTransportSid, DenyInsecureTransport("s3:*", "arn:aws:s3:::b", "arn:aws:s3:::b/*"))

	if err != nil || !changed || len(got) != 2 {
		t.Fatalf("Upsert() = %d statements, changed %v, err %v; want 2, true, nil", len(got), changed, err)
	}
	if string(got[0]) != string(foreign) {
		t.Errorf("a statement we do not own was rewritten: %s", got[0])
	}
	if !strings.Contains(string(got[1]), `"aws:SecureTransport":"false"`) {
		t.Errorf("the added statement should deny insecure transport, got %s", got[1])
	}
}

func TestUpsert_ASameStatementIsNotAChange(t *testing.T) {
	// Written with another key order and spacing, as AWS may hand it back.
	existing := raw(t, `{ "Resource": ["arn:aws:sqs:us-east-1:123456789012:q"], "Condition": {"Bool": {"aws:SecureTransport": "false"}},
		"Action": "sqs:*", "Principal": "*", "Effect": "Deny", "Sid": "`+InsecureTransportSid+`" }`)

	got, changed, err := Upsert([]json.RawMessage{existing}, InsecureTransportSid, DenyInsecureTransport("sqs:*", "arn:aws:sqs:us-east-1:123456789012:q"))

	if err != nil || changed || len(got) != 1 {
		t.Errorf("Upsert() = %d statements, changed %v, err %v; want 1, false, nil", len(got), changed, err)
	}
}

func TestUpsert_ADifferentStatementUnderOurSidIsReplaced(t *testing.T) {
	existing := raw(t, `{"Sid":"`+InsecureTransportSid+`","Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"}`)

	got, changed, err := Upsert([]json.RawMessage{existing}, InsecureTransportSid, DenyInsecureTransport("s3:*", "arn:aws:s3:::b"))

	if err != nil || !changed || len(got) != 1 || !strings.Contains(string(got[0]), `"Deny"`) {
		t.Errorf("Upsert() = %s, changed %v, err %v; want our deny in place of the edited statement", got, changed, err)
	}
}

func TestRemove_DropsOnlyOurSid(t *testing.T) {
	ours := raw(t, `{"Sid":"`+InsecureTransportSid+`","Effect":"Deny"}`)
	theirs := raw(t, `{"Sid":"theirs","Effect":"Allow"}`)
	noSid := raw(t, `{"Effect":"Allow"}`)

	got, changed := Remove([]json.RawMessage{ours, theirs, noSid}, InsecureTransportSid)

	if !changed || len(got) != 2 || string(got[0]) != string(theirs) || string(got[1]) != string(noSid) {
		t.Errorf("Remove() = %s, changed %v; want the other two statements untouched", got, changed)
	}
	if _, changed := Remove(got, InsecureTransportSid); changed {
		t.Error("removing a statement that is not there should report no change")
	}
}

// AWS may hand a stored statement back with a one-element list written as its
// element, or the other way round. Not counting that as a difference keeps a
// pass that changes nothing from rewriting the policy.
func TestUpsert_AOneElementListAndItsElementAreTheSameStatement(t *testing.T) {
	stored := raw(t, `{"Sid":"`+InsecureTransportSid+`","Effect":"Deny","Principal":"*","Action":"sqs:*",
		"Resource":"arn:aws:sqs:us-east-1:123456789012:q","Condition":{"Bool":{"aws:SecureTransport":"false"}}}`)

	got, changed, err := Upsert([]json.RawMessage{stored}, InsecureTransportSid, DenyInsecureTransport("sqs:*", "arn:aws:sqs:us-east-1:123456789012:q"))

	if err != nil || changed || len(got) != 1 {
		t.Errorf("Upsert() = %d statements, changed %v, err %v; want the stored statement left alone", len(got), changed, err)
	}
}

func TestUpsert_TwoResourcesAreNotCollapsedIntoOne(t *testing.T) {
	stored := raw(t, `{"Sid":"`+InsecureTransportSid+`","Effect":"Deny","Principal":"*","Action":"s3:*",
		"Resource":["arn:aws:s3:::b"],"Condition":{"Bool":{"aws:SecureTransport":"false"}}}`)

	_, changed, err := Upsert([]json.RawMessage{stored}, InsecureTransportSid, DenyInsecureTransport("s3:*", "arn:aws:s3:::b", "arn:aws:s3:::b/*"))

	if err != nil || !changed {
		t.Errorf("Upsert() changed %v, err %v; a statement missing a resource must be replaced", changed, err)
	}
}
