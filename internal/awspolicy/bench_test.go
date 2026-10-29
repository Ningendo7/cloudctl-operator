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
	"fmt"
	"testing"
)

// policyOf builds a policy of n foreign statements plus the transport one,
// which is the shape a pass that changes nothing reads.
func policyOf(b *testing.B, n int) []json.RawMessage {
	b.Helper()
	statements := make([]json.RawMessage, 0, n+1)
	for i := range n {
		statements = append(statements, json.RawMessage(fmt.Sprintf(
			`{"Sid":"team%d","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:role/r%d"},"Action":["sqs:SendMessage"],"Resource":"arn:aws:sqs:us-east-1:111122223333:q"}`, i, i)))
	}
	encoded, err := json.Marshal(DenyInsecureTransport("sqs:*", "arn:aws:sqs:us-east-1:111122223333:q"))
	if err != nil {
		b.Fatal(err)
	}
	return append(statements, encoded)
}

func BenchmarkUpsert_NothingToChange(b *testing.B) {
	statements := policyOf(b, 10)
	want := DenyInsecureTransport("sqs:*", "arn:aws:sqs:us-east-1:111122223333:q")
	b.ReportAllocs()
	for b.Loop() {
		if _, changed, err := Upsert(statements, InsecureTransportSid, want); err != nil || changed {
			b.Fatalf("changed = %v, err = %v", changed, err)
		}
	}
}

func BenchmarkRemove_NothingToRemove(b *testing.B) {
	statements := policyOf(b, 10)
	b.ReportAllocs()
	for b.Loop() {
		Remove(statements, "cloudctl-pending-deletion-deny")
	}
}
