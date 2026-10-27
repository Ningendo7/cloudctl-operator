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

// Package awspolicy holds the small pieces of resource-policy handling every
// service package needs: adding, replacing and removing the statements this
// operator owns, found by Sid, without touching anyone else's. Statements are
// kept as raw JSON because AWS policy fields can be a string or a list
// depending on who wrote them, and a statement we do not own is never
// reinterpreted.
package awspolicy

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// InsecureTransportSid is the statement that refuses plain-HTTP requests.
const InsecureTransportSid = "cloudctl-deny-insecure-transport"

// DenyInsecureTransport is a statement refusing every action of a service on
// the given resources unless the request used TLS.
func DenyInsecureTransport(action string, resources ...string) map[string]any {
	return map[string]any{
		"Sid":       InsecureTransportSid,
		"Effect":    "Deny",
		"Principal": "*",
		"Action":    action,
		"Resource":  resources,
		"Condition": map[string]any{"Bool": map[string]any{"aws:SecureTransport": "false"}},
	}
}

// Upsert puts stmt in statements in place of the one with the same Sid, or at
// the end when there is none, and reports whether anything changed. A
// statement that already says the same is left as it is, so a policy is only
// written when it differs.
func Upsert(statements []json.RawMessage, sid string, stmt any) ([]json.RawMessage, bool, error) {
	encoded, err := json.Marshal(stmt)
	if err != nil {
		return statements, false, fmt.Errorf("encoding policy statement %q: %w", sid, err)
	}
	for i, existing := range statements {
		if sidOf(existing) != sid {
			continue
		}
		if equivalent(existing, encoded) {
			return statements, false, nil
		}
		out := append([]json.RawMessage{}, statements...)
		out[i] = encoded
		return out, true, nil
	}
	return append(append([]json.RawMessage{}, statements...), encoded), true, nil
}

// Remove drops the statement with the given Sid and reports whether there
// was one. Every other statement is kept.
func Remove(statements []json.RawMessage, sid string) ([]json.RawMessage, bool) {
	kept := make([]json.RawMessage, 0, len(statements))
	for _, s := range statements {
		if sidOf(s) != sid {
			kept = append(kept, s)
		}
	}
	return kept, len(kept) != len(statements)
}

func sidOf(statement json.RawMessage) string {
	var probe struct {
		Sid string `json:"Sid"`
	}
	if err := json.Unmarshal(statement, &probe); err != nil {
		return ""
	}
	return probe.Sid
}

// equivalent compares two statements as JSON values, so key order and
// whitespace do not count as a difference. A one-element list counts as its
// element, which is how a stored policy may hand it back: without that, a
// statement written with a list of one resource would look different on
// every read and be rewritten each time.
func equivalent(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(collapseSingletons(x), collapseSingletons(y))
}

func collapseSingletons(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = collapseSingletons(e)
		}
		return out
	case []any:
		if len(t) == 1 {
			return collapseSingletons(t[0])
		}
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = collapseSingletons(e)
		}
		return out
	default:
		return v
	}
}
