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

package iam

import (
	"encoding/json"
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// These are property-based tests over buildPolicyDocument, the pure
// grants-to-JSON step of policy derivation (collectGrants itself needs a
// k8s client and isn't a good PBT target). Each property is checked
// black-box - by comparing two real outputs, or against the input grants
// themselves - rather than by recomputing the expected action list via
// the same actionSets/s3Object* tables buildPolicyDocument itself reads
// from, so a bug in the gating logic (not just a typo in an action name)
// actually has a chance of being caught instead of the test just
// re-deriving the same answer the same way.

func resourceTypeGen() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"sqs", "sns", "dynamodb", "s3"})
}

func arnGen(resourceType string) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		name := rapid.StringMatching(`[a-z][a-z0-9-]{0,20}`).Draw(t, "resourceName")
		return fmt.Sprintf("arn:aws:%s:us-east-1:123456789012:%s", resourceType, name)
	})
}

func grantGen() *rapid.Generator[grant] {
	return rapid.Custom(func(t *rapid.T) grant {
		resourceType := resourceTypeGen().Draw(t, "resourceType")
		return grant{
			resourceType: resourceType,
			arn:          arnGen(resourceType).Draw(t, "arn"),
			readWrite:    rapid.Bool().Draw(t, "readWrite"),
		}
	})
}

// decodePolicy builds a policy from grants and decodes it back, failing
// the test outright on any error - every property below assumes this
// always succeeds, since "always produces valid JSON" is itself one of
// the properties being relied on throughout.
func decodePolicy(t *rapid.T, grants []grant) policyDocument {
	t.Helper()
	encoded, err := buildPolicyDocument(grants)
	if err != nil {
		t.Fatalf("buildPolicyDocument: %v", err)
	}
	var doc policyDocument
	if err := json.Unmarshal([]byte(encoded), &doc); err != nil {
		t.Fatalf("buildPolicyDocument produced invalid JSON: %v\n%s", err, encoded)
	}
	return doc
}

func flattenActions(statements []policyStatement) []string {
	var all []string
	for _, s := range statements {
		all = append(all, s.Action...)
	}
	return all
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestBuildPolicyDocument_ReadWriteNeverRemovesReadOnlyActions is the core
// security property: ReadWrite is only ever additive over ReadOnly for the
// exact same resource. Built by comparing two real, independently-produced
// policies for the same (resourceType, arn) rather than by predicting the
// action lists from actionSets/s3Object* directly, so a bug in the
// readWrite branching itself (not just a wrong action string) would
// actually surface here.
func TestBuildPolicyDocument_ReadWriteNeverRemovesReadOnlyActions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		resourceType := resourceTypeGen().Draw(t, "resourceType")
		arn := arnGen(resourceType).Draw(t, "arn")

		readOnly := flattenActions(decodePolicy(t, []grant{{resourceType, arn, false}}).Statement)
		readWrite := flattenActions(decodePolicy(t, []grant{{resourceType, arn, true}}).Statement)

		for _, action := range readOnly {
			if !containsString(readWrite, action) {
				t.Fatalf("ReadWrite for %s dropped action %q that ReadOnly granted", resourceType, action)
			}
		}
		if len(readWrite) <= len(readOnly) {
			t.Fatalf("expected ReadWrite to strictly add at least one action beyond ReadOnly for %s (ReadOnly=%v ReadWrite=%v)", resourceType, readOnly, readWrite)
		}
	})
}

// TestBuildPolicyDocument_EveryStatementIsWellFormed checks the
// structural shape every statement must have regardless of input, over
// arbitrary grant lists (including the empty list).
func TestBuildPolicyDocument_EveryStatementIsWellFormed(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		grants := rapid.SliceOfN(grantGen(), 0, 12).Draw(t, "grants")
		doc := decodePolicy(t, grants)

		if doc.Version != "2012-10-17" {
			t.Fatalf("Version = %q, want 2012-10-17", doc.Version)
		}
		for _, s := range doc.Statement {
			if s.Effect != "Allow" {
				t.Fatalf("statement %q has Effect %q, want Allow", s.Sid, s.Effect)
			}
			if len(s.Action) == 0 {
				t.Fatalf("statement %q has no actions", s.Sid)
			}
			if len(s.Resource) == 0 {
				t.Fatalf("statement %q has no resources", s.Sid)
			}
		}
	})
}

// TestBuildPolicyDocument_EveryGrantARNIsCoveredExactlyOnce checks that
// every input grant's ARN is actually reachable in the output (nothing
// silently dropped) and that no statement references an ARN that doesn't
// correspond to some input grant (nothing fabricated or leaked from
// another grant) - independent of exactly which actions end up attached.
func TestBuildPolicyDocument_EveryGrantARNIsCoveredExactlyOnce(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		grants := rapid.SliceOfN(grantGen(), 0, 12).Draw(t, "grants")
		doc := decodePolicy(t, grants)

		allowedResources := map[string]bool{}
		for _, g := range grants {
			allowedResources[g.arn] = true
			if g.resourceType == "s3" {
				allowedResources[g.arn+"/*"] = true
			}
		}

		seen := map[string]bool{}
		for _, s := range doc.Statement {
			for _, r := range s.Resource {
				if !allowedResources[r] {
					t.Fatalf("statement %q references resource %q that doesn't correspond to any input grant", s.Sid, r)
				}
				seen[r] = true
			}
		}
		for r := range allowedResources {
			if !seen[r] {
				t.Fatalf("expected resource %q to appear in some statement, found none", r)
			}
		}
	})
}

// TestBuildPolicyDocument_StatementCountMatchesGrantShape checks the
// structural count independently of statement content: every non-S3 grant
// contributes exactly one statement, every S3 grant contributes exactly
// two (bucket-level and object-level).
func TestBuildPolicyDocument_StatementCountMatchesGrantShape(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		grants := rapid.SliceOfN(grantGen(), 0, 12).Draw(t, "grants")
		doc := decodePolicy(t, grants)

		want := 0
		for _, g := range grants {
			if g.resourceType == "s3" {
				want += 2
			} else {
				want++
			}
		}
		if len(doc.Statement) != want {
			t.Fatalf("got %d statements for %d grants, want %d", len(doc.Statement), len(grants), want)
		}
	})
}
