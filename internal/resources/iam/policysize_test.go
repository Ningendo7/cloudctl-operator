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
	"fmt"
	"strings"
	"testing"
)

// maxedOutSectionGrants builds the grant set for one section's Resources
// and Consumes lists both maxed out at the CRD's own 50-item MaxItems bound
// (every owned resource dedicated-key-encrypted) - not an exotic scenario,
// just one busy team's CR near its own documented schema limit.
func maxedOutSectionGrants(resourceType string) []grant {
	var grants []grant
	const maxPerList = 50
	for i := range maxPerList {
		grants = append(grants, grant{resourceType: resourceType, arn: fmt.Sprintf("arn:aws:%s:us-east-1:123456789012:resource-name-%d", resourceType, i), readWrite: true})
		grants = append(grants, grant{resourceType: "kms", arn: fmt.Sprintf("arn:aws:kms:us-east-1:123456789012:key/%08d-0000-0000-0000-000000000000", i), readWrite: true})
	}
	for i := range maxPerList {
		grants = append(grants, grant{resourceType: resourceType, arn: fmt.Sprintf("arn:aws:%s:us-east-1:123456789012:other-resource-name-%d", resourceType, i), readWrite: i%2 == 0})
	}
	return grants
}

// TestEnsurePermissionsPolicy_RefusesOversizedPolicyWithClearError confirms
// the proactive size check actually fires for a realistic, reachable case:
// a single section maxed out at its own CRD-declared limit renders a
// 32,758-character document (confirmed via this exact grant set), well
// over PutRolePolicy's 10,240-character ceiling - this used to only
// surface via AWS's own generic LimitExceededException after a doomed
// round-trip.
func TestEnsurePermissionsPolicy_RefusesOversizedPolicyWithClearError(t *testing.T) {
	grants := maxedOutSectionGrants("sqs")

	policy, err := ensurePermissionsPolicy(grants)
	if err == nil {
		t.Fatal("expected an error for an oversized policy, got nil")
	}
	if policy != "" {
		t.Errorf("expected an empty policy alongside the error, got %q", policy)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("%d", rolePolicySizeLimit)) {
		t.Errorf("error = %q, want it to mention the %d-character limit", err.Error(), rolePolicySizeLimit)
	}
}

// TestEnsurePermissionsPolicy_AllowsOrdinaryPolicyUnderLimit guards against
// the size check itself being miscalibrated and refusing a normal CR.
func TestEnsurePermissionsPolicy_AllowsOrdinaryPolicyUnderLimit(t *testing.T) {
	grants := []grant{
		{resourceType: "sqs", arn: "arn:aws:sqs:us-east-1:123456789012:orders", readWrite: true},
		{resourceType: "s3", arn: "arn:aws:s3:::receipts", readWrite: false},
	}

	policy, err := ensurePermissionsPolicy(grants)
	if err != nil {
		t.Fatalf("ensurePermissionsPolicy() error = %v, want nil for an ordinary small CR", err)
	}
	if policy == "" {
		t.Error("expected a non-empty policy document")
	}
}
