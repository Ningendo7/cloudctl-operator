//go:build live

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

// Package policycheck validates the policies the live tests produce with IAM
// Access Analyzer. It is separate from livetest so that packages livetest
// itself imports can use it.
package policycheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Policy types ValidatePolicy accepts.
const (
	IdentityPolicy = "IDENTITY_POLICY"
	ResourcePolicy = "RESOURCE_POLICY"
)

// Resource types that make ValidatePolicy check a resource policy against the
// service's own rules. Services without one (SQS, SNS, KMS) are validated as
// a plain resource policy.
const (
	S3BucketPolicy  = "AWS::S3::Bucket"
	DynamoDBTable   = "AWS::DynamoDB::Table"
	RoleTrustPolicy = "AWS::IAM::AssumeRolePolicyDocument"
	// NoResourceType is passed for a resource policy of a service
	// ValidatePolicy has no resource type for.
	NoResourceType = ""
)

type policyFinding struct {
	FindingType    string `json:"findingType"`
	IssueCode      string `json:"issueCode"`
	FindingDetails string `json:"findingDetails"`
}

// ValidatePolicy runs document through IAM Access Analyzer's policy checks,
// which are free and need no resource, and fails the test on any ERROR: an
// action name AWS does not know, a condition key that does not exist, a
// principal in the wrong shape. IAM and the services themselves accept many of
// those silently, so a policy that was written without complaint can still do
// nothing. Warnings and suggestions are logged. The test is skipped when the
// caller may not call access-analyzer:ValidatePolicy.
//
// The request is signed here rather than sent through the Access Analyzer SDK
// module, which the rest of the code has no use for.
func ValidatePolicy(t *testing.T, cfg aws.Config, policyType, resourceType, document string) {
	t.Helper()
	ctx := context.Background()

	payload := map[string]string{"policyDocument": document, "policyType": policyType}
	if resourceType != NoResourceType {
		payload["validatePolicyResourceType"] = resourceType
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encoding the ValidatePolicy request: %v", err)
	}

	endpoint := fmt.Sprintf("https://access-analyzer.%s.amazonaws.com/policy/validation?maxResults=100", cfg.Region)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building the ValidatePolicy request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		t.Fatalf("resolving credentials to validate a policy: %v", err)
	}
	sum := sha256.Sum256(body)
	if err := v4.NewSigner().SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), "access-analyzer", cfg.Region, time.Now()); err != nil {
		t.Fatalf("signing the ValidatePolicy request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("calling ValidatePolicy: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the ValidatePolicy response: %v", err)
	}
	switch {
	case resp.StatusCode == http.StatusForbidden:
		t.Skipf("not allowed to call access-analyzer:ValidatePolicy, so the policy was not validated: %s", raw)
	case resp.StatusCode != http.StatusOK:
		t.Fatalf("ValidatePolicy returned %s: %s", resp.Status, raw)
	}

	var out struct {
		Findings  []policyFinding `json:"findings"`
		NextToken string          `json:"nextToken"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decoding the ValidatePolicy response %q: %v", raw, err)
	}
	for _, f := range out.Findings {
		if f.FindingType == "ERROR" {
			t.Errorf("Access Analyzer rejects the policy: %s: %s\n%s", f.IssueCode, f.FindingDetails, document)
		} else {
			t.Logf("Access Analyzer %s %s: %s", f.FindingType, f.IssueCode, f.FindingDetails)
		}
	}
	if out.NextToken != "" {
		t.Logf("Access Analyzer returned more findings than the first page; only the first %d were checked", len(out.Findings))
	}
}
