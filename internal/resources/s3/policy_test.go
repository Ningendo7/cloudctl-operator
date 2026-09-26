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

package s3

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestAddPendingDeletionDeny_CreatesPolicyFromScratch(t *testing.T) {
	client := newFakeS3()
	bucket := "default-checkout-service-receipts-abcd1234"
	client.buckets[bucket] = &fakeBucket{}

	if err := addPendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}

	policy := client.buckets[bucket].policy
	if !strings.Contains(policy, pendingDeletionDenySid) {
		t.Fatalf("expected policy to contain our deny Sid, got %s", policy)
	}
	if !strings.Contains(policy, `"Effect":"Deny"`) {
		t.Errorf("expected a Deny effect in the policy, got %s", policy)
	}
	if !strings.Contains(policy, `"s3:PutObject"`) {
		t.Errorf("expected the deny to target s3:PutObject, got %s", policy)
	}
}

func TestAddPendingDeletionDeny_PreservesExistingStatements(t *testing.T) {
	client := newFakeS3()
	bucket := "default-checkout-service-receipts-abcd1234"
	client.buckets[bucket] = &fakeBucket{
		policy: `{"Version":"2012-10-17","Statement":[{"Sid":"cross-account-read","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`,
	}

	if err := addPendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}

	policy := client.buckets[bucket].policy
	if !strings.Contains(policy, "cross-account-read") {
		t.Errorf("expected pre-existing unrelated statement to survive, got %s", policy)
	}
	if !strings.Contains(policy, pendingDeletionDenySid) {
		t.Errorf("expected our deny statement to be added, got %s", policy)
	}
}

func TestAddPendingDeletionDeny_IsIdempotent(t *testing.T) {
	client := newFakeS3()
	bucket := "default-checkout-service-receipts-abcd1234"
	client.buckets[bucket] = &fakeBucket{}

	if err := addPendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("first addPendingDeletionDeny() error = %v", err)
	}
	if err := addPendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("second addPendingDeletionDeny() error = %v", err)
	}

	var doc policyDocument
	if err := json.Unmarshal([]byte(client.buckets[bucket].policy), &doc); err != nil {
		t.Fatalf("failed to parse resulting policy: %v", err)
	}
	count := 0
	for _, s := range doc.Statement {
		if strings.Contains(string(s), pendingDeletionDenySid) {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one deny statement after calling twice, found %d", count)
	}
}

func TestRemovePendingDeletionDeny_DeletesPolicyWhenNothingElseRemains(t *testing.T) {
	client := newFakeS3()
	bucket := "default-checkout-service-receipts-abcd1234"
	client.buckets[bucket] = &fakeBucket{}

	if err := addPendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	if err := removePendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}

	if client.buckets[bucket].policy != "" {
		t.Errorf("expected policy to be deleted entirely, got %s", client.buckets[bucket].policy)
	}
}

func TestRemovePendingDeletionDeny_PreservesOtherStatements(t *testing.T) {
	client := newFakeS3()
	bucket := "default-checkout-service-receipts-abcd1234"
	client.buckets[bucket] = &fakeBucket{
		policy: `{"Version":"2012-10-17","Statement":[{"Sid":"cross-account-read","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`,
	}

	if err := addPendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	if err := removePendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}

	policy := client.buckets[bucket].policy
	if strings.Contains(policy, pendingDeletionDenySid) {
		t.Errorf("expected our deny statement to be gone, got %s", policy)
	}
	if !strings.Contains(policy, "cross-account-read") {
		t.Errorf("expected the pre-existing statement to survive removal, got %s", policy)
	}
}

func TestRemovePendingDeletionDeny_NoOpWhenAbsent(t *testing.T) {
	client := newFakeS3()
	bucket := "default-checkout-service-receipts-abcd1234"
	client.buckets[bucket] = &fakeBucket{}

	if err := removePendingDeletionDeny(context.Background(), client, bucket); err != nil {
		t.Fatalf("removePendingDeletionDeny() on a bucket with no policy should be a no-op, got error: %v", err)
	}
	if client.buckets[bucket].policy != "" {
		t.Errorf("expected policy to remain empty, got %s", client.buckets[bucket].policy)
	}
}
