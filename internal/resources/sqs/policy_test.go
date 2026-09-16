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
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func TestAddPendingDeletionDeny_CreatesPolicyFromScratch(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	q := client.queues[queueName]

	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}

	if !strings.Contains(q.policy, pendingDeletionDenySid) {
		t.Fatalf("expected policy to contain our deny Sid, got %s", q.policy)
	}
	if !strings.Contains(q.policy, "\"Effect\":\"Deny\"") {
		t.Errorf("expected a Deny effect in the policy, got %s", q.policy)
	}
}

func TestAddPendingDeletionDeny_PreservesExistingStatements(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	q := client.queues[queueName]
	q.policy = `{"Version":"2012-10-17","Statement":[{"Sid":"cross-account-read","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},"Action":"sqs:ReceiveMessage","Resource":"` + q.arn + `"}]}`

	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}

	if !strings.Contains(q.policy, "cross-account-read") {
		t.Errorf("expected pre-existing unrelated statement to survive, got %s", q.policy)
	}
	if !strings.Contains(q.policy, pendingDeletionDenySid) {
		t.Errorf("expected our deny statement to be added, got %s", q.policy)
	}
}

func TestAddPendingDeletionDeny_IsIdempotent(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	q := client.queues[queueName]

	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("first addPendingDeletionDeny() error = %v", err)
	}
	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("second addPendingDeletionDeny() error = %v", err)
	}

	var doc policyDocument
	if err := json.Unmarshal([]byte(q.policy), &doc); err != nil {
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

func TestRemovePendingDeletionDeny_ClearsPolicyWhenNothingElseRemains(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	q := client.queues[queueName]

	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	if err := removePendingDeletionDeny(context.Background(), client, q.url); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}

	if q.policy != "" {
		t.Errorf("expected policy to be cleared entirely, got %s", q.policy)
	}
}

func TestRemovePendingDeletionDeny_PreservesOtherStatements(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	q := client.queues[queueName]
	q.policy = `{"Version":"2012-10-17","Statement":[{"Sid":"cross-account-read","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},"Action":"sqs:ReceiveMessage","Resource":"` + q.arn + `"}]}`

	if err := addPendingDeletionDeny(context.Background(), client, q.url, q.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	if err := removePendingDeletionDeny(context.Background(), client, q.url); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}

	if strings.Contains(q.policy, pendingDeletionDenySid) {
		t.Errorf("expected our deny statement to be gone, got %s", q.policy)
	}
	if !strings.Contains(q.policy, "cross-account-read") {
		t.Errorf("expected the pre-existing statement to survive removal, got %s", q.policy)
	}
}

func TestRemovePendingDeletionDeny_NoOpWhenAbsent(t *testing.T) {
	client := newFakeSQS()
	queueName := cloudctlaws.ResourceName("default", "checkout-service", "orders")
	_, _ = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: &queueName})
	q := client.queues[queueName]

	if err := removePendingDeletionDeny(context.Background(), client, q.url); err != nil {
		t.Fatalf("removePendingDeletionDeny() on a queue with no policy should be a no-op, got error: %v", err)
	}
	if q.policy != "" {
		t.Errorf("expected policy to remain empty, got %s", q.policy)
	}
}
