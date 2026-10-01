//go:build integration

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

// Integration tests run against a real AWS SDK client pointed at a
// LocalStack container instead of this package's own hand-written fakes.
// The fakes only ever encode our own beliefs about how DynamoDB's API
// behaves; these tests catch the case where that belief is simply wrong.
// Excluded from `go test ./...` by the "integration" build tag — see
// docs/testing.md for how to run them (LOCALSTACK_ENDPOINT, or
// `make test-integration`).
package dynamodb

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// newIntegrationClient builds a real DynamoDB client pointed at LocalStack.
// Deliberately never uses config.LoadDefaultConfig or picks up the
// environment's own AWS credentials/profile — an integration test must be
// structurally incapable of ever reaching real AWS by accident.
func newIntegrationClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	})
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

// uniqueSuffix keeps each test's table name distinct so parallel/repeated
// runs against the same long-lived LocalStack container never collide.
func uniqueSuffix(t *testing.T) string {
	return "test-" + t.Name()[len("TestIntegration_"):]
}

func deleteTableIfExists(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &tableName})
}

// waitForTableActive polls DescribeTable until the table reports ACTIVE or
// timeout elapses. LocalStack usually finishes near-instantly, unlike real
// AWS's genuinely asynchronous creation, but ensureTable's own CREATING/
// UPDATING handling still applies here and a reconcile can still observe it
// mid-transition.
func waitForTableActive(t *testing.T, client *dynamodb.Client, tableName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName})
		if err == nil && out.Table.TableStatus == types.TableStatusActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("table %q never became ACTIVE within %s", tableName, timeout)
}

// ensureUntilActive drives Ensure through the CREATING retry loop
// ensureTable's own async handling requires, rather than assuming a single
// call suffices the way SQS/SNS/S3's synchronous creation does.
func ensureUntilActive(t *testing.T, client *dynamodb.Client, namespace, crName, crUID string, spec *depsv1alpha1.DynamoDBSpec, ledger []depsv1alpha1.ManagedResource, tableName string) []depsv1alpha1.ManagedResource {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for {
		updated, err := Ensure(ctx, client, nil, nil, namespace, crName, crUID, spec, ledger, nil, nil)
		ledger = updated
		if err == nil {
			return ledger
		}
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable || time.Now().After(deadline) {
			t.Fatalf("Ensure() error = %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestIntegration_Ensure_CreatesRealTableWithKeySchemaAndTags(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger := ensureUntilActive(t, client, namespace, crName, "uid-1", spec, nil, tableName)
	entry := findEntry(ledger, "sessions")
	if entry == nil {
		t.Fatal("expected a ledger entry for sessions")
	}

	describeOut, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		t.Fatalf("real DescribeTable() error = %v — table wasn't actually created against LocalStack", err)
	}
	if pk, _ := tableKeySchema(describeOut.Table.KeySchema); pk != "id" {
		t.Errorf("real partition key = %q, want %q", pk, "id")
	}
	if *describeOut.Table.TableArn != entry.ARN {
		t.Errorf("ledger ARN %q doesn't match the real table's ARN %q", entry.ARN, *describeOut.Table.TableArn)
	}

	tags, err := listAllTags(context.Background(), client, entry.ARN)
	if err != nil {
		t.Fatalf("real ListTagsOfResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, "uid-1") {
		t.Errorf("real table tags don't satisfy IsOwnedBy: %+v", tags)
	}
}

func TestIntegration_Ensure_IsIdempotentAgainstRealAWS(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger := ensureUntilActive(t, client, namespace, crName, "uid-1", spec, nil, tableName)
	ledger, err := Ensure(context.Background(), client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected exactly one ledger entry after two reconciles against real AWS, got %d", len(ledger))
	}
}

func TestIntegration_Ensure_AdoptsRealUntaggedTable(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()
	namespace, crName := "integration", uniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	// Create the table directly via the real SDK, with no ownership tags at
	// all — simulating a resource that already existed before this CR ever
	// reconciled.
	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            &tableName,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("setting up pre-existing real table: %v", err)
	}
	waitForTableActive(t, client, tableName, 30*time.Second)

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	ledger := ensureUntilActive(t, client, namespace, crName, "uid-1", spec, nil, tableName)
	entry := findEntry(ledger, "sessions")
	if entry == nil {
		t.Fatal("expected a ledger entry for sessions")
	}

	tags, err := listAllTags(ctx, client, entry.ARN)
	if err != nil {
		t.Fatalf("real ListTagsOfResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, "uid-1") {
		t.Errorf("expected adopt:true to tag the pre-existing real table as owned, got tags %+v", tags)
	}
}

func TestIntegration_Ensure_CorrectsBillingModeDriftOnRealTable(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger := ensureUntilActive(t, client, namespace, crName, "uid-1", spec, nil, tableName)

	spec.Resources[0].Overrides = &depsv1alpha1.DynamoDBOverrides{BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned}
	ensureUntilActive(t, client, namespace, crName, "uid-1", spec, ledger, tableName)

	describeOut, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		t.Fatalf("real DescribeTable() error = %v", err)
	}
	if describeOut.Table.BillingModeSummary == nil || describeOut.Table.BillingModeSummary.BillingMode != types.BillingModeProvisioned {
		t.Errorf("real billing mode after drift correction = %+v, want Provisioned", describeOut.Table.BillingModeSummary)
	}
}

func TestIntegration_Cleanup_DeletesRealTableImmediatelyWhenForced(t *testing.T) {
	client := newIntegrationClient(t)
	namespace, crName := "integration", uniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger := ensureUntilActive(t, client, namespace, crName, "uid-1", spec, nil, tableName)

	ledger, _, err := Cleanup(context.Background(), client, namespace, crName, "uid-1", spec, ledger, true, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ledger) != 0 {
		t.Errorf("expected the ledger entry to be removed, got %+v", ledger)
	}

	if _, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName}); err == nil {
		t.Error("expected the real table to be gone after Cleanup, but DescribeTable succeeded")
	}
}
