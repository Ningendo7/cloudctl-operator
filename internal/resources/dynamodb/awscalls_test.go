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

package dynamodb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// awsCalls counts the calls Ensure makes, by operation name.
type awsCalls map[string]int

// writes are the calls that change something in AWS: everything but reads.
func (c awsCalls) writes() map[string]int {
	out := map[string]int{}
	for op, n := range c {
		if !strings.HasPrefix(op, "Get") && !strings.HasPrefix(op, "List") &&
			!strings.HasPrefix(op, "Describe") && !strings.HasPrefix(op, "Head") {
			out[op] = n
		}
	}
	return out
}

// countingDynamoDBAPI holds the client in a named field rather than embedding it: a method
// added to dynamodbAPI then stops this file compiling until it is counted too.
type countingDynamoDBAPI struct {
	client dynamodbAPI
	calls  awsCalls
}

func (w countingDynamoDBAPI) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	w.calls["DescribeTable"]++
	return w.client.DescribeTable(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	w.calls["CreateTable"]++
	return w.client.CreateTable(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) DeleteTable(ctx context.Context, in *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	w.calls["DeleteTable"]++
	return w.client.DeleteTable(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) UpdateTable(ctx context.Context, in *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	w.calls["UpdateTable"]++
	return w.client.UpdateTable(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) GetResourcePolicy(ctx context.Context, in *dynamodb.GetResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetResourcePolicyOutput, error) {
	w.calls["GetResourcePolicy"]++
	return w.client.GetResourcePolicy(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) PutResourcePolicy(ctx context.Context, in *dynamodb.PutResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutResourcePolicyOutput, error) {
	w.calls["PutResourcePolicy"]++
	return w.client.PutResourcePolicy(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) DeleteResourcePolicy(ctx context.Context, in *dynamodb.DeleteResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteResourcePolicyOutput, error) {
	w.calls["DeleteResourcePolicy"]++
	return w.client.DeleteResourcePolicy(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) ListTagsOfResource(ctx context.Context, in *dynamodb.ListTagsOfResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	w.calls["ListTagsOfResource"]++
	return w.client.ListTagsOfResource(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) TagResource(ctx context.Context, in *dynamodb.TagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error) {
	w.calls["TagResource"]++
	return w.client.TagResource(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) UntagResource(ctx context.Context, in *dynamodb.UntagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error) {
	w.calls["UntagResource"]++
	return w.client.UntagResource(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) UpdateContinuousBackups(ctx context.Context, in *dynamodb.UpdateContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	w.calls["UpdateContinuousBackups"]++
	return w.client.UpdateContinuousBackups(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) DescribeContinuousBackups(ctx context.Context, in *dynamodb.DescribeContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	w.calls["DescribeContinuousBackups"]++
	return w.client.DescribeContinuousBackups(ctx, in, optFns...)
}

func (w countingDynamoDBAPI) Scan(ctx context.Context, in *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	w.calls["Scan"]++
	return w.client.Scan(ctx, in, optFns...)
}

// A pass over a table with nothing to change must change nothing in AWS,
// inside the trust window and at the recheck after it.
func TestEnsure_AnIdleTableIsNotWrittenTo(t *testing.T) {
	calls := awsCalls{}
	client := countingDynamoDBAPI{client: newFakeDynamoDB(), calls: calls}
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{
		Name:         "sessions",
		PartitionKey: "id",
		Backup:       &depsv1alpha1.DynamoDBBackupSpec{Enabled: true},
	}}}
	ensure := func(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
		t.Helper()
		clear(calls)
		updated, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
		return updated
	}
	ledger := ensure(ensure(nil))

	ledger = ensure(ledger)
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("a pass inside the trust window wrote to DynamoDB: %v", writes)
	}

	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	for i := range ledger {
		ledger[i].LastVerifiedAt = &stale
	}
	ensure(ledger)
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("the recheck after the trust window wrote to DynamoDB: %v", writes)
	}
	if len(calls) == 0 {
		t.Error("the recheck made no calls at all, so it checked nothing")
	}
	t.Logf("calls at the recheck: %v", calls)
}
