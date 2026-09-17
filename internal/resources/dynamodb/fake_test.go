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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (throttling, permission-denied, ...) into fakeDynamoDB
// calls, so error-classification behavior is actually exercisable in
// tests, not just in internal/aws's own unit tests.
type fakeAWSError struct {
	code  string
	fault smithy.ErrorFault
}

func (e *fakeAWSError) Error() string                 { return e.code }
func (e *fakeAWSError) ErrorCode() string             { return e.code }
func (e *fakeAWSError) ErrorMessage() string          { return e.code }
func (e *fakeAWSError) ErrorFault() smithy.ErrorFault { return e.fault }

// fakeTable and fakeDynamoDB are a minimal in-memory stand-in for the real
// DynamoDB client, implementing just the dynamodbAPI methods this package
// calls, so Ensure/Cleanup can be tested without hitting real AWS.
type fakeTable struct {
	arn           string
	tags          map[string]string
	status        types.TableStatus
	billingMode   types.BillingMode
	pitrEnabled   bool
	retentionDays int32
	// partitionKey/sortKey back DescribeTable's KeySchema response, so
	// adopt-path key-schema-mismatch checks are actually exercisable
	// against a fake table pre-seeded with a different schema than spec
	// declares.
	partitionKey string
	sortKey      string
	// itemCount drives Scan's Count response — 0 means empty, anything
	// else means the count-limited Scan finds at least one item.
	itemCount int
}

type fakeDynamoDB struct {
	tables map[string]*fakeTable // keyed by table name

	createTableErr             error
	describeTableErr           error
	updateTableErr             error
	updateContinuousBackupsErr error
}

func newFakeDynamoDB() *fakeDynamoDB {
	return &fakeDynamoDB{tables: map[string]*fakeTable{}}
}

func (f *fakeDynamoDB) CreateTable(_ context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	if f.createTableErr != nil {
		return nil, f.createTableErr
	}
	name := *in.TableName
	arn := "arn:aws:dynamodb:us-east-1:123456789012:table/" + name
	partitionKey, sortKey := tableKeySchema(in.KeySchema)
	f.tables[name] = &fakeTable{
		arn:          arn,
		tags:         tagsToMap(in.Tags),
		status:       types.TableStatusActive,
		billingMode:  in.BillingMode,
		partitionKey: partitionKey,
		sortKey:      sortKey,
	}
	return &dynamodb.CreateTableOutput{
		TableDescription: &types.TableDescription{
			TableArn:    &arn,
			TableName:   &name,
			TableStatus: types.TableStatusActive,
		},
	}, nil
}

func (f *fakeDynamoDB) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if f.describeTableErr != nil {
		return nil, f.describeTableErr
	}
	t, ok := f.tables[*in.TableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{}
	}
	name := *in.TableName
	var billingSummary *types.BillingModeSummary
	if t.billingMode != "" {
		billingSummary = &types.BillingModeSummary{BillingMode: t.billingMode}
	}
	var keySchema []types.KeySchemaElement
	if t.partitionKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &t.partitionKey, KeyType: types.KeyTypeHash})
	}
	if t.sortKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &t.sortKey, KeyType: types.KeyTypeRange})
	}
	return &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{
			TableArn:           &t.arn,
			TableName:          &name,
			TableStatus:        t.status,
			BillingModeSummary: billingSummary,
			KeySchema:          keySchema,
		},
	}, nil
}

func (f *fakeDynamoDB) UpdateTable(_ context.Context, in *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	if f.updateTableErr != nil {
		return nil, f.updateTableErr
	}
	t, ok := f.tables[*in.TableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{}
	}
	if in.BillingMode != "" {
		t.billingMode = in.BillingMode
	}
	return &dynamodb.UpdateTableOutput{}, nil
}

func (f *fakeDynamoDB) DeleteTable(_ context.Context, in *dynamodb.DeleteTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	if _, ok := f.tables[*in.TableName]; !ok {
		return nil, &types.ResourceNotFoundException{}
	}
	delete(f.tables, *in.TableName)
	return &dynamodb.DeleteTableOutput{}, nil
}

func (f *fakeDynamoDB) ListTagsOfResource(_ context.Context, in *dynamodb.ListTagsOfResourceInput, _ ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	t := f.findByARN(*in.ResourceArn)
	if t == nil {
		return nil, &types.ResourceNotFoundException{}
	}
	return &dynamodb.ListTagsOfResourceOutput{Tags: mapToTags(t.tags)}, nil
}

func (f *fakeDynamoDB) TagResource(_ context.Context, in *dynamodb.TagResourceInput, _ ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error) {
	t := f.findByARN(*in.ResourceArn)
	if t == nil {
		return nil, &types.ResourceNotFoundException{}
	}
	if t.tags == nil {
		t.tags = map[string]string{}
	}
	for k, v := range tagsToMap(in.Tags) {
		t.tags[k] = v
	}
	return &dynamodb.TagResourceOutput{}, nil
}

func (f *fakeDynamoDB) UntagResource(_ context.Context, in *dynamodb.UntagResourceInput, _ ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error) {
	t := f.findByARN(*in.ResourceArn)
	if t == nil {
		return nil, &types.ResourceNotFoundException{}
	}
	for _, k := range in.TagKeys {
		delete(t.tags, k)
	}
	return &dynamodb.UntagResourceOutput{}, nil
}

func (f *fakeDynamoDB) UpdateContinuousBackups(_ context.Context, in *dynamodb.UpdateContinuousBackupsInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	if f.updateContinuousBackupsErr != nil {
		return nil, f.updateContinuousBackupsErr
	}
	t, ok := f.tables[*in.TableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{}
	}
	if in.PointInTimeRecoverySpecification != nil {
		t.pitrEnabled = in.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled != nil && *in.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled
		if in.PointInTimeRecoverySpecification.RecoveryPeriodInDays != nil {
			t.retentionDays = *in.PointInTimeRecoverySpecification.RecoveryPeriodInDays
		}
	}
	return &dynamodb.UpdateContinuousBackupsOutput{}, nil
}

func (f *fakeDynamoDB) DescribeContinuousBackups(_ context.Context, in *dynamodb.DescribeContinuousBackupsInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	t, ok := f.tables[*in.TableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{}
	}
	status := types.PointInTimeRecoveryStatusDisabled
	if t.pitrEnabled {
		status = types.PointInTimeRecoveryStatusEnabled
	}
	var retentionDays *int32
	if t.retentionDays != 0 {
		retentionDays = &t.retentionDays
	}
	return &dynamodb.DescribeContinuousBackupsOutput{
		ContinuousBackupsDescription: &types.ContinuousBackupsDescription{
			PointInTimeRecoveryDescription: &types.PointInTimeRecoveryDescription{
				PointInTimeRecoveryStatus: status,
				RecoveryPeriodInDays:      retentionDays,
			},
		},
	}, nil
}

func (f *fakeDynamoDB) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	t, ok := f.tables[*in.TableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{}
	}
	if t.itemCount == 0 {
		return &dynamodb.ScanOutput{Count: 0}, nil
	}
	return &dynamodb.ScanOutput{Count: 1}, nil
}

func (f *fakeDynamoDB) findByARN(arn string) *fakeTable {
	for _, t := range f.tables {
		if t.arn == arn {
			return t
		}
	}
	return nil
}
