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

package rds

import (
	"context"
	"maps"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
)

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (throttling, permission-denied, ...) into fakeRDS
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

// fakeInstance and fakeRDS are a minimal in-memory stand-in for the real
// RDS client, implementing just the rdsAPI methods this package calls, so
// Ensure can be tested without hitting real AWS. status defaults to
// "available" on create - tests exercising the still-transitioning or
// failed paths set it directly afterward, since the real API doesn't let
// a caller dictate status either.
type fakeInstance struct {
	arn                   string
	tags                  map[string]string
	status                string
	engine, engineVersion string
	instanceClass         string
	dbSubnetGroupName     string
	multiAZ               bool
	backupRetentionPeriod int32
	kmsKeyARN             string
}

type fakeRDS struct {
	instances map[string]*fakeInstance // keyed by DBInstanceIdentifier

	createDBInstanceErr    error
	describeDBInstancesErr error
	listTagsForResourceErr error
	addTagsToResourceErr   error
}

func newFakeRDS() *fakeRDS {
	return &fakeRDS{instances: map[string]*fakeInstance{}}
}

func (f *fakeRDS) findByARN(arn string) *fakeInstance {
	for _, i := range f.instances {
		if i.arn == arn {
			return i
		}
	}
	return nil
}

func (f *fakeRDS) CreateDBInstance(_ context.Context, in *rds.CreateDBInstanceInput, _ ...func(*rds.Options)) (*rds.CreateDBInstanceOutput, error) {
	if f.createDBInstanceErr != nil {
		return nil, f.createDBInstanceErr
	}
	id := *in.DBInstanceIdentifier
	arn := "arn:aws:rds:us-east-1:123456789012:db:" + id
	var kmsKeyARN string
	if in.KmsKeyId != nil {
		kmsKeyARN = *in.KmsKeyId
	}
	var backupRetention int32
	if in.BackupRetentionPeriod != nil {
		backupRetention = *in.BackupRetentionPeriod
	}
	var multiAZ bool
	if in.MultiAZ != nil {
		multiAZ = *in.MultiAZ
	}
	f.instances[id] = &fakeInstance{
		arn:                   arn,
		tags:                  tagsToMap(in.Tags),
		status:                "available",
		engine:                *in.Engine,
		engineVersion:         *in.EngineVersion,
		instanceClass:         *in.DBInstanceClass,
		dbSubnetGroupName:     *in.DBSubnetGroupName,
		multiAZ:               multiAZ,
		backupRetentionPeriod: backupRetention,
		kmsKeyARN:             kmsKeyARN,
	}
	return &rds.CreateDBInstanceOutput{
		DBInstance: &types.DBInstance{
			DBInstanceArn:        &arn,
			DBInstanceIdentifier: &id,
			DBInstanceStatus:     strPtr("creating"),
		},
	}, nil
}

func (f *fakeRDS) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput, _ ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	if f.describeDBInstancesErr != nil {
		return nil, f.describeDBInstancesErr
	}
	i, ok := f.instances[*in.DBInstanceIdentifier]
	if !ok {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	id := *in.DBInstanceIdentifier
	var kmsKeyID *string
	if i.kmsKeyARN != "" {
		kmsKeyID = &i.kmsKeyARN
	}
	return &rds.DescribeDBInstancesOutput{
		DBInstances: []types.DBInstance{
			{
				DBInstanceArn:         &i.arn,
				DBInstanceIdentifier:  &id,
				DBInstanceStatus:      &i.status,
				Engine:                &i.engine,
				EngineVersion:         &i.engineVersion,
				DBInstanceClass:       &i.instanceClass,
				DBSubnetGroup:         &types.DBSubnetGroup{DBSubnetGroupName: &i.dbSubnetGroupName},
				MultiAZ:               &i.multiAZ,
				BackupRetentionPeriod: &i.backupRetentionPeriod,
				KmsKeyId:              kmsKeyID,
				StorageEncrypted:      boolPtr(i.kmsKeyARN != ""),
			},
		},
	}, nil
}

func (f *fakeRDS) ListTagsForResource(_ context.Context, in *rds.ListTagsForResourceInput, _ ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error) {
	if f.listTagsForResourceErr != nil {
		return nil, f.listTagsForResourceErr
	}
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	return &rds.ListTagsForResourceOutput{TagList: mapToTags(i.tags)}, nil
}

func (f *fakeRDS) AddTagsToResource(_ context.Context, in *rds.AddTagsToResourceInput, _ ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error) {
	if f.addTagsToResourceErr != nil {
		return nil, f.addTagsToResourceErr
	}
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	if i.tags == nil {
		i.tags = map[string]string{}
	}
	maps.Copy(i.tags, tagsToMap(in.Tags))
	return &rds.AddTagsToResourceOutput{}, nil
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
