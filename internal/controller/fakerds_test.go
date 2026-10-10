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

package controller

import (
	"context"
	"maps"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// fakeRDSInstance and fakeRDSClient are a minimal in-memory stand-in for
// the real RDS client, implementing cloudctlaws.RDSClient, so the
// controller's own wiring (section dispatch, status conditions, the
// RDSSubnetGroupGrant watch) can be tested without hitting real AWS.
// Detailed RDS behavior - adoption, trust window, pending-deletion,
// snapshot retirement - is already covered exhaustively by
// internal/resources/rds's own unit tests; this fake only needs to be
// complete enough to exercise the controller's plumbing around it, so
// (like fakeSQSClient) it resolves every instance to "available"
// immediately rather than modeling AWS's real async creation.
type fakeRDSInstance struct {
	arn, engine, dbSubnetGroupName string
	tags                           map[string]string
	status                         string
	endpointAddress                string
	endpointPort                   int32
	instanceClass                  string
	multiAZ                        bool
	backupRetentionPeriod          int32
}

type fakeRDSSnapshot struct {
	id, instanceID, status string
}

type fakeRDSClient struct {
	instances map[string]*fakeRDSInstance // keyed by DBInstanceIdentifier
	snapshots map[string]*fakeRDSSnapshot // keyed by DBSnapshotIdentifier

	createDBInstanceErr error

	// describeDBInstancesCalls counts every call, keyed by instance ID -
	// lets a test prove the describe-cache dedup actually collapses
	// repeated lookups within one reconcile pass instead of just trusting
	// it does.
	describeDBInstancesCalls map[string]int
}

func newFakeRDSClient() *fakeRDSClient {
	return &fakeRDSClient{
		instances:                map[string]*fakeRDSInstance{},
		snapshots:                map[string]*fakeRDSSnapshot{},
		describeDBInstancesCalls: map[string]int{},
	}
}

func (f *fakeRDSClient) findByARN(arn string) *fakeRDSInstance {
	for _, i := range f.instances {
		if i.arn == arn {
			return i
		}
	}
	return nil
}

func (f *fakeRDSClient) CreateDBInstance(_ context.Context, in *rds.CreateDBInstanceInput, _ ...func(*rds.Options)) (*rds.CreateDBInstanceOutput, error) {
	if f.createDBInstanceErr != nil {
		return nil, f.createDBInstanceErr
	}
	id := *in.DBInstanceIdentifier
	arn := "arn:aws:rds:us-east-1:123456789012:db:" + id
	i := &fakeRDSInstance{
		arn:                   arn,
		engine:                aws.ToString(in.Engine),
		dbSubnetGroupName:     aws.ToString(in.DBSubnetGroupName),
		tags:                  tagsToFakeMap(in.Tags),
		status:                "available",
		endpointAddress:       id + ".fake.us-east-1.rds.amazonaws.com",
		endpointPort:          5432,
		instanceClass:         aws.ToString(in.DBInstanceClass),
		multiAZ:               aws.ToBool(in.MultiAZ),
		backupRetentionPeriod: aws.ToInt32(in.BackupRetentionPeriod),
	}
	f.instances[id] = i
	return &rds.CreateDBInstanceOutput{
		DBInstance: &types.DBInstance{DBInstanceArn: &arn, DBInstanceIdentifier: &id, DBInstanceStatus: aws.String("available")},
	}, nil
}

func (f *fakeRDSClient) ModifyDBInstance(_ context.Context, in *rds.ModifyDBInstanceInput, _ ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error) {
	i, ok := f.instances[*in.DBInstanceIdentifier]
	if !ok {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	if in.DBInstanceClass != nil {
		i.instanceClass = *in.DBInstanceClass
	}
	if in.MultiAZ != nil {
		i.multiAZ = *in.MultiAZ
	}
	if in.BackupRetentionPeriod != nil {
		i.backupRetentionPeriod = *in.BackupRetentionPeriod
	}
	id := *in.DBInstanceIdentifier
	return &rds.ModifyDBInstanceOutput{DBInstance: &types.DBInstance{DBInstanceIdentifier: &id}}, nil
}

func (f *fakeRDSClient) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput, _ ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	f.describeDBInstancesCalls[*in.DBInstanceIdentifier]++
	i, ok := f.instances[*in.DBInstanceIdentifier]
	if !ok {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	id := *in.DBInstanceIdentifier
	return &rds.DescribeDBInstancesOutput{DBInstances: []types.DBInstance{{
		DBInstanceArn:         &i.arn,
		DBInstanceIdentifier:  &id,
		DBInstanceStatus:      &i.status,
		Engine:                &i.engine,
		DBSubnetGroup:         &types.DBSubnetGroup{DBSubnetGroupName: &i.dbSubnetGroupName},
		DBInstanceClass:       &i.instanceClass,
		MultiAZ:               &i.multiAZ,
		BackupRetentionPeriod: &i.backupRetentionPeriod,
		Endpoint:              &types.Endpoint{Address: &i.endpointAddress, Port: &i.endpointPort},
	}}}, nil
}

func (f *fakeRDSClient) DescribeDBSubnetGroups(_ context.Context, in *rds.DescribeDBSubnetGroupsInput, _ ...func(*rds.Options)) (*rds.DescribeDBSubnetGroupsOutput, error) {
	vpcID := "vpc-" + *in.DBSubnetGroupName
	return &rds.DescribeDBSubnetGroupsOutput{
		DBSubnetGroups: []types.DBSubnetGroup{{DBSubnetGroupName: in.DBSubnetGroupName, VpcId: &vpcID}},
	}, nil
}

func (f *fakeRDSClient) ListTagsForResource(_ context.Context, in *rds.ListTagsForResourceInput, _ ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error) {
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	return &rds.ListTagsForResourceOutput{TagList: fakeMapToTags(i.tags)}, nil
}

func (f *fakeRDSClient) AddTagsToResource(_ context.Context, in *rds.AddTagsToResourceInput, _ ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error) {
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	if i.tags == nil {
		i.tags = map[string]string{}
	}
	maps.Copy(i.tags, tagsToFakeMap(in.Tags))
	return &rds.AddTagsToResourceOutput{}, nil
}

func (f *fakeRDSClient) RemoveTagsFromResource(_ context.Context, in *rds.RemoveTagsFromResourceInput, _ ...func(*rds.Options)) (*rds.RemoveTagsFromResourceOutput, error) {
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	for _, k := range in.TagKeys {
		delete(i.tags, k)
	}
	return &rds.RemoveTagsFromResourceOutput{}, nil
}

func (f *fakeRDSClient) CreateDBSnapshot(_ context.Context, in *rds.CreateDBSnapshotInput, _ ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error) {
	id := *in.DBSnapshotIdentifier
	if _, exists := f.snapshots[id]; exists {
		return nil, &types.DBSnapshotAlreadyExistsFault{}
	}
	f.snapshots[id] = &fakeRDSSnapshot{id: id, instanceID: *in.DBInstanceIdentifier, status: "available"}
	return &rds.CreateDBSnapshotOutput{DBSnapshot: &types.DBSnapshot{DBSnapshotIdentifier: &id}}, nil
}

func (f *fakeRDSClient) DescribeDBSnapshots(_ context.Context, in *rds.DescribeDBSnapshotsInput, _ ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error) {
	s, ok := f.snapshots[*in.DBSnapshotIdentifier]
	if !ok {
		return &rds.DescribeDBSnapshotsOutput{}, nil
	}
	return &rds.DescribeDBSnapshotsOutput{DBSnapshots: []types.DBSnapshot{{DBSnapshotIdentifier: &s.id, Status: &s.status}}}, nil
}

func (f *fakeRDSClient) DeleteDBSnapshot(_ context.Context, in *rds.DeleteDBSnapshotInput, _ ...func(*rds.Options)) (*rds.DeleteDBSnapshotOutput, error) {
	if _, ok := f.snapshots[*in.DBSnapshotIdentifier]; !ok {
		return nil, &types.DBSnapshotNotFoundFault{}
	}
	delete(f.snapshots, *in.DBSnapshotIdentifier)
	return &rds.DeleteDBSnapshotOutput{}, nil
}

func tagsToFakeMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func fakeMapToTags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return tags
}
