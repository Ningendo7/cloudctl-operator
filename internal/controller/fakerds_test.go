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
	"encoding/json"
	"maps"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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

// mu guards the maps: the reconciler goroutine mutates them while a test
// goroutine may be reading.
type fakeRDSClient struct {
	mu sync.Mutex

	// exposeMasterSecret makes instances report the managed master-user
	// secret AWS would create. Off by default so specs that never wire a
	// Secrets Manager client are unaffected.
	exposeMasterSecret bool

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
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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
		MasterUserSecret:      f.masterUserSecretFor(id),
	}}}, nil
}

func (f *fakeRDSClient) DescribeDBSubnetGroups(_ context.Context, in *rds.DescribeDBSubnetGroupsInput, _ ...func(*rds.Options)) (*rds.DescribeDBSubnetGroupsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vpcID := "vpc-" + *in.DBSubnetGroupName
	return &rds.DescribeDBSubnetGroupsOutput{
		DBSubnetGroups: []types.DBSubnetGroup{{DBSubnetGroupName: in.DBSubnetGroupName, VpcId: &vpcID}},
	}, nil
}

func (f *fakeRDSClient) DescribeDBEngineVersions(_ context.Context, _ *rds.DescribeDBEngineVersionsInput, _ ...func(*rds.Options)) (*rds.DescribeDBEngineVersionsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &rds.DescribeDBEngineVersionsOutput{}, nil
}

func (f *fakeRDSClient) ListTagsForResource(_ context.Context, in *rds.ListTagsForResourceInput, _ ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	return &rds.ListTagsForResourceOutput{TagList: fakeMapToTags(i.tags)}, nil
}

func (f *fakeRDSClient) AddTagsToResource(_ context.Context, in *rds.AddTagsToResourceInput, _ ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	id := *in.DBSnapshotIdentifier
	if _, exists := f.snapshots[id]; exists {
		return nil, &types.DBSnapshotAlreadyExistsFault{}
	}
	f.snapshots[id] = &fakeRDSSnapshot{id: id, instanceID: *in.DBInstanceIdentifier, status: "available"}
	return &rds.CreateDBSnapshotOutput{DBSnapshot: &types.DBSnapshot{DBSnapshotIdentifier: &id}}, nil
}

func (f *fakeRDSClient) DescribeDBSnapshots(_ context.Context, in *rds.DescribeDBSnapshotsInput, _ ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.snapshots[*in.DBSnapshotIdentifier]
	if !ok {
		return &rds.DescribeDBSnapshotsOutput{}, nil
	}
	return &rds.DescribeDBSnapshotsOutput{DBSnapshots: []types.DBSnapshot{{DBSnapshotIdentifier: &s.id, Status: &s.status}}}, nil
}

func (f *fakeRDSClient) DeleteDBSnapshot(_ context.Context, in *rds.DeleteDBSnapshotInput, _ ...func(*rds.Options)) (*rds.DeleteDBSnapshotOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakeRDSClient) hasInstance(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.instances[id]
	return ok
}

func (f *fakeRDSClient) setExposeMasterSecret(v bool) {
	f.mu.Lock()
	f.exposeMasterSecret = v
	f.mu.Unlock()
}

// masterUserSecretFor must be called with f.mu held.
func (f *fakeRDSClient) masterUserSecretFor(instanceID string) *types.MasterUserSecret {
	if !f.exposeMasterSecret {
		return nil
	}
	return &types.MasterUserSecret{SecretArn: aws.String(fakeMasterSecretARN(instanceID))}
}

func fakeMasterSecretARN(instanceID string) string {
	return "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds!db-" + instanceID
}

// fakeSecretsManager serves one username/password pair for every secret ARN
// it is asked about, and lets a spec change the password to simulate AWS
// rotating it. Safe to use from a live manager and a spec at once.
type fakeSecretsManager struct {
	mu       sync.Mutex
	password string
}

func newFakeSecretsManager(password string) *fakeSecretsManager {
	return &fakeSecretsManager{password: password}
}

func (f *fakeSecretsManager) rotate(password string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.password = password
}

func (f *fakeSecretsManager) GetSecretValue(_ context.Context, _ *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := json.Marshal(map[string]string{"username": "cloudctl_admin", "password": f.password})
	s := string(body)
	return &secretsmanager.GetSecretValueOutput{SecretString: &s}, nil
}
