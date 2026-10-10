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
	"fmt"
	"maps"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
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
	allocatedStorage      int32
	dbSubnetGroupName     string
	multiAZ               bool
	backupRetentionPeriod int32
	kmsKeyARN             string
	masterUsername        string
	manageMasterPassword  bool
	endpointAddress       string
	endpointPort          int32
	masterUserSecretARN   string
}

type fakeSnapshot struct {
	id, arn, instanceID, status string
}

type fakeRDS struct {
	instances map[string]*fakeInstance // keyed by DBInstanceIdentifier
	snapshots map[string]*fakeSnapshot // keyed by DBSnapshotIdentifier

	createDBInstanceErr       error
	modifyDBInstanceErr       error
	describeDBInstancesErr    error
	listTagsForResourceErr    error
	addTagsToResourceErr      error
	removeTagsFromResourceErr error
	describeDBSubnetGroupsErr error
	createDBSnapshotErr       error
	describeDBSnapshotsErr    error
	deleteDBSnapshotErr       error

	// listTagsForResourceCalls counts real calls so trust-window tests can
	// assert a within-window reconcile skips re-verifying ownership tags
	// entirely, the same property every other resource package's own
	// trust-window test already proves.
	listTagsForResourceCalls    int
	describeDBSubnetGroupsCalls int
	createDBSnapshotCalls    int
	describeDBSnapshotsCalls int
	deleteDBSnapshotCalls    int
	// describeDBInstancesCalls counts every DescribeDBInstances call, keyed
	// by instance ID - lets the describe-cache tests prove a cached lookup
	// never reaches the client a second time.
	describeDBInstancesCalls map[string]int
	// deletedSnapshotIDs records every ID DeleteDBSnapshot was actually
	// called with, in order - lets a test assert exactly which snapshot(s)
	// were deleted, not just how many calls happened.
	deletedSnapshotIDs []string

	// lastCreateDBInstanceInput captures the raw input to the most recent
	// CreateDBInstance call, so a test can assert directly on what this
	// package actually sent AWS - in particular, that it never sets a
	// static MasterUserPassword, which would make this operator a
	// competing source of truth for credentials instead of Secrets
	// Manager.
	lastCreateDBInstanceInput *rds.CreateDBInstanceInput
	lastModifyDBInstanceInput *rds.ModifyDBInstanceInput
	modifyDBInstanceCalls     int

	// snapshotStatus overrides a newly created snapshot's status so cleanup
	// tests can simulate the still-creating/available/failed progression
	// AWS itself goes through; defaults to "available" like fakeInstance's
	// own create behavior, a deliberate test simplification over the real
	// (always async) API.
	snapshotStatus string
}

func newFakeRDS() *fakeRDS {
	return &fakeRDS{
		instances:                map[string]*fakeInstance{},
		snapshots:                map[string]*fakeSnapshot{},
		describeDBInstancesCalls: map[string]int{},
	}
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
	f.lastCreateDBInstanceInput = in
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
	var masterUsername string
	if in.MasterUsername != nil {
		masterUsername = *in.MasterUsername
	}
	var manageMasterPassword bool
	if in.ManageMasterUserPassword != nil {
		manageMasterPassword = *in.ManageMasterUserPassword
	}
	f.instances[id] = &fakeInstance{
		arn:                   arn,
		tags:                  tagsToMap(in.Tags),
		status:                "available",
		engine:                *in.Engine,
		engineVersion:         *in.EngineVersion,
		instanceClass:         *in.DBInstanceClass,
		allocatedStorage:      aws.ToInt32(in.AllocatedStorage),
		dbSubnetGroupName:     *in.DBSubnetGroupName,
		multiAZ:               multiAZ,
		backupRetentionPeriod: backupRetention,
		kmsKeyARN:             kmsKeyARN,
		masterUsername:        masterUsername,
		manageMasterPassword:  manageMasterPassword,
	}
	return &rds.CreateDBInstanceOutput{
		DBInstance: &types.DBInstance{
			DBInstanceArn:        &arn,
			DBInstanceIdentifier: &id,
			DBInstanceStatus:     strPtr("creating"),
		},
	}, nil
}

func (f *fakeRDS) ModifyDBInstance(_ context.Context, in *rds.ModifyDBInstanceInput, _ ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error) {
	f.modifyDBInstanceCalls++
	f.lastModifyDBInstanceInput = in
	if f.modifyDBInstanceErr != nil {
		return nil, f.modifyDBInstanceErr
	}
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
	if in.AllocatedStorage != nil {
		i.allocatedStorage = *in.AllocatedStorage
	}
	id := *in.DBInstanceIdentifier
	return &rds.ModifyDBInstanceOutput{DBInstance: &types.DBInstance{DBInstanceIdentifier: &id}}, nil
}

func (f *fakeRDS) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput, _ ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	f.describeDBInstancesCalls[*in.DBInstanceIdentifier]++
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
	var endpoint *types.Endpoint
	if i.endpointAddress != "" {
		endpoint = &types.Endpoint{Address: &i.endpointAddress, Port: &i.endpointPort}
	}
	var masterUserSecret *types.MasterUserSecret
	if i.masterUserSecretARN != "" {
		masterUserSecret = &types.MasterUserSecret{SecretArn: &i.masterUserSecretARN}
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
				AllocatedStorage:      &i.allocatedStorage,
				DBSubnetGroup:         &types.DBSubnetGroup{DBSubnetGroupName: &i.dbSubnetGroupName},
				MultiAZ:               &i.multiAZ,
				BackupRetentionPeriod: &i.backupRetentionPeriod,
				KmsKeyId:              kmsKeyID,
				Endpoint:              endpoint,
				MasterUserSecret:      masterUserSecret,
				StorageEncrypted:      boolPtr(i.kmsKeyARN != ""),
			},
		},
	}, nil
}

func (f *fakeRDS) ListTagsForResource(_ context.Context, in *rds.ListTagsForResourceInput, _ ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error) {
	f.listTagsForResourceCalls++
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

func (f *fakeRDS) RemoveTagsFromResource(_ context.Context, in *rds.RemoveTagsFromResourceInput, _ ...func(*rds.Options)) (*rds.RemoveTagsFromResourceOutput, error) {
	if f.removeTagsFromResourceErr != nil {
		return nil, f.removeTagsFromResourceErr
	}
	i := f.findByARN(*in.ResourceName)
	if i == nil {
		return nil, &types.DBInstanceNotFoundFault{}
	}
	for _, k := range in.TagKeys {
		delete(i.tags, k)
	}
	return &rds.RemoveTagsFromResourceOutput{}, nil
}

func (f *fakeRDS) CreateDBSnapshot(_ context.Context, in *rds.CreateDBSnapshotInput, _ ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error) {
	f.createDBSnapshotCalls++
	if f.createDBSnapshotErr != nil {
		return nil, f.createDBSnapshotErr
	}
	id := *in.DBSnapshotIdentifier
	if _, exists := f.snapshots[id]; exists {
		return nil, &types.DBSnapshotAlreadyExistsFault{}
	}
	snapStatus := f.snapshotStatus
	if snapStatus == "" {
		snapStatus = "available"
	}
	arn := "arn:aws:rds:us-east-1:123456789012:snapshot:" + id
	snap := &fakeSnapshot{id: id, arn: arn, instanceID: *in.DBInstanceIdentifier, status: snapStatus}
	f.snapshots[id] = snap
	return &rds.CreateDBSnapshotOutput{
		DBSnapshot: &types.DBSnapshot{DBSnapshotIdentifier: &id, DBSnapshotArn: &arn, Status: &snap.status},
	}, nil
}

func (f *fakeRDS) DescribeDBSnapshots(_ context.Context, in *rds.DescribeDBSnapshotsInput, _ ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error) {
	f.describeDBSnapshotsCalls++
	if f.describeDBSnapshotsErr != nil {
		return nil, f.describeDBSnapshotsErr
	}
	if in.DBSnapshotIdentifier == nil {
		return &rds.DescribeDBSnapshotsOutput{}, nil
	}
	s, ok := f.snapshots[*in.DBSnapshotIdentifier]
	if !ok {
		// Also accept lookup by ARN - cleanup.go stores the snapshot's
		// identifier as the ledger ARN and passes it straight back in as
		// DBSnapshotIdentifier, so this only ever needs to match by ID in
		// practice, but staying lenient here costs nothing.
		for _, candidate := range f.snapshots {
			if candidate.arn == *in.DBSnapshotIdentifier {
				s = candidate
				ok = true
				break
			}
		}
	}
	if !ok {
		return &rds.DescribeDBSnapshotsOutput{}, nil
	}
	return &rds.DescribeDBSnapshotsOutput{
		DBSnapshots: []types.DBSnapshot{{DBSnapshotIdentifier: &s.id, DBInstanceIdentifier: &s.instanceID, DBSnapshotArn: &s.arn, Status: &s.status}},
	}, nil
}

func (f *fakeRDS) DeleteDBSnapshot(_ context.Context, in *rds.DeleteDBSnapshotInput, _ ...func(*rds.Options)) (*rds.DeleteDBSnapshotOutput, error) {
	f.deleteDBSnapshotCalls++
	f.deletedSnapshotIDs = append(f.deletedSnapshotIDs, *in.DBSnapshotIdentifier)
	if f.deleteDBSnapshotErr != nil {
		return nil, f.deleteDBSnapshotErr
	}
	if _, ok := f.snapshots[*in.DBSnapshotIdentifier]; !ok {
		return nil, &types.DBSnapshotNotFoundFault{}
	}
	delete(f.snapshots, *in.DBSnapshotIdentifier)
	return &rds.DeleteDBSnapshotOutput{}, nil
}

func (f *fakeRDS) DescribeDBSubnetGroups(_ context.Context, in *rds.DescribeDBSubnetGroupsInput, _ ...func(*rds.Options)) (*rds.DescribeDBSubnetGroupsOutput, error) {
	f.describeDBSubnetGroupsCalls++
	if f.describeDBSubnetGroupsErr != nil {
		return nil, f.describeDBSubnetGroupsErr
	}
	// Deterministic, not configurable per-test: every existing test only
	// needs this call to succeed, never cares which VPC it resolves to.
	vpcID := "vpc-" + *in.DBSubnetGroupName
	return &rds.DescribeDBSubnetGroupsOutput{
		DBSubnetGroups: []types.DBSubnetGroup{{DBSubnetGroupName: in.DBSubnetGroupName, VpcId: &vpcID}},
	}, nil
}

// setSnapshotStatus lets a cleanup test simulate a snapshot's async
// progression (creating -> available/failed) between two Cleanup calls,
// mirroring how fakeInstance's own status field is mutated directly in
// rds_test.go rather than through any client call the real API lacks too.
func (f *fakeRDS) setSnapshotStatus(id, status string) {
	if s, ok := f.snapshots[id]; ok {
		s.status = status
	}
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

// fakeIngressRule records just enough of one AuthorizeSecurityGroupIngress
// call to assert on later - which other security group it allows in, and
// on which port.
type fakeIngressRule struct {
	sourceGroupID    string
	fromPort, toPort int32
}

// fakeSecurityGroup and fakeEC2 are a minimal in-memory stand-in for the
// real EC2 client, implementing just the methods EC2Client declares that
// this package's security-group logic actually calls.
type fakeSecurityGroup struct {
	id, name, vpcID string
	tags            map[string]string
	ingress         []fakeIngressRule
}

type fakeEC2 struct {
	groups map[string]*fakeSecurityGroup // keyed by GroupId
	nextID int

	createSecurityGroupErr           error
	describeSecurityGroupsErr        error
	describeSecurityGroupsByIDErr    error // scoped to the GroupIds-based lookup reconcileIngress's revoke check uses
	authorizeSecurityGroupIngressErr error
	revokeSecurityGroupIngressErr    error

	deleteSecurityGroupErr error

	authorizeSecurityGroupIngressCalls int
	revokeSecurityGroupIngressCalls    int
	deleteSecurityGroupCalls           int
	// revokedGroupIDs records every consumer security group ID actually
	// revoked, in order - lets a test assert exactly which one(s) were
	// removed, not just how many calls happened.
	revokedGroupIDs []string
}

func newFakeEC2() *fakeEC2 {
	return &fakeEC2{groups: map[string]*fakeSecurityGroup{}}
}

func (f *fakeEC2) findByNameAndVPC(name, vpcID string) *fakeSecurityGroup {
	for _, g := range f.groups {
		if g.name == name && g.vpcID == vpcID {
			return g
		}
	}
	return nil
}

func (f *fakeEC2) CreateSecurityGroup(_ context.Context, in *ec2.CreateSecurityGroupInput, _ ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
	if f.createSecurityGroupErr != nil {
		return nil, f.createSecurityGroupErr
	}
	f.nextID++
	id := fmt.Sprintf("sg-%08d", f.nextID)
	tags := map[string]string{}
	for _, spec := range in.TagSpecifications {
		for _, t := range spec.Tags {
			if t.Key != nil && t.Value != nil {
				tags[*t.Key] = *t.Value
			}
		}
	}
	f.groups[id] = &fakeSecurityGroup{id: id, name: *in.GroupName, vpcID: *in.VpcId, tags: tags}
	return &ec2.CreateSecurityGroupOutput{GroupId: &id}, nil
}

func (f *fakeEC2) DescribeSecurityGroups(_ context.Context, in *ec2.DescribeSecurityGroupsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	if f.describeSecurityGroupsErr != nil {
		return nil, f.describeSecurityGroupsErr
	}

	var g *fakeSecurityGroup
	if len(in.GroupIds) > 0 {
		if f.describeSecurityGroupsByIDErr != nil {
			return nil, f.describeSecurityGroupsByIDErr
		}
		g = f.groups[in.GroupIds[0]]
	} else {
		var name, vpcID string
		for _, filt := range in.Filters {
			if filt.Name == nil || len(filt.Values) == 0 {
				continue
			}
			switch *filt.Name {
			case "group-name":
				name = filt.Values[0]
			case "vpc-id":
				vpcID = filt.Values[0]
			}
		}
		g = f.findByNameAndVPC(name, vpcID)
	}
	if g == nil {
		return &ec2.DescribeSecurityGroupsOutput{}, nil
	}

	perms := make([]ec2types.IpPermission, 0, len(g.ingress))
	for _, rule := range g.ingress {
		sourceGroupID := rule.sourceGroupID
		perms = append(perms, ec2types.IpPermission{
			IpProtocol:       aws.String("tcp"),
			FromPort:         aws.Int32(rule.fromPort),
			ToPort:           aws.Int32(rule.toPort),
			UserIdGroupPairs: []ec2types.UserIdGroupPair{{GroupId: &sourceGroupID}},
		})
	}
	return &ec2.DescribeSecurityGroupsOutput{
		SecurityGroups: []ec2types.SecurityGroup{{GroupId: &g.id, GroupName: &g.name, VpcId: &g.vpcID, IpPermissions: perms}},
	}, nil
}

func (f *fakeEC2) AuthorizeSecurityGroupIngress(_ context.Context, in *ec2.AuthorizeSecurityGroupIngressInput, _ ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	f.authorizeSecurityGroupIngressCalls++
	if f.authorizeSecurityGroupIngressErr != nil {
		return nil, f.authorizeSecurityGroupIngressErr
	}
	g, ok := f.groups[*in.GroupId]
	if !ok {
		return nil, &fakeAWSError{code: "InvalidGroup.NotFound", fault: smithy.FaultClient}
	}
	for _, perm := range in.IpPermissions {
		for _, pair := range perm.UserIdGroupPairs {
			for _, existing := range g.ingress {
				if existing.sourceGroupID == *pair.GroupId && existing.fromPort == *perm.FromPort && existing.toPort == *perm.ToPort {
					return nil, &fakeAWSError{code: "InvalidPermission.Duplicate", fault: smithy.FaultClient}
				}
			}
			g.ingress = append(g.ingress, fakeIngressRule{sourceGroupID: *pair.GroupId, fromPort: *perm.FromPort, toPort: *perm.ToPort})
		}
	}
	return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
}

func (f *fakeEC2) RevokeSecurityGroupIngress(_ context.Context, in *ec2.RevokeSecurityGroupIngressInput, _ ...func(*ec2.Options)) (*ec2.RevokeSecurityGroupIngressOutput, error) {
	f.revokeSecurityGroupIngressCalls++
	if f.revokeSecurityGroupIngressErr != nil {
		return nil, f.revokeSecurityGroupIngressErr
	}
	g, ok := f.groups[*in.GroupId]
	if !ok {
		return nil, &fakeAWSError{code: "InvalidGroup.NotFound", fault: smithy.FaultClient}
	}
	for _, perm := range in.IpPermissions {
		for _, pair := range perm.UserIdGroupPairs {
			found := false
			filtered := g.ingress[:0]
			for _, existing := range g.ingress {
				if existing.sourceGroupID == *pair.GroupId {
					found = true
					continue
				}
				filtered = append(filtered, existing)
			}
			g.ingress = filtered
			if !found {
				return nil, &fakeAWSError{code: "InvalidPermission.NotFound", fault: smithy.FaultClient}
			}
			f.revokedGroupIDs = append(f.revokedGroupIDs, *pair.GroupId)
		}
	}
	return &ec2.RevokeSecurityGroupIngressOutput{}, nil
}

func (f *fakeEC2) DeleteSecurityGroup(_ context.Context, in *ec2.DeleteSecurityGroupInput, _ ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
	f.deleteSecurityGroupCalls++
	if f.deleteSecurityGroupErr != nil {
		return nil, f.deleteSecurityGroupErr
	}
	if _, ok := f.groups[*in.GroupId]; !ok {
		return nil, &fakeAWSError{code: "InvalidGroup.NotFound", fault: smithy.FaultClient}
	}
	delete(f.groups, *in.GroupId)
	return &ec2.DeleteSecurityGroupOutput{}, nil
}

func (f *fakeEC2) CreateTags(context.Context, *ec2.CreateTagsInput, ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	panic("not used - this package tags security groups atomically at creation via TagSpecifications")
}

func (f *fakeEC2) DescribeTags(context.Context, *ec2.DescribeTagsInput, ...func(*ec2.Options)) (*ec2.DescribeTagsOutput, error) {
	panic("not used - this package tags security groups atomically at creation via TagSpecifications")
}

// fakeSecretsManager is a minimal in-memory stand-in for the real Secrets
// Manager client, keyed by secret ARN since that's the only identifier
// this package ever looks a secret up by.
type fakeSecretsManager struct {
	secrets             map[string]string // ARN -> raw SecretString JSON
	getSecretValueErr   error
	getSecretValueCalls int
}

func newFakeSecretsManager() *fakeSecretsManager {
	return &fakeSecretsManager{secrets: map[string]string{}}
}

func (f *fakeSecretsManager) GetSecretValue(_ context.Context, in *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.getSecretValueCalls++
	if f.getSecretValueErr != nil {
		return nil, f.getSecretValueErr
	}
	raw, ok := f.secrets[*in.SecretId]
	if !ok {
		return nil, &smtypes.ResourceNotFoundException{}
	}
	return &secretsmanager.GetSecretValueOutput{SecretString: &raw}, nil
}
