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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// fakeSecurityGroup and fakeEC2Client are a minimal in-memory stand-in for
// the real EC2 client, implementing cloudctlaws.EC2Client - see
// fakeRDSClient's own doc comment for why this stays deliberately thin
// (internal/resources/rds's own unit tests already cover the detailed
// security-group behavior exhaustively).
type fakeIngressRule struct {
	port            int32
	consumerGroupID string
}

type fakeSecurityGroup struct {
	id, name, vpcID string
	tags            map[string]string
	ingress         []fakeIngressRule
}

type fakeEC2Client struct {
	groups map[string]*fakeSecurityGroup // keyed by GroupId
	nextID int
}

func newFakeEC2Client() *fakeEC2Client {
	return &fakeEC2Client{groups: map[string]*fakeSecurityGroup{}}
}

func (f *fakeEC2Client) findByNameAndVPC(name, vpcID string) *fakeSecurityGroup {
	for _, g := range f.groups {
		if g.name == name && g.vpcID == vpcID {
			return g
		}
	}
	return nil
}

func (f *fakeEC2Client) CreateSecurityGroup(_ context.Context, in *ec2.CreateSecurityGroupInput, _ ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
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

func (f *fakeEC2Client) DescribeSecurityGroups(_ context.Context, in *ec2.DescribeSecurityGroupsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	if len(in.GroupIds) > 0 {
		g, ok := f.groups[in.GroupIds[0]]
		if !ok {
			return &ec2.DescribeSecurityGroupsOutput{}, nil
		}
		return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []types.SecurityGroup{toSecurityGroupType(g)}}, nil
	}

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
	g := f.findByNameAndVPC(name, vpcID)
	if g == nil {
		return &ec2.DescribeSecurityGroupsOutput{}, nil
	}
	return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []types.SecurityGroup{toSecurityGroupType(g)}}, nil
}

// toSecurityGroupType renders ingress as revokeStaleIngress reads it back:
// one IpPermission per rule, each carrying exactly one UserIdGroupPair -
// matching how this package's own AuthorizeSecurityGroupIngress calls are
// always shaped (see securitygroup.go's reconcileIngress).
func toSecurityGroupType(g *fakeSecurityGroup) types.SecurityGroup {
	perms := make([]types.IpPermission, 0, len(g.ingress))
	for _, rule := range g.ingress {
		perms = append(perms, types.IpPermission{
			IpProtocol:       aws.String("tcp"),
			FromPort:         aws.Int32(rule.port),
			ToPort:           aws.Int32(rule.port),
			UserIdGroupPairs: []types.UserIdGroupPair{{GroupId: aws.String(rule.consumerGroupID)}},
		})
	}
	return types.SecurityGroup{GroupId: &g.id, GroupName: &g.name, VpcId: &g.vpcID, IpPermissions: perms}
}

func (f *fakeEC2Client) AuthorizeSecurityGroupIngress(_ context.Context, in *ec2.AuthorizeSecurityGroupIngressInput, _ ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	g, ok := f.groups[*in.GroupId]
	if !ok {
		return nil, &fakeAWSError{code: "InvalidGroup.NotFound"}
	}
	perm := in.IpPermissions[0]
	port := aws.ToInt32(perm.FromPort)
	consumerGroupID := aws.ToString(perm.UserIdGroupPairs[0].GroupId)
	for _, rule := range g.ingress {
		if rule.port == port && rule.consumerGroupID == consumerGroupID {
			return nil, &fakeAWSError{code: "InvalidPermission.Duplicate"}
		}
	}
	g.ingress = append(g.ingress, fakeIngressRule{port: port, consumerGroupID: consumerGroupID})
	return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
}

func (f *fakeEC2Client) RevokeSecurityGroupIngress(_ context.Context, in *ec2.RevokeSecurityGroupIngressInput, _ ...func(*ec2.Options)) (*ec2.RevokeSecurityGroupIngressOutput, error) {
	g, ok := f.groups[*in.GroupId]
	if !ok {
		return nil, &fakeAWSError{code: "InvalidGroup.NotFound"}
	}
	perm := in.IpPermissions[0]
	port := aws.ToInt32(perm.FromPort)
	consumerGroupID := aws.ToString(perm.UserIdGroupPairs[0].GroupId)
	for i, rule := range g.ingress {
		if rule.port == port && rule.consumerGroupID == consumerGroupID {
			g.ingress = append(g.ingress[:i], g.ingress[i+1:]...)
			return &ec2.RevokeSecurityGroupIngressOutput{}, nil
		}
	}
	return nil, &fakeAWSError{code: "InvalidPermission.NotFound"}
}

func (f *fakeEC2Client) DeleteSecurityGroup(_ context.Context, in *ec2.DeleteSecurityGroupInput, _ ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
	delete(f.groups, *in.GroupId)
	return &ec2.DeleteSecurityGroupOutput{}, nil
}

func (f *fakeEC2Client) CreateTags(context.Context, *ec2.CreateTagsInput, ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	panic("not used - this package tags security groups atomically at creation via TagSpecifications")
}

func (f *fakeEC2Client) DescribeTags(context.Context, *ec2.DescribeTagsInput, ...func(*ec2.Options)) (*ec2.DescribeTagsOutput, error) {
	panic("not used - this package tags security groups atomically at creation via TagSpecifications")
}
