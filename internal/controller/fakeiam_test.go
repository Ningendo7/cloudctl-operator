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

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// fakeRole and fakeIAMClient are a minimal in-memory stand-in for the real
// IAM client, implementing cloudctlaws.IAMClient, so the controller's own
// wiring (IAM role derivation as part of a full Reconcile()) can be tested
// without hitting real AWS. Detailed IAM behavior (grant collection,
// trust-policy drift correction, ownership verification) is already
// covered by internal/resources/iam's own unit tests; this fake only needs
// to be complete enough to exercise the controller's plumbing around it.
type fakeRole struct {
	arn         string
	tags        map[string]string
	trustPolicy string
	policies    map[string]string // policyName -> policyDocument
}

type fakeIAMClient struct {
	roles map[string]*fakeRole // keyed by role name

	createRoleErr error
}

func newFakeIAMClient() *fakeIAMClient {
	return &fakeIAMClient{roles: map[string]*fakeRole{}}
}

func (f *fakeIAMClient) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	name := *in.RoleName
	arn := r.arn
	trust := r.trustPolicy
	return &iam.GetRoleOutput{Role: &types.Role{RoleName: &name, Arn: &arn, AssumeRolePolicyDocument: &trust}}, nil
}

func (f *fakeIAMClient) CreateRole(_ context.Context, in *iam.CreateRoleInput, _ ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	if f.createRoleErr != nil {
		return nil, f.createRoleErr
	}
	name := *in.RoleName
	arn := "arn:aws:iam::123456789012:role/" + name
	f.roles[name] = &fakeRole{
		arn:         arn,
		tags:        tagsFromIAMSlice(in.Tags),
		trustPolicy: *in.AssumeRolePolicyDocument,
		policies:    map[string]string{},
	}
	return &iam.CreateRoleOutput{Role: &types.Role{RoleName: &name, Arn: &arn}}, nil
}

func (f *fakeIAMClient) UpdateAssumeRolePolicy(_ context.Context, in *iam.UpdateAssumeRolePolicyInput, _ ...func(*iam.Options)) (*iam.UpdateAssumeRolePolicyOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	r.trustPolicy = *in.PolicyDocument
	return &iam.UpdateAssumeRolePolicyOutput{}, nil
}

func (f *fakeIAMClient) ListRoleTags(_ context.Context, in *iam.ListRoleTagsInput, _ ...func(*iam.Options)) (*iam.ListRoleTagsOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	return &iam.ListRoleTagsOutput{Tags: tagsToIAMSlice(r.tags)}, nil
}

func (f *fakeIAMClient) PutRolePolicy(_ context.Context, in *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	r.policies[*in.PolicyName] = *in.PolicyDocument
	return &iam.PutRolePolicyOutput{}, nil
}

func (f *fakeIAMClient) DeleteRolePolicy(_ context.Context, in *iam.DeleteRolePolicyInput, _ ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	if _, exists := r.policies[*in.PolicyName]; !exists {
		return nil, &types.NoSuchEntityException{}
	}
	delete(r.policies, *in.PolicyName)
	return &iam.DeleteRolePolicyOutput{}, nil
}

func (f *fakeIAMClient) DeleteRole(_ context.Context, in *iam.DeleteRoleInput, _ ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	if _, ok := f.roles[*in.RoleName]; !ok {
		return nil, &types.NoSuchEntityException{}
	}
	delete(f.roles, *in.RoleName)
	return &iam.DeleteRoleOutput{}, nil
}

func tagsFromIAMSlice(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func tagsToIAMSlice(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{Key: &k, Value: &v})
	}
	return tags
}
