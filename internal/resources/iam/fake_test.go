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

package iam

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/smithy-go"
)

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (throttling, permission-denied, ...) into fakeIAM
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

// fakeRole and fakeIAM are a minimal in-memory stand-in for the real IAM
// client, implementing just the iamAPI methods this package calls, so
// Ensure/Cleanup can be tested without hitting real AWS.
type fakeRole struct {
	arn         string
	tags        map[string]string
	trustPolicy string
	policies    map[string]string // policyName -> policyDocument

	// Attached by someone other than the operator, which is what stops
	// DeleteRole in AWS.
	attachedPolicies []string // managed policy ARNs
	instanceProfiles []string
}

type fakeIAM struct {
	roles map[string]*fakeRole // keyed by role name

	createRoleErr             error
	updateAssumeRolePolicyErr error
	listRoleTagsErr           error
	getRolePolicyErr          error
	putRolePolicyErr          error
	deleteRolePolicyErr       error
	deleteRoleErr             error
	listAttachedErr           error
	listProfilesErr           error

	// putRolePolicyCalls counts every real call - lets a test prove the
	// diff-before-write check actually skips the call when nothing
	// changed, not just trusts it does.
	putRolePolicyCalls int
	listRoleTagsCalls  int
	getRolePolicyCalls int
}

func newFakeIAM() *fakeIAM {
	return &fakeIAM{roles: map[string]*fakeRole{}}
}

func (f *fakeIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	name := *in.RoleName
	arn := r.arn
	trust := r.trustPolicy
	return &iam.GetRoleOutput{Role: &types.Role{RoleName: &name, Arn: &arn, AssumeRolePolicyDocument: &trust}}, nil
}

func (f *fakeIAM) CreateRole(_ context.Context, in *iam.CreateRoleInput, _ ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	if f.createRoleErr != nil {
		return nil, f.createRoleErr
	}
	name := *in.RoleName
	arn := "arn:aws:iam::123456789012:role/" + name
	f.roles[name] = &fakeRole{
		arn:         arn,
		tags:        tagsToMap(in.Tags),
		trustPolicy: *in.AssumeRolePolicyDocument,
		policies:    map[string]string{},
	}
	return &iam.CreateRoleOutput{Role: &types.Role{RoleName: &name, Arn: &arn}}, nil
}

func (f *fakeIAM) UpdateAssumeRolePolicy(_ context.Context, in *iam.UpdateAssumeRolePolicyInput, _ ...func(*iam.Options)) (*iam.UpdateAssumeRolePolicyOutput, error) {
	if f.updateAssumeRolePolicyErr != nil {
		return nil, f.updateAssumeRolePolicyErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	r.trustPolicy = *in.PolicyDocument
	return &iam.UpdateAssumeRolePolicyOutput{}, nil
}

func (f *fakeIAM) TagRole(_ context.Context, in *iam.TagRoleInput, _ ...func(*iam.Options)) (*iam.TagRoleOutput, error) {
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	for _, t := range in.Tags {
		r.tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return &iam.TagRoleOutput{}, nil
}

func (f *fakeIAM) ListRoleTags(_ context.Context, in *iam.ListRoleTagsInput, _ ...func(*iam.Options)) (*iam.ListRoleTagsOutput, error) {
	f.listRoleTagsCalls++
	if f.listRoleTagsErr != nil {
		return nil, f.listRoleTagsErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	return &iam.ListRoleTagsOutput{Tags: mapToTags(r.tags)}, nil
}

func (f *fakeIAM) GetRolePolicy(_ context.Context, in *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	f.getRolePolicyCalls++
	if f.getRolePolicyErr != nil {
		return nil, f.getRolePolicyErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	doc, ok := r.policies[*in.PolicyName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	name, policyName := *in.RoleName, *in.PolicyName
	return &iam.GetRolePolicyOutput{RoleName: &name, PolicyName: &policyName, PolicyDocument: &doc}, nil
}

func (f *fakeIAM) PutRolePolicy(_ context.Context, in *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	f.putRolePolicyCalls++
	if f.putRolePolicyErr != nil {
		return nil, f.putRolePolicyErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	r.policies[*in.PolicyName] = *in.PolicyDocument
	return &iam.PutRolePolicyOutput{}, nil
}

func (f *fakeIAM) DeleteRolePolicy(_ context.Context, in *iam.DeleteRolePolicyInput, _ ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	if f.deleteRolePolicyErr != nil {
		return nil, f.deleteRolePolicyErr
	}
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

func (f *fakeIAM) DeleteRole(_ context.Context, in *iam.DeleteRoleInput, _ ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	if f.deleteRoleErr != nil {
		return nil, f.deleteRoleErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	if len(r.attachedPolicies) > 0 || len(r.instanceProfiles) > 0 {
		return nil, &types.DeleteConflictException{Message: aws.String("Cannot delete entity, must detach all policies first.")}
	}
	delete(f.roles, *in.RoleName)
	return &iam.DeleteRoleOutput{}, nil
}

func (f *fakeIAM) ListAttachedRolePolicies(_ context.Context, in *iam.ListAttachedRolePoliciesInput, _ ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	if f.listAttachedErr != nil {
		return nil, f.listAttachedErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	out := &iam.ListAttachedRolePoliciesOutput{}
	for _, arn := range r.attachedPolicies {
		out.AttachedPolicies = append(out.AttachedPolicies, types.AttachedPolicy{PolicyArn: aws.String(arn)})
	}
	return out, nil
}

func (f *fakeIAM) ListInstanceProfilesForRole(_ context.Context, in *iam.ListInstanceProfilesForRoleInput, _ ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error) {
	if f.listProfilesErr != nil {
		return nil, f.listProfilesErr
	}
	r, ok := f.roles[*in.RoleName]
	if !ok {
		return nil, &types.NoSuchEntityException{}
	}
	out := &iam.ListInstanceProfilesForRoleOutput{}
	for _, name := range r.instanceProfiles {
		out.InstanceProfiles = append(out.InstanceProfiles, types.InstanceProfile{InstanceProfileName: aws.String(name)})
	}
	return out, nil
}
