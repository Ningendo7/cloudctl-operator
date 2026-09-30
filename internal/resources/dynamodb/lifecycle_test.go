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
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/lifecycletest"
)

// lifecyclePartitionKey is fixed across EnsureOne and SeedForeign so a
// seeded foreign table's key schema always matches what Ensure declares -
// a real key-schema mismatch is its own, resource-specific test elsewhere.
const lifecyclePartitionKey = "id"

// TestSharedLifecycleScenarios plugs this package's real Ensure/Cleanup
// into the cross-resource-type lifecycle suite - see
// internal/resources/lifecycletest's package doc for what it covers and
// why.
func TestSharedLifecycleScenarios(t *testing.T) {
	lifecycletest.Run(t, newLifecycleSubject)
}

type dynamodbLifecycleSubject struct {
	client                   *fakeDynamoDB
	namespace, crName, crUID string
}

func newLifecycleSubject(t *testing.T) lifecycletest.Subject {
	t.Helper()
	return &dynamodbLifecycleSubject{
		client:    newFakeDynamoDB(),
		namespace: "default",
		crName:    "lifecycle-test",
		crUID:     "lifecycle-uid",
	}
}

func (s *dynamodbLifecycleSubject) ResourceType() string { return resourceType }

func (s *dynamodbLifecycleSubject) EnsureOne(ctx context.Context, ledger []depsv1alpha1.ManagedResource, name string, opts lifecycletest.EnsureOpts) ([]depsv1alpha1.ManagedResource, error) {
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: name, PartitionKey: lifecyclePartitionKey, DeletionPolicy: opts.DeletionPolicy, Adopt: opts.Adopt},
	}}
	updated, err := Ensure(ctx, s.client, nil, nil, s.namespace, s.crName, s.crUID, spec, ledger, nil, nil)
	if err != nil {
		return updated, err
	}
	// Move past Creating to Verified, same as a real second reconcile
	// would - the shared Create scenario only checks for a ledger entry
	// with an ARN, which a Creating entry already has, but every other
	// scenario needs a settled Verified entry to build on.
	return Ensure(ctx, s.client, nil, nil, s.namespace, s.crName, s.crUID, spec, updated, nil, nil)
}

func (s *dynamodbLifecycleSubject) Cleanup(ctx context.Context, ledger []depsv1alpha1.ManagedResource, declared []string, deleting bool) ([]depsv1alpha1.ManagedResource, []lifecycletest.CleanupResult, error) {
	resources := make([]depsv1alpha1.DynamoDBTableSpec, 0, len(declared))
	for _, n := range declared {
		resources = append(resources, depsv1alpha1.DynamoDBTableSpec{Name: n, PartitionKey: lifecyclePartitionKey})
	}
	spec := &depsv1alpha1.DynamoDBSpec{Resources: resources}
	updated, results, err := Cleanup(ctx, s.client, s.namespace, s.crName, s.crUID, spec, ledger, deleting, nil)
	return updated, convertResults(results), err
}

func convertResults(results []CleanupResult) []lifecycletest.CleanupResult {
	out := make([]lifecycletest.CleanupResult, 0, len(results))
	for _, r := range results {
		out = append(out, lifecycletest.CleanupResult{Name: r.Name, Reason: lifecycletest.CleanupReason(r.Reason)})
	}
	return out
}

func (s *dynamodbLifecycleSubject) tableName(name string) string {
	return cloudctlaws.ResourceName(s.namespace, s.crName, resourceType, name, 255)
}

func (s *dynamodbLifecycleSubject) SeedForeign(name string, owner *lifecycletest.OwnerIdentity) error {
	tableName := s.tableName(name)
	tags := map[string]string{}
	if owner != nil {
		tags[cloudctlaws.OwnerTagKey] = cloudctlaws.OwnerTagValue(owner.Namespace, owner.CRName)
		tags[cloudctlaws.OwnerUIDTagKey] = owner.CRUID
	}
	s.client.tables[tableName] = &fakeTable{
		arn:          "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:       types.TableStatusActive,
		tags:         tags,
		partitionKey: lifecyclePartitionKey,
	}
	return nil
}

func (s *dynamodbLifecycleSubject) SetNonEmpty(name string, nonEmpty bool) error {
	table, ok := s.client.tables[s.tableName(name)]
	if !ok {
		return fmt.Errorf("table %q not found", name)
	}
	if nonEmpty {
		table.itemCount = 5
	} else {
		table.itemCount = 0
	}
	return nil
}
