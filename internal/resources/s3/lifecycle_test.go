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

package s3

import (
	"context"
	"fmt"
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/lifecycletest"
)

// TestSharedLifecycleScenarios plugs this package's real Ensure/Cleanup
// into the cross-resource-type lifecycle suite - see
// internal/resources/lifecycletest's package doc for what it covers and
// why.
func TestSharedLifecycleScenarios(t *testing.T) {
	lifecycletest.Run(t, newLifecycleSubject)
}

type s3LifecycleSubject struct {
	client                   *fakeS3
	namespace, crName, crUID string
}

func newLifecycleSubject(t *testing.T) lifecycletest.Subject {
	t.Helper()
	return &s3LifecycleSubject{
		client:    newFakeS3(),
		namespace: "default",
		crName:    "lifecycle-test",
		crUID:     "lifecycle-uid",
	}
}

func (s *s3LifecycleSubject) ResourceType() string { return resourceType }

func (s *s3LifecycleSubject) EnsureOne(ctx context.Context, ledger []depsv1alpha1.ManagedResource, name string, opts lifecycletest.EnsureOpts) ([]depsv1alpha1.ManagedResource, error) {
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
		{Name: name, DeletionPolicy: opts.DeletionPolicy, Adopt: opts.Adopt},
	}}
	return Ensure(ctx, s.client, nil, nil, s.namespace, s.crName, s.crUID, testRegion, testAccountID, spec, ledger, nil, nil)
}

func (s *s3LifecycleSubject) Cleanup(ctx context.Context, ledger []depsv1alpha1.ManagedResource, declared []string, deleting bool) ([]depsv1alpha1.ManagedResource, []lifecycletest.CleanupResult, error) {
	resources := make([]depsv1alpha1.S3BucketSpec, 0, len(declared))
	for _, n := range declared {
		resources = append(resources, depsv1alpha1.S3BucketSpec{Name: n})
	}
	spec := &depsv1alpha1.S3Spec{Resources: resources}
	updated, results, err := Cleanup(ctx, s.client, s.namespace, s.crName, s.crUID, spec, ledger, deleting, false, nil)
	return updated, convertResults(results), err
}

func convertResults(results []CleanupResult) []lifecycletest.CleanupResult {
	out := make([]lifecycletest.CleanupResult, 0, len(results))
	for _, r := range results {
		out = append(out, lifecycletest.CleanupResult{Name: r.Name, Reason: lifecycletest.CleanupReason(r.Reason)})
	}
	return out
}

func (s *s3LifecycleSubject) bucketName(name string) string {
	return bucketName(s.namespace, s.crName, name, testAccountID)
}

func (s *s3LifecycleSubject) SeedForeign(name string, owner *lifecycletest.OwnerIdentity) error {
	bucket := s.bucketName(name)
	tags := map[string]string{}
	if owner != nil {
		tags[cloudctlaws.OwnerTagKey] = cloudctlaws.OwnerTagValue(owner.Namespace, owner.CRName)
		tags[cloudctlaws.OwnerUIDTagKey] = owner.CRUID
	}
	s.client.buckets[bucket] = &fakeBucket{tags: tags}
	return nil
}

func (s *s3LifecycleSubject) SetNonEmpty(name string, nonEmpty bool) error {
	b, ok := s.client.buckets[s.bucketName(name)]
	if !ok {
		return fmt.Errorf("bucket %q not found", name)
	}
	if nonEmpty {
		b.versions = []fakeObjectVersion{{key: "file.txt", versionID: "v1"}}
	} else {
		b.versions = nil
	}
	return nil
}
