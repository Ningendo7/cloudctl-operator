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

package kms

import (
	"context"
	"fmt"

	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/lifecycletest"
)

// TestSharedLifecycleScenarios plugs this package's real Ensure/Cleanup
// into the cross-resource-type lifecycle suite - see
// internal/resources/lifecycletest's package doc for what it covers and
// why. The DeleteBlockedWhenNonEmpty/DeleteRemovesWhenEmpty scenarios skip
// here (SetNonEmpty returns ErrUnsupported): a KMS key has no emptiness
// concept, deletion is gated by AWS's own ScheduleKeyDeletion wait instead.
func TestSharedLifecycleScenarios(t *testing.T) {
	lifecycletest.Run(t, newLifecycleSubject)
}

type kmsLifecycleSubject struct {
	client                   *fakeKMS
	k8sClient                client.Client
	namespace, crName, crUID string
}

func newLifecycleSubject(t *testing.T) lifecycletest.Subject {
	t.Helper()
	return &kmsLifecycleSubject{
		client:    newFakeKMS(),
		k8sClient: fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).Build(),
		namespace: "default",
		crName:    "lifecycle-test",
		crUID:     "lifecycle-uid",
	}
}

func (s *kmsLifecycleSubject) ResourceType() string { return resourceType }

func (s *kmsLifecycleSubject) EnsureOne(ctx context.Context, ledger []depsv1alpha1.ManagedResource, name string, opts lifecycletest.EnsureOpts) ([]depsv1alpha1.ManagedResource, error) {
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: name, DeletionPolicy: opts.DeletionPolicy, Adopt: opts.Adopt},
	}}
	return Ensure(ctx, s.client, s.namespace, s.crName, s.crUID, spec, ledger, nil, nil)
}

func (s *kmsLifecycleSubject) Cleanup(ctx context.Context, ledger []depsv1alpha1.ManagedResource, declared []string, deleting bool) ([]depsv1alpha1.ManagedResource, []lifecycletest.CleanupResult, error) {
	resources := make([]depsv1alpha1.KMSKeySpec, 0, len(declared))
	for _, n := range declared {
		resources = append(resources, depsv1alpha1.KMSKeySpec{Name: n})
	}
	spec := &depsv1alpha1.KMSSpec{Resources: resources}
	updated, results, err := Cleanup(ctx, s.client, s.k8sClient, s.namespace, s.crName, s.crUID, spec, nil, ledger, deleting, nil)
	return updated, convertResults(results), err
}

func convertResults(results []CleanupResult) []lifecycletest.CleanupResult {
	out := make([]lifecycletest.CleanupResult, 0, len(results))
	for _, r := range results {
		out = append(out, lifecycletest.CleanupResult{Name: r.Name, Reason: lifecycletest.CleanupReason(r.Reason)})
	}
	return out
}

func (s *kmsLifecycleSubject) SeedForeign(name string, owner *lifecycletest.OwnerIdentity) error {
	arn := "arn:aws:kms:us-east-1:123456789012:key/foreign-" + name
	tags := map[string]string{}
	if owner != nil {
		tags[cloudctlaws.OwnerTagKey] = cloudctlaws.OwnerTagValue(owner.Namespace, owner.CRName)
		tags[cloudctlaws.OwnerUIDTagKey] = owner.CRUID
	}
	s.client.keys[arn] = &fakeKey{arn: arn, keyID: "foreign-" + name, keyState: types.KeyStateEnabled, tags: tags}
	s.client.aliases[aliasName(s.namespace, s.crName, name, keyOptions{})] = arn
	return nil
}

func (s *kmsLifecycleSubject) SetNonEmpty(name string, nonEmpty bool) error {
	return fmt.Errorf("kms keys have no emptiness concept: %w", lifecycletest.ErrUnsupported)
}
