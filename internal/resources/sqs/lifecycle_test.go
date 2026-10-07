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

package sqs

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

type sqsLifecycleSubject struct {
	client                   *fakeSQS
	namespace, crName, crUID string
}

func newLifecycleSubject(t *testing.T) lifecycletest.Subject {
	t.Helper()
	return &sqsLifecycleSubject{
		client:    newFakeSQS(),
		namespace: "default",
		crName:    "lifecycle-test",
		crUID:     "lifecycle-uid",
	}
}

func (s *sqsLifecycleSubject) ResourceType() string { return resourceType }

func (s *sqsLifecycleSubject) EnsureOne(ctx context.Context, ledger []depsv1alpha1.ManagedResource, name string, opts lifecycletest.EnsureOpts) ([]depsv1alpha1.ManagedResource, error) {
	spec := &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
		{Name: name, DeletionPolicy: opts.DeletionPolicy, Adopt: opts.Adopt},
	}}
	return Ensure(ctx, s.client, nil, nil, nil, s.namespace, s.crName, s.crUID, spec, ledger, nil, nil)
}

func (s *sqsLifecycleSubject) Cleanup(ctx context.Context, ledger []depsv1alpha1.ManagedResource, declared []string, deleting bool) ([]depsv1alpha1.ManagedResource, []lifecycletest.CleanupResult, error) {
	resources := make([]depsv1alpha1.SQSQueueSpec, 0, len(declared))
	for _, n := range declared {
		resources = append(resources, depsv1alpha1.SQSQueueSpec{Name: n})
	}
	spec := &depsv1alpha1.SQSSpec{Resources: resources}
	updated, results, err := Cleanup(ctx, s.client, nil, nil, s.namespace, s.crName, s.crUID, spec, ledger, deleting, false, nil)
	return updated, convertResults(results), err
}

func convertResults(results []CleanupResult) []lifecycletest.CleanupResult {
	out := make([]lifecycletest.CleanupResult, 0, len(results))
	for _, r := range results {
		out = append(out, lifecycletest.CleanupResult{Name: r.Name, Reason: lifecycletest.CleanupReason(r.Reason)})
	}
	return out
}

func (s *sqsLifecycleSubject) queueName(name string) string {
	return cloudctlaws.ResourceName(s.namespace, s.crName, resourceType, name, 80)
}

func (s *sqsLifecycleSubject) SeedForeign(name string, owner *lifecycletest.OwnerIdentity) error {
	queueName := s.queueName(name)
	tags := map[string]string{}
	if owner != nil {
		tags[cloudctlaws.OwnerTagKey] = cloudctlaws.OwnerTagValue(owner.Namespace, owner.CRName)
		tags[cloudctlaws.OwnerUIDTagKey] = owner.CRUID
	}
	s.client.queues[queueName] = &fakeQueue{
		url:                   "https://sqs.us-east-1.amazonaws.com/000000000000/" + queueName,
		arn:                   "arn:aws:sqs:us-east-1:000000000000:" + queueName,
		tags:                  tags,
		approxMessages:        "0",
		approxMessagesHidden:  "0",
		approxMessagesDelayed: "0",
	}
	return nil
}

func (s *sqsLifecycleSubject) SetNonEmpty(name string, nonEmpty bool) error {
	q, ok := s.client.queues[s.queueName(name)]
	if !ok {
		return fmt.Errorf("queue %q not found", name)
	}
	if nonEmpty {
		q.approxMessages = "5"
	} else {
		q.approxMessages = "0"
	}
	return nil
}
