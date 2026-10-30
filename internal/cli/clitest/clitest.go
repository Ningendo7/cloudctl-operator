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

// Package clitest holds fixtures and a fake cluster reader for the CLI's
// tests. It is test support only and must never be imported by production code.
package clitest

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
)

// Now is the fixed time every CLI test treats as the present.
var Now = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

// Option changes a CR built by CR.
type Option func(*depsv1alpha1.AppDependencies)

// CR builds a CR at generation 12 that was created five days before Now.
func CR(namespace, name string, opts ...Option) *depsv1alpha1.AppDependencies {
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{
		Namespace:         namespace,
		Name:              name,
		Generation:        12,
		CreationTimestamp: metav1.NewTime(Now.Add(-5 * 24 * time.Hour)),
	}}
	for _, opt := range opts {
		opt(cr)
	}
	return cr
}

// Generation sets metadata.generation.
func Generation(g int64) Option {
	return func(cr *depsv1alpha1.AppDependencies) { cr.Generation = g }
}

// Deleting marks the CR as being deleted.
func Deleting() Option {
	return func(cr *depsv1alpha1.AppDependencies) {
		t := metav1.NewTime(Now.Add(-time.Minute))
		cr.DeletionTimestamp = &t
	}
}

// Condition appends a condition that last changed `ago` before Now.
func Condition(condType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64, ago time.Duration) Option {
	return func(cr *depsv1alpha1.AppDependencies) {
		cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
			Type: condType, Status: status, Reason: reason, Message: message,
			ObservedGeneration: observedGeneration, LastTransitionTime: metav1.NewTime(Now.Add(-ago)),
		})
	}
}

// Entry appends a ledger entry.
func Entry(e depsv1alpha1.ManagedResource) Option {
	return func(cr *depsv1alpha1.AppDependencies) {
		cr.Status.ManagedResources = append(cr.Status.ManagedResources, e)
	}
}

// Res builds a Verified, Retain-policy ledger entry created two days before
// Now, which mods can change.
func Res(resourceType, name string, mods ...func(*depsv1alpha1.ManagedResource)) depsv1alpha1.ManagedResource {
	e := depsv1alpha1.ManagedResource{
		Type: resourceType, Name: name,
		ARN:            fmt.Sprintf("arn:aws:%s:us-east-1:123456789012:%s", resourceType, name),
		DeletionPolicy: depsv1alpha1.DeletionPolicyRetain,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		OnDelete:       depsv1alpha1.OnDeleteRetain,
		CreatedAt:      metav1.NewTime(Now.Add(-48 * time.Hour)),
	}
	for _, m := range mods {
		m(&e)
	}
	return e
}

// Ago returns a pointer to the time `d` before Now, for the optional
// timestamp fields.
func Ago(d time.Duration) *metav1.Time {
	t := metav1.NewTime(Now.Add(-d))
	return &t
}

// FakeReader is an in-memory kube.Reader that records what it was asked.
type FakeReader struct {
	CRs       []*depsv1alpha1.AppDependencies
	EventList []kube.Event
	Server    kube.ServerInfo

	// GetErr, ListErr, EventsErr and ServerErr are returned as they are.
	GetErr, ListErr, EventsErr, ServerErr error

	// Block makes every call wait until its context ends and fail with the
	// context's error, as a request to a cluster that never answers does.
	Block bool

	// Calls lists every request as "get ns/name", "list ns", "events ns/name"
	// or "server".
	Calls []string

	// Deadlines records, call by call, whether the call's context had a
	// deadline.
	Deadlines []bool
}

// record notes the call, and with Block waits for the context to end.
func (f *FakeReader) record(ctx context.Context, call string) error {
	f.Calls = append(f.Calls, call)
	_, hasDeadline := ctx.Deadline()
	f.Deadlines = append(f.Deadlines, hasDeadline)
	if !f.Block {
		return nil
	}
	<-ctx.Done()
	return fmt.Errorf("%s: %w", call, ctx.Err())
}

var _ kube.Reader = (*FakeReader)(nil)

// Get returns the CR, or an error wrapping kube.ErrNotFound.
func (f *FakeReader) Get(ctx context.Context, namespace, name string) (*depsv1alpha1.AppDependencies, error) {
	if err := f.record(ctx, fmt.Sprintf("get %s/%s", namespace, name)); err != nil {
		return nil, err
	}
	if f.GetErr != nil {
		return nil, f.GetErr
	}
	for _, cr := range f.CRs {
		if cr.Namespace == namespace && cr.Name == name {
			return cr.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("appdependencies %q not found in namespace %q: %w", name, namespace, kube.ErrNotFound)
}

// List returns the CRs of one namespace, or of all when namespace is empty.
func (f *FakeReader) List(ctx context.Context, namespace string) ([]depsv1alpha1.AppDependencies, error) {
	if err := f.record(ctx, "list "+namespace); err != nil {
		return nil, err
	}
	if f.ListErr != nil {
		return nil, f.ListErr
	}
	var out []depsv1alpha1.AppDependencies
	for _, cr := range f.CRs {
		if namespace == "" || cr.Namespace == namespace {
			out = append(out, *cr.DeepCopy())
		}
	}
	return out, nil
}

// Events returns EventList.
func (f *FakeReader) Events(ctx context.Context, namespace, name string) ([]kube.Event, error) {
	if err := f.record(ctx, fmt.Sprintf("events %s/%s", namespace, name)); err != nil {
		return nil, err
	}
	return f.EventList, f.EventsErr
}

// ServerVersions returns Server.
func (f *FakeReader) ServerVersions(ctx context.Context) (kube.ServerInfo, error) {
	if err := f.record(ctx, "server"); err != nil {
		return kube.ServerInfo{}, err
	}
	return f.Server, f.ServerErr
}
