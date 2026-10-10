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

package predicates

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func withPodIdentityARN(arn string) *depsv1alpha1.AppDependencies {
	cr := &depsv1alpha1.AppDependencies{}
	if arn != "" {
		cr.Status.ManagedResources = []depsv1alpha1.ManagedResource{
			{Type: "ec2-securitygroup", Name: "pod-network-identity", ARN: arn},
		}
	}
	return cr
}

func TestPodNetworkIdentityPublishedPredicate_UpdateFunc(t *testing.T) {
	pred := PodNetworkIdentityPublishedPredicate()

	tests := []struct {
		name    string
		oldARN  string
		newARN  string
		wantOut bool
	}{
		{name: "newly published", oldARN: "", newARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1", wantOut: true},
		{name: "removed", oldARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1", newARN: "", wantOut: true},
		{name: "changed", oldARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1", newARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-2", wantOut: true},
		{name: "unchanged, both set", oldARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1", newARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1", wantOut: false},
		{name: "unchanged, both empty", oldARN: "", newARN: "", wantOut: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := event.UpdateEvent{ObjectOld: withPodIdentityARN(tc.oldARN), ObjectNew: withPodIdentityARN(tc.newARN)}
			if got := pred.Update(e); got != tc.wantOut {
				t.Errorf("Update() = %v, want %v", got, tc.wantOut)
			}
		})
	}
}

func TestPodNetworkIdentityPublishedPredicate_IgnoresUnrelatedStatusChurn(t *testing.T) {
	pred := PodNetworkIdentityPublishedPredicate()
	old := &depsv1alpha1.AppDependencies{Status: depsv1alpha1.AppDependenciesStatus{
		ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: "ec2-securitygroup", Name: "pod-network-identity", ARN: "arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
			{Type: "rds", Name: "orders-db", ARN: "arn:aws:rds:us-east-1:123456789012:db:orders-db", State: depsv1alpha1.ManagedResourceStateVerified},
		},
	}}
	new := old.DeepCopy()
	// An unrelated ledger entry's own trust-window re-verification bumping
	// LastVerifiedAt - must not be mistaken for the pod identity changing.
	now := metav1.Now()
	new.Status.ManagedResources[1].LastVerifiedAt = &now

	if got := pred.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: new}); got {
		t.Error("expected false for a change to an unrelated ledger entry")
	}
}

func TestPodNetworkIdentityPublishedPredicate_CreateDeleteGeneric(t *testing.T) {
	pred := PodNetworkIdentityPublishedPredicate()
	if !pred.Create(event.CreateEvent{Object: withPodIdentityARN("")}) {
		t.Error("expected Create to always let the event through")
	}
	if !pred.Delete(event.DeleteEvent{Object: withPodIdentityARN("arn:aws:ec2:us-east-1:123456789012:security-group/sg-1")}) {
		t.Error("expected Delete to always let the event through, so producers revoke access promptly")
	}
	if pred.Generic(event.GenericEvent{Object: withPodIdentityARN("")}) {
		t.Error("expected Generic events to be filtered out")
	}
}

func TestPodNetworkIdentityPublishedPredicate_IgnoresNonAppDependenciesObjects(t *testing.T) {
	pred := PodNetworkIdentityPublishedPredicate()
	e := event.UpdateEvent{ObjectOld: &depsv1alpha1.RDSSubnetGroupGrant{}, ObjectNew: &depsv1alpha1.RDSSubnetGroupGrant{}}
	if pred.Update(e) {
		t.Error("expected false for objects that aren't *AppDependencies")
	}
}
