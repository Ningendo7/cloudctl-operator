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

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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

func withLedger(entries ...depsv1alpha1.ManagedResource) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: entries}}
}

func TestProducerChangedPredicate_UpdateFunc(t *testing.T) {
	pred := ProducerChangedPredicate()
	queue := depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:orders"}
	verified := queue
	verified.State = depsv1alpha1.ManagedResourceStateVerified
	now := metav1.Now()
	verified.LastVerifiedAt = &now

	tests := []struct {
		name     string
		old, new *depsv1alpha1.AppDependencies
		want     bool
	}{
		{name: "ARN appears", old: withLedger(), new: withLedger(queue), want: true},
		{name: "ARN disappears", old: withLedger(queue), new: withLedger(), want: true},
		{name: "ARN changes", old: withLedger(queue), new: withLedger(depsv1alpha1.ManagedResource{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:other"}), want: true},
		{name: "state and timestamps only", old: withLedger(queue), new: withLedger(verified), want: false},
		{name: "no ledger change", old: withLedger(queue), new: withLedger(queue), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pred.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProducerChangedPredicate_StillPassesSpecChanges(t *testing.T) {
	old := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
	updated := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	if !ProducerChangedPredicate().Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Error("expected a generation bump to still pass")
	}
}

func crWithAnnotations(annotations map[string]string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Generation: 1, Annotations: annotations}}
}

// The annotation is how a person hands a CR its previous incarnation's
// resources; setting it has to trigger a reconcile, not wait for the next
// drift pass.
func TestAppDependenciesPredicate_ReactsToTheAdoptFromAnnotation(t *testing.T) {
	pred := AppDependenciesPredicate()
	for name, tc := range map[string]struct {
		old, new map[string]string
		want     bool
	}{
		"annotation added":       {nil, map[string]string{cloudctlaws.AdoptFromUIDAnnotation: "old-uid"}, true},
		"annotation changed":     {map[string]string{cloudctlaws.AdoptFromUIDAnnotation: "a"}, map[string]string{cloudctlaws.AdoptFromUIDAnnotation: "b"}, true},
		"annotation removed":     {map[string]string{cloudctlaws.AdoptFromUIDAnnotation: "a"}, nil, false},
		"unrelated annotation":   {nil, map[string]string{"example.com/x": "1"}, false},
		"force-reconcile bumped": {nil, map[string]string{ForceReconcileAnnotation: "1"}, true},
		"force-delete-all added": {nil, map[string]string{ForceDeleteAllAnnotation: "true"}, true},
		"force-delete-all taken off": {
			map[string]string{ForceDeleteAllAnnotation: "true"}, nil, false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := pred.Update(event.UpdateEvent{ObjectOld: crWithAnnotations(tc.old), ObjectNew: crWithAnnotations(tc.new)})
			if got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}

func consumerOf(deleting bool, consumes ...depsv1alpha1.ConsumeRef) *depsv1alpha1.AppDependencies {
	cr := &depsv1alpha1.AppDependencies{}
	if deleting {
		now := metav1.Now()
		cr.DeletionTimestamp = &now
	}
	if consumes != nil {
		cr.Spec.RDS = &depsv1alpha1.RDSSpec{Consumes: consumes}
	}
	return cr
}

func TestPodNetworkIdentityPublishedPredicate_WakesProducerWhenConsumerStopsBeingEntitled(t *testing.T) {
	pred := PodNetworkIdentityPublishedPredicate()
	orders := depsv1alpha1.ConsumeRef{Namespace: "shop", Name: "checkout", ResourceName: "orders-db"}
	billing := depsv1alpha1.ConsumeRef{Namespace: "shop", Name: "checkout", ResourceName: "billing-db"}

	for name, tc := range map[string]struct {
		old, new *depsv1alpha1.AppDependencies
		want     bool
	}{
		"deletion starts":             {consumerOf(false, orders), consumerOf(true, orders), true},
		"rds.consumes emptied":        {consumerOf(false, orders), consumerOf(false), true},
		"rds.consumes entry swapped":  {consumerOf(false, orders), consumerOf(false, billing), true},
		"rds.consumes entry added":    {consumerOf(false, orders), consumerOf(false, orders, billing), true},
		"nothing changed":             {consumerOf(false, orders), consumerOf(false, orders), false},
		"already deleting, unchanged": {consumerOf(true, orders), consumerOf(true, orders), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := pred.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}
