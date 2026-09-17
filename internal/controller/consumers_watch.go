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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// mapProducerToConsumers maps a change on one AppDependencies CR (the
// "producer" side - its sharedWith grants, or the resources it owns at
// all) to a reconcile request for every OTHER AppDependencies CR that
// currently consumes from it. Without this, revoking (or granting) a
// sharedWith entry only takes effect on the consumer's next periodic
// drift-detection reconcile - up to DriftDetectionInterval later - instead
// of immediately, which matters for a security-relevant IAM grant.
//
// Deliberately doesn't try to detect exactly what changed (sharedWith vs.
// something unrelated) - a MapFunc only sees the current object, not old
// vs new, so that distinction isn't available here without a heavier
// custom event handler. Re-mapping on every producer update means a
// consumer occasionally gets an extra, no-op reconcile when the producer
// changed something that doesn't actually affect it - harmless, since
// reconciles are idempotent - versus the alternative of missing a real
// sharedWith change, which is a staleness window on an access-control
// decision, not just an efficiency one.
//
// Lists every AppDependencies CR cluster-wide on every producer event -
// fine at the scale this operator targets (one CR per app team), but
// doesn't scale indefinitely; revisit with a client.IndexField on a
// computed "consumes" key if this ever shows up as a real cost.
func (r *AppDependenciesReconciler) mapProducerToConsumers(ctx context.Context, producer client.Object) []reconcile.Request {
	producerNS, producerName := producer.GetNamespace(), producer.GetName()

	var all depsv1alpha1.AppDependenciesList
	if err := r.List(ctx, &all); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for i := range all.Items {
		consumer := &all.Items[i]
		if consumer.Namespace == producerNS && consumer.Name == producerName {
			continue // never need to map a CR to itself
		}
		if consumesFrom(consumer, producerNS, producerName) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(consumer),
			})
		}
	}
	return requests
}

// consumesFrom reports whether consumer has any consumes entry, in any
// section, naming the given producer.
func consumesFrom(consumer *depsv1alpha1.AppDependencies, producerNS, producerName string) bool {
	refsInclude := func(refs []depsv1alpha1.ConsumeRef) bool {
		for _, ref := range refs {
			if ref.Namespace == producerNS && ref.Name == producerName {
				return true
			}
		}
		return false
	}
	if consumer.Spec.SQS != nil && refsInclude(consumer.Spec.SQS.Consumes) {
		return true
	}
	if consumer.Spec.SNS != nil && refsInclude(consumer.Spec.SNS.Consumes) {
		return true
	}
	if consumer.Spec.DynamoDB != nil && refsInclude(consumer.Spec.DynamoDB.Consumes) {
		return true
	}
	if consumer.Spec.S3 != nil && refsInclude(consumer.Spec.S3.Consumes) {
		return true
	}
	return false
}
