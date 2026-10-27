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

package watches

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// ConsumerPodIdentityToRDSProducers maps a change on one AppDependencies
// CR (the "consumer" side - its own published pod-network-identity) to a
// reconcile request for every RDS-owning producer that lists it in
// sharedWith. Without this, a newly-published (or removed) identity only
// takes effect once the producer's own spec changes or its drift timer
// fires - up to DriftDetectionInterval later. Paired with
// predicates.PodNetworkIdentityPublishedPredicate, which keeps this from
// firing on unrelated status churn.
//
// Looks producers up through IndexRDSSharedWith rather than listing every CR.
func ConsumerPodIdentityToRDSProducers(reader client.Reader) handler.MapFunc {
	return func(ctx context.Context, consumer client.Object) []reconcile.Request {
		consumerNS, consumerName := consumer.GetNamespace(), consumer.GetName()

		var producers depsv1alpha1.AppDependenciesList
		// Read-only, so the cache needn't copy what it returns.
		if err := reader.List(ctx, &producers, client.MatchingFields{IndexRDSSharedWith: NamespacedKey(consumerNS, consumerName)}, client.UnsafeDisableDeepCopy); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to list the producers that share RDS with a changed consumer", "consumer", consumerNS+"/"+consumerName)
			return nil
		}

		var requests []reconcile.Request
		for i := range producers.Items {
			producer := &producers.Items[i]
			if producer.Namespace == consumerNS && producer.Name == consumerName {
				continue
			}
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(producer)})
		}
		return requests
	}
}
