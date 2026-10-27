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

// SubnetGroupGrantToAffectedCRs maps a change on one RDSSubnetGroupGrant
// (created, updated to add/remove a namespace, or deleted) to a reconcile
// request for every AppDependencies CR declaring an rds resource whose
// dbSubnetGroupName matches it. Without this, a CR already blocked on
// SubnetGroupNotAuthorized would stay blocked until its next periodic
// drift-detection reconcile instead of unblocking the moment a platform
// admin grants it - the same immediacy ProducerToConsumers already gives
// sharedWith changes, just for this cluster-scoped grant instead of
// another AppDependencies CR.
//
// Looks CRs up through IndexRDSSubnetGroup rather than listing them all.
func SubnetGroupGrantToAffectedCRs(reader client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		grant, ok := obj.(*depsv1alpha1.RDSSubnetGroupGrant)
		if !ok {
			return nil
		}

		var affected depsv1alpha1.AppDependenciesList
		// Read-only, so the cache needn't copy what it returns.
		if err := reader.List(ctx, &affected, client.MatchingFields{IndexRDSSubnetGroup: grant.Spec.DBSubnetGroupName}, client.UnsafeDisableDeepCopy); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to list the CRs affected by a changed subnet group grant", "subnetGroup", grant.Spec.DBSubnetGroupName)
			return nil
		}

		var requests []reconcile.Request
		for i := range affected.Items {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&affected.Items[i])})
		}
		return requests
	}
}
