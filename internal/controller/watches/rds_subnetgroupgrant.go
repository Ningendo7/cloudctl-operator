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
// Lists every AppDependencies CR cluster-wide on every grant event - see
// ProducerToConsumers for why that's an accepted tradeoff at this
// project's target scale.
func SubnetGroupGrantToAffectedCRs(reader client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		grant, ok := obj.(*depsv1alpha1.RDSSubnetGroupGrant)
		if !ok {
			return nil
		}

		var all depsv1alpha1.AppDependenciesList
		if err := reader.List(ctx, &all); err != nil {
			return nil
		}

		var requests []reconcile.Request
		for i := range all.Items {
			cr := &all.Items[i]
			if declaresSubnetGroup(cr, grant.Spec.DBSubnetGroupName) {
				requests = append(requests, reconcile.Request{
					NamespacedName: client.ObjectKeyFromObject(cr),
				})
			}
		}
		return requests
	}
}

// declaresSubnetGroup reports whether cr declares any rds resource
// referencing dbSubnetGroupName.
func declaresSubnetGroup(cr *depsv1alpha1.AppDependencies, dbSubnetGroupName string) bool {
	if cr.Spec.RDS == nil {
		return false
	}
	for _, res := range cr.Spec.RDS.Resources {
		if res.DBSubnetGroupName == dbSubnetGroupName {
			return true
		}
	}
	return false
}
