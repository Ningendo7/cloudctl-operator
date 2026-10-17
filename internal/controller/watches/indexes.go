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

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// Field indexes on AppDependencies, so each watch asks the cache for only
// the CRs it needs instead of listing every CR on every event.
const (
	// IndexConsumesFrom holds "namespace/name" of every producer CR a CR
	// consumes from, subscribes to or notifies.
	IndexConsumesFrom = "cloudctl.io/consumes-from"
	// IndexRDSSubnetGroup holds the dbSubnetGroupName of each rds resource.
	IndexRDSSubnetGroup = "cloudctl.io/rds-subnet-group"
	// IndexRDSSharedWith holds "namespace/name" of every CR an rds resource
	// is shared with.
	IndexRDSSharedWith = "cloudctl.io/rds-shared-with"
)

// Indexes maps each index name to its extractor. The controller registers
// them on the manager; tests build fake clients with the same set.
var Indexes = map[string]client.IndexerFunc{
	IndexConsumesFrom:   indexConsumesFrom,
	IndexRDSSubnetGroup: indexRDSSubnetGroup,
	IndexRDSSharedWith:  indexRDSSharedWith,
}

// RegisterIndexes adds every index to the manager's cache.
func RegisterIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	for field, extract := range Indexes {
		if err := indexer.IndexField(ctx, &depsv1alpha1.AppDependencies{}, field, extract); err != nil {
			return err
		}
	}
	return nil
}

// NamespacedKey is the "namespace/name" form the indexes use.
func NamespacedKey(namespace, name string) string {
	return namespace + "/" + name
}

func indexConsumesFrom(obj client.Object) []string {
	cr, ok := obj.(*depsv1alpha1.AppDependencies)
	if !ok {
		return nil
	}
	var keys []string
	add := func(refs []depsv1alpha1.ConsumeRef) {
		for _, ref := range refs {
			keys = append(keys, NamespacedKey(ref.Namespace, ref.Name))
		}
	}
	if cr.Spec.SQS != nil {
		add(cr.Spec.SQS.Consumes)
		// Otherwise nothing wakes a queue whose topic doesn't exist yet.
		for _, q := range cr.Spec.SQS.Resources {
			add(q.SubscribesTo)
		}
	}
	if cr.Spec.Alarms != nil && cr.Spec.Alarms.SnsTopicRef != nil {
		add([]depsv1alpha1.ConsumeRef{*cr.Spec.Alarms.SnsTopicRef})
	}
	if cr.Spec.SNS != nil {
		add(cr.Spec.SNS.Consumes)
	}
	if cr.Spec.DynamoDB != nil {
		add(cr.Spec.DynamoDB.Consumes)
	}
	if cr.Spec.S3 != nil {
		add(cr.Spec.S3.Consumes)
	}
	if cr.Spec.RDS != nil {
		add(cr.Spec.RDS.Consumes)
	}
	return keys
}

func indexRDSSubnetGroup(obj client.Object) []string {
	cr, ok := obj.(*depsv1alpha1.AppDependencies)
	if !ok || cr.Spec.RDS == nil {
		return nil
	}
	groups := make([]string, 0, len(cr.Spec.RDS.Resources))
	for _, r := range cr.Spec.RDS.Resources {
		groups = append(groups, r.DBSubnetGroupName)
	}
	return groups
}

func indexRDSSharedWith(obj client.Object) []string {
	cr, ok := obj.(*depsv1alpha1.AppDependencies)
	if !ok || cr.Spec.RDS == nil {
		return nil
	}
	var keys []string
	for _, r := range cr.Spec.RDS.Resources {
		for _, sw := range r.SharedWith {
			keys = append(keys, NamespacedKey(sw.Namespace, sw.Name))
		}
	}
	return keys
}
