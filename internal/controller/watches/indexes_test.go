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
	"slices"
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func TestIndexConsumesFrom_CoversEverySection(t *testing.T) {
	ref := func(ns, name string) []depsv1alpha1.ConsumeRef {
		return []depsv1alpha1.ConsumeRef{{Namespace: ns, Name: name, ResourceName: "r"}}
	}
	cr := &depsv1alpha1.AppDependencies{Spec: depsv1alpha1.AppDependenciesSpec{
		SQS:      &depsv1alpha1.SQSSpec{Consumes: ref("a", "sqs-producer")},
		SNS:      &depsv1alpha1.SNSSpec{Consumes: ref("a", "sns-producer")},
		DynamoDB: &depsv1alpha1.DynamoDBSpec{Consumes: ref("b", "dynamo-producer")},
		S3:       &depsv1alpha1.S3Spec{Consumes: ref("b", "s3-producer")},
		RDS:      &depsv1alpha1.RDSSpec{Consumes: ref("c", "rds-producer")},
	}}

	got := indexConsumesFrom(cr)
	for _, want := range []string{"a/sqs-producer", "a/sns-producer", "b/dynamo-producer", "b/s3-producer", "c/rds-producer"} {
		if !slices.Contains(got, want) {
			t.Errorf("indexConsumesFrom() = %v, missing %q", got, want)
		}
	}
	if len(got) != 5 {
		t.Errorf("expected exactly 5 keys, got %v", got)
	}
}

func TestRDSIndexes(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{
		Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders", DBSubnetGroupName: "prod-tier", SharedWith: []depsv1alpha1.SharedWithEntry{
				{Namespace: "fulfillment", Name: "fulfillment-service"},
				{Namespace: "billing", Name: "invoices"},
			}},
			{Name: "sessions", DBSubnetGroupName: "staging-tier"},
		},
	}}}

	if got := indexRDSSubnetGroup(cr); !slices.Equal(got, []string{"prod-tier", "staging-tier"}) {
		t.Errorf("indexRDSSubnetGroup() = %v", got)
	}
	if got := indexRDSSharedWith(cr); !slices.Equal(got, []string{"fulfillment/fulfillment-service", "billing/invoices"}) {
		t.Errorf("indexRDSSharedWith() = %v", got)
	}
}

func TestIndexes_IgnoreCRsWithNothingToIndex(t *testing.T) {
	empty := &depsv1alpha1.AppDependencies{}
	for name, extract := range Indexes {
		if got := extract(empty); len(got) != 0 {
			t.Errorf("index %s returned %v for an empty CR", name, got)
		}
	}
}
