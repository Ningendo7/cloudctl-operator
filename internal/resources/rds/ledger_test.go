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

package rds

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func TestCheckLedgerEntry_PassesWhatEnsureRecordsAndRefusesAnotherInstance(t *testing.T) {
	id := cloudctlaws.NewIdentity("us-east-1", "123456789012")
	grant := newAuthorizedSubnetGroupGrant("prod-private-data-tier", "default")
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant).Build()
	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{baseInstanceSpec("orders-db")}}
	ledger, err := Ensure(context.Background(), newFakeRDS(), kmsWithRDSKey(), newFakeEC2(), k8sClient, "default", "checkout-service", "uid-1", id.Region, id.AccountID, spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	checks := map[string]func(cloudctlaws.Identity, string, string, depsv1alpha1.ManagedResource) error{
		resourceType:              CheckLedgerEntry,
		SecurityGroupResourceType: CheckSecurityGroupEntry,
	}
	seen := map[string]bool{}
	for _, e := range ledger {
		check, ok := checks[e.Type]
		if !ok || e.ARN == "" {
			continue
		}
		seen[e.Type] = true
		if err := check(id, "default", "checkout-service", e); err != nil {
			t.Errorf("the entry Ensure recorded fails the check: %v", err)
		}
	}
	for typ := range checks {
		if !seen[typ] {
			t.Errorf("Ensure recorded no %s entry; the test no longer covers it", typ)
		}
	}

	forged := depsv1alpha1.ManagedResource{Type: resourceType, Name: "orders-db", ARN: id.RegionalARN("rds", "db:"+instanceIdentifier("payments", "billing", "orders-db"))}
	if CheckLedgerEntry(id, "default", "checkout-service", forged) == nil {
		t.Errorf("another CR's instance %s passed the check", forged.ARN)
	}
}
