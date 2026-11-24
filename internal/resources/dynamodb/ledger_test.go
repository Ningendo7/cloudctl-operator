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

package dynamodb

import (
	"context"
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func TestCheckLedgerEntry_PassesWhatEnsureRecordsAndRefusesAnotherTable(t *testing.T) {
	id := cloudctlaws.NewIdentity("us-east-1", "123456789012")
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{Name: "sessions", PartitionKey: "id"}}}
	ledger, err := Ensure(context.Background(), newFakeDynamoDB(), nil, nil, "default", "checkout-service", "uid-1", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if len(ledger) == 0 {
		t.Fatal("Ensure() recorded nothing")
	}
	for _, e := range ledger {
		if err := CheckLedgerEntry(id, "default", "checkout-service", e); err != nil {
			t.Errorf("the entry Ensure recorded fails the check: %v", err)
		}
		e.ARN = id.RegionalARN("dynamodb", "table/"+cloudctlaws.TableName("payments", "billing", e.Name))
		if CheckLedgerEntry(id, "default", "checkout-service", e) == nil {
			t.Errorf("another CR's table %s passed the check", e.ARN)
		}
	}
}
