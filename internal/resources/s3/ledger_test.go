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

package s3

import (
	"context"
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func TestCheckLedgerEntry_PassesWhatEnsureRecordsAndRefusesAnotherBucket(t *testing.T) {
	id := cloudctlaws.NewIdentity(testRegion, testAccountID)
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	ledger, err := Ensure(context.Background(), newFakeS3(), nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, nil)
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
		for _, forged := range []string{
			id.S3BucketARN(bucketName("payments", "billing", e.Name, testAccountID)),
			id.S3BucketARN(bucketName("default", "checkout-service", e.Name, "999999999999")),
		} {
			e.ARN = forged
			if CheckLedgerEntry(id, "default", "checkout-service", e) == nil {
				t.Errorf("bucket %s passed the check", forged)
			}
		}
	}
}
