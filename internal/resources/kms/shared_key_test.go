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

package kms

import (
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func TestSharedKeyARN_ResolvesAKeyInThisCRsLedger(t *testing.T) {
	ledger := []depsv1alpha1.ManagedResource{
		{Type: resourceType, Name: "shared-key", ARN: "arn:aws:kms:us-east-1:123456789012:key/shared-id"},
	}

	arn, ok := SharedKeyARN(ledger, "shared-key")
	if !ok || arn != "arn:aws:kms:us-east-1:123456789012:key/shared-id" {
		t.Errorf("SharedKeyARN() = (%q, %v), want the key's ARN", arn, ok)
	}
}

func TestSharedKeyARN_NotYetResolvable(t *testing.T) {
	tests := map[string][]depsv1alpha1.ManagedResource{
		"not in the ledger yet":                      nil,
		"created without an ARN":                     {{Type: resourceType, Name: "shared-key"}},
		"a different resource type of the same name": {{Type: "sqs", Name: "shared-key", ARN: "arn:aws:sqs:us-east-1:123456789012:q"}},
	}
	for name, ledger := range tests {
		t.Run(name, func(t *testing.T) {
			if arn, ok := SharedKeyARN(ledger, "shared-key"); ok {
				t.Errorf("SharedKeyARN() = (%q, true), want not resolvable", arn)
			}
		})
	}
}
