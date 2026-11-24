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
	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// CheckLedgerEntry refuses a key entry outside this account and region. A key
// ID is AWS's, so whose key it is is checked through its alias where it is
// used.
func CheckLedgerEntry(id cloudctlaws.Identity, _, _ string, entry depsv1alpha1.ManagedResource) error {
	return cloudctlaws.CheckLedgerARNPrefix(entry.Type, entry.Name, entry.ARN, id.RegionalARN("kms", "key/"))
}
