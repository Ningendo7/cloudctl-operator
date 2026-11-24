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

package iam

import (
	"strings"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// CheckLedgerEntry refuses a role entry that isn't this CR's role. The role
// may sit under any path, since --iam-role-path applies to new roles only.
func CheckLedgerEntry(id cloudctlaws.Identity, namespace, crName string, entry depsv1alpha1.ManagedResource) error {
	name := roleName(namespace, crName)
	prefix := id.GlobalARN("iam", "role/")
	if entry.Name == roleLedgerName && strings.HasPrefix(entry.ARN, prefix) && strings.HasSuffix(entry.ARN, "/"+name) {
		return nil
	}
	return &cloudctlaws.LedgerMismatchError{Type: entry.Type, Name: entry.Name, ARN: entry.ARN, Want: prefix + "[path/]" + name}
}
