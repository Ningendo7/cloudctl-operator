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

package aws

import "fmt"

// ResourceName derives a deterministic AWS resource name from an
// AppDependencies CR's namespace/name and a resource's spec-level key. Safe
// to recompute at any time — nothing about ownership tracking depends on
// persisted random state.
//
// S3 needs its own variant (in the s3 resource package, not here) since
// bucket names are unique across every AWS account globally, not just this
// one, and need an account-id-derived suffix to avoid colliding with an
// unrelated AWS customer.
func ResourceName(namespace, crName, resourceKey string) string {
	return fmt.Sprintf("%s-%s-%s", namespace, crName, resourceKey)
}
