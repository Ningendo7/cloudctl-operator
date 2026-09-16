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

const (
	// OwnerTagKey identifies which AppDependencies CR owns a resource, as
	// "<namespace>/<name>".
	OwnerTagKey = "cloudctl.io/owner"
	// OwnerUIDTagKey carries the owning CR's UID, so a deleted-and-recreated
	// CR with the same name doesn't get confused with a stale orphan.
	OwnerUIDTagKey = "cloudctl.io/owner-uid"
)

// OwnerTagValue formats the value stored under OwnerTagKey.
func OwnerTagValue(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// IsOwnedBy reports whether a resource's tags show it's owned by the given
// CR (matched on both namespace/name and UID). A resource with no ownership
// tags, or tags belonging to a different CR, is not owned by us — it must
// not be adopted, mutated, or deleted without an explicit adopt:true.
func IsOwnedBy(tags map[string]string, namespace, name, uid string) bool {
	return tags[OwnerTagKey] == OwnerTagValue(namespace, name) && tags[OwnerUIDTagKey] == uid
}
