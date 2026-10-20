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

import (
	"context"
	"fmt"
)

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

// IsStaleUID reports whether tags carry this exact CR's own owner-name tag
// (namespace/name) but a different UID — a deleted-and-recreated CR
// reusing the same name found a resource left over from its previous
// incarnation. A name match alone is never ownership: this must be
// surfaced as a conflict and never silently adopted, even with
// adopt:true, since the resource may still be referenced elsewhere under
// its old identity.
func IsStaleUID(tags map[string]string, namespace, name, uid string) (existingUID string, isStale bool) {
	if tags[OwnerTagKey] != OwnerTagValue(namespace, name) {
		return "", false
	}
	existingUID, ok := tags[OwnerUIDTagKey]
	if !ok || existingUID == uid {
		return "", false
	}
	return existingUID, true
}

// AdoptFromUIDAnnotation, set on a CR to a previous owner UID, lets the CR
// take over the resources that UID owned, once. A recreated CR gets a new
// UID (a restore, a re-sync into a new cluster, a blue/green upgrade), and
// every resource would otherwise be refused as belonging to a stale
// incarnation. The annotation names the exact UID, so any other leftover is
// still refused.
const AdoptFromUIDAnnotation = "cloudctl.io/adopt-from-uid"

// EventOwnershipTransferred is the reason of the event recorded when a
// resource is taken over this way.
const EventOwnershipTransferred = "OwnershipTransferred"

type adoptFromUIDKey struct{}

// WithAdoptFromUID returns ctx carrying the UID the CR was told to take
// resources over from. An empty uid returns ctx unchanged.
func WithAdoptFromUID(ctx context.Context, uid string) context.Context {
	if uid == "" {
		return ctx
	}
	return context.WithValue(ctx, adoptFromUIDKey{}, uid)
}

// TakingOver reports whether the CR was told to take resources over from a
// previous UID.
func TakingOver(ctx context.Context) bool {
	from, _ := ctx.Value(adoptFromUIDKey{}).(string)
	return from != ""
}

// MayTransferFrom reports whether the CR was told to take over from exactly
// this UID. Callers pass the stale UID IsStaleUID found on a resource that
// already carries this CR's own name.
func MayTransferFrom(ctx context.Context, staleUID string) bool {
	from, _ := ctx.Value(adoptFromUIDKey{}).(string)
	return from != "" && from == staleUID
}

// CheckExistingOwnership decides what to do with a resource that exists but
// isn't tagged for this incarnation of the CR. It returns nil when the
// resource may be re-tagged, and transferred is true when that is a
// takeover from a previous UID rather than an adoption. kind is the noun used
// in the error ("queue", "bucket").
func CheckExistingOwnership(ctx context.Context, kind, name string, tags map[string]string, namespace, crName, crUID string, adopt bool) (transferred bool, err error) {
	if owner, ok := tags[OwnerTagKey]; ok && owner != OwnerTagValue(namespace, crName) {
		return false, fmt.Errorf("%s %q is already owned by a different AppDependencies CR (%s) — this looks like a naming collision, not adopting", kind, name, owner)
	}
	if staleUID, stale := IsStaleUID(tags, namespace, crName, crUID); stale {
		if !MayTransferFrom(ctx, staleUID) {
			return false, fmt.Errorf("%s %q is tagged with this CR's name but a different UID (%s) — likely a stale resource from a deleted-and-recreated CR, refusing to adopt automatically; to take it over, annotate this CR with %s=%s", kind, name, staleUID, AdoptFromUIDAnnotation, staleUID)
		}
		return true, nil
	}
	if !adopt {
		return false, fmt.Errorf("%s %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", kind, name)
	}
	return false, nil
}
