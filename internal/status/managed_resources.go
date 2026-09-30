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

package status

import (
	"time"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// TrustWindow is how long a Verified ledger entry is trusted before it must
// be revalidated against the AWS-side ownership tag again. A stale-but-
// present ledger entry is enough to justify a read/re-check; it is never by
// itself enough to justify a mutating or destructive action once its
// window has expired.
const TrustWindow = 10 * time.Minute

// FindManagedResource returns the ledger entry for the given type/name, or
// nil if none exists.
func FindManagedResource(resources []depsv1alpha1.ManagedResource, resourceType, name string) *depsv1alpha1.ManagedResource {
	for i := range resources {
		if resources[i].Type == resourceType && resources[i].Name == name {
			return &resources[i]
		}
	}
	return nil
}

// UpsertManagedResource inserts or updates a ledger entry, matched by
// type+name.
func UpsertManagedResource(resources *[]depsv1alpha1.ManagedResource, entry depsv1alpha1.ManagedResource) {
	for i := range *resources {
		if (*resources)[i].Type == entry.Type && (*resources)[i].Name == entry.Name {
			(*resources)[i] = entry
			return
		}
	}
	*resources = append(*resources, entry)
}

// RemoveManagedResource deletes a ledger entry by type+name. No-op if absent.
func RemoveManagedResource(resources *[]depsv1alpha1.ManagedResource, resourceType, name string) {
	filtered := make([]depsv1alpha1.ManagedResource, 0, len(*resources))
	for _, r := range *resources {
		if r.Type == resourceType && r.Name == name {
			continue
		}
		filtered = append(filtered, r)
	}
	*resources = filtered
}

// NeedsRevalidation reports whether a ledger entry's trust window has
// expired (or it was never verified, or it isn't Verified in the first
// place) and must be re-checked against the AWS-side ownership tag before
// being used for any mutating action.
func NeedsRevalidation(entry depsv1alpha1.ManagedResource) bool {
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		return true
	}
	if entry.LastVerifiedAt == nil {
		return true
	}
	return time.Since(entry.LastVerifiedAt.Time) > TrustWindow
}

// EmptyCheckBackoffStartAfter is how long a resource can sit pending
// deletion before its emptiness checks start backing off at all - long
// enough that an actively-managed drain is never affected by it.
const EmptyCheckBackoffStartAfter = 24 * time.Hour

// EmptyCheckBaseInterval is the required gap once backoff starts, doubling
// once per additional elapsed day.
const EmptyCheckBaseInterval = 10 * time.Minute

// EmptyCheckMaxInterval caps how infrequently a long-neglected resource
// pending deletion gets checked, so it's never abandoned outright.
const EmptyCheckMaxInterval = 20 * time.Hour

// NeedsEmptyCheck reports whether enough time has passed to justify
// another real AWS emptiness check for a resource pending deletion. Always
// true within EmptyCheckBackoffStartAfter of PendingDeletionSince or before
// any check has run; after that, gated by the doubling interval above.
func NeedsEmptyCheck(entry depsv1alpha1.ManagedResource) bool {
	if entry.PendingDeletionSince == nil {
		return true
	}
	age := time.Since(entry.PendingDeletionSince.Time)
	if age < EmptyCheckBackoffStartAfter || entry.LastEmptyCheckAt == nil {
		return true
	}
	return time.Since(entry.LastEmptyCheckAt.Time) >= emptyCheckInterval(age)
}

func emptyCheckInterval(age time.Duration) time.Duration {
	interval := EmptyCheckBaseInterval
	for daysPastStart := int((age - EmptyCheckBackoffStartAfter) / (24 * time.Hour)); daysPastStart > 0; daysPastStart-- {
		interval *= 2
		if interval >= EmptyCheckMaxInterval {
			return EmptyCheckMaxInterval
		}
	}
	return interval
}
