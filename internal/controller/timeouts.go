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

package controller

import (
	"context"
	"time"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

const (
	// baseSectionTimeout covers a section's fixed overhead even with zero
	// declared resources of that type - Cleanup still has to run to check
	// for orphaned ledger entries.
	baseSectionTimeout = 15 * time.Second

	// perResourceTimeout is added once per resource a section could touch.
	// Sized generously against each resource type's actual worst-case call
	// count (SQS's DLQ+FIFO+adopt path, SNS's simpler no-DLQ path,
	// DynamoDB's multi-call attribute reconciliation) - all land within the
	// same order of magnitude at any given resource count, which is why
	// this is one shared constant rather than a different one per type.
	perResourceTimeout = 3 * time.Second
)

// sectionContext bounds a section's reconcile/finalize call so a hung AWS
// request fails as a normal, retryable timeout instead of blocking this
// CR's reconcile - and this worker's queue slot - indefinitely.
//
// The timeout scales with how much work this section could actually do:
// the larger of declaredCount (what Ensure processes, driven by spec) and
// the current ledger count for resourceType (what Cleanup's ledger-diff
// processes - during finalize in particular, nothing counts as "declared"
// at all, so the ledger count is the only signal of real work there).
// Scaling on resource count rather than a flat number avoids either
// starving a large CR of the time it legitimately needs or making every
// small CR wait out a worst-case timeout before a genuine hang is caught.
func sectionContext(ctx context.Context, ledger []depsv1alpha1.ManagedResource, resourceType string, declaredCount int) (context.Context, context.CancelFunc) {
	ledgerCount := 0
	for _, e := range ledger {
		if e.Type == resourceType {
			ledgerCount++
		}
	}
	count := declaredCount
	if ledgerCount > count {
		count = ledgerCount
	}
	timeout := baseSectionTimeout + time.Duration(count)*perResourceTimeout
	return context.WithTimeout(ctx, timeout)
}
