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
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// conditionTypeSharedWithReferences is deliberately not in sectionTypes —
// a stale reference is a hygiene signal, not a reconcile failure, so it
// must never affect the aggregate Ready condition.
const conditionTypeSharedWithReferences = "SharedWithReferencesValid"

type sharedWithRef struct {
	resourceType, resourceName string
	entry                      depsv1alpha1.SharedWithEntry
}

// checkSharedWithReferences flags sharedWith entries across this CR's owned
// resources whose target consumer CR no longer exists — most commonly
// because that CR was deleted sometime after being granted access.
//
// Deliberately status-only, never touching spec: sharedWith is
// user-authored, and this operator has never mutated spec anywhere else.
// Silently pruning a stale entry would just fight a GitOps-managed
// manifest on its next sync (delete it here, GitOps restores it, delete it
// again next reconcile) rather than actually resolving anything. This
// makes the otherwise-invisible state visible and lets a human decide,
// instead of picking a destructive action on their behalf.
//
// Deliberately not an error and not a hard failure: a grant referencing a
// gone CR grants nothing (resolveConsume in the iam package already
// requires the referenced CR to currently exist), so this is a cleanliness
// signal, not a correctness or security problem on its own — see it as the
// project's own note on this same tradeoff for what would make it one
// (recreating an unrelated CR under the exact same namespace+name later).
func checkSharedWithReferences(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) {
	var stale []string
	for _, ref := range allSharedWithRefs(cr) {
		var consumer depsv1alpha1.AppDependencies
		key := client.ObjectKey{Namespace: ref.entry.Namespace, Name: ref.entry.Name}
		err := k8sClient.Get(ctx, key, &consumer)
		if apierrors.IsNotFound(err) {
			stale = append(stale, fmt.Sprintf(
				"%s %q shares with %s/%s, which no longer exists",
				ref.resourceType, ref.resourceName, ref.entry.Namespace, ref.entry.Name,
			))
			continue
		}
		// Any other Get error (a transient API-server issue) is skipped
		// rather than flagged — this check must never report a false
		// positive just because a single Get hiccuped.
	}

	if len(stale) == 0 {
		status.SetSectionCondition(
			&cr.Status.Conditions, conditionTypeSharedWithReferences, metav1.ConditionTrue,
			"AllReferencesValid", "every sharedWith entry resolves to an existing AppDependencies CR",
			cr.Generation, sectionTypes,
		)
		return
	}
	status.SetSectionCondition(
		&cr.Status.Conditions, conditionTypeSharedWithReferences, metav1.ConditionFalse,
		"StaleReferencesFound", strings.Join(stale, "; "),
		cr.Generation, sectionTypes,
	)
}

// allSharedWithRefs flattens every sharedWith entry across every owned
// resource in every implemented section into one list.
func allSharedWithRefs(cr *depsv1alpha1.AppDependencies) []sharedWithRef {
	var refs []sharedWithRef
	if cr.Spec.SQS != nil {
		for _, q := range cr.Spec.SQS.Resources {
			for _, e := range q.SharedWith {
				refs = append(refs, sharedWithRef{"sqs", q.Name, e})
			}
		}
	}
	if cr.Spec.SNS != nil {
		for _, t := range cr.Spec.SNS.Resources {
			for _, e := range t.SharedWith {
				refs = append(refs, sharedWithRef{"sns", t.Name, e})
			}
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, tbl := range cr.Spec.DynamoDB.Resources {
			for _, e := range tbl.SharedWith {
				refs = append(refs, sharedWithRef{"dynamodb", tbl.Name, e})
			}
		}
	}
	if cr.Spec.S3 != nil {
		for _, b := range cr.Spec.S3.Resources {
			for _, e := range b.SharedWith {
				refs = append(refs, sharedWithRef{"s3", b.Name, e})
			}
		}
	}
	return refs
}
