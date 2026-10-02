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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// conditionTypeConsumeReferences is the consumer-side mirror of
// conditionTypeSharedWithReferences - deliberately not in sectionTypes,
// for the same reason: IAMReady already gates on a consumes entry that
// can't yet be resolved (see iam.skippedToError), so this condition exists
// purely to make "how long has this been true" visible, not to add a
// second gate on the same fact.
const conditionTypeConsumeReferences = "ConsumeReferencesValid"

const (
	reasonProducerNotFoundYet         = "ProducerNotFoundYet"
	reasonProducerLikelyMisconfigured = "ProducerLikelyMisconfigured"
)

// consumeReferenceDanglingThreshold is how long a consumes entry's
// producer CR can be missing before the message stops reading like an
// ordinary startup-ordering race (a GitOps apply of producer+consumer in
// the same batch, no ordering guarantee) and starts flagging itself as
// more likely a real misconfiguration (wrong namespace/name, or a producer
// that's gone for good). Long enough that normal apply/sync delays never
// trip it, short enough to still be useful to a human debugging it.
const consumeReferenceDanglingThreshold = 30 * time.Minute

type consumeRefLocation struct {
	resourceType string
	ref          depsv1alpha1.ConsumeRef
}

// checkConsumeReferences flags consumes entries across this CR's resource
// sections whose named producer CR does not currently exist. Unlike
// resolveConsume's own per-reconcile "not found yet" skip reason (which
// correctly self-resolves and retries), this walks the CR's own prior
// condition to report how long that's been true, since a producer CR name
// that will simply never exist (a typo, wrong namespace) looks otherwise
// identical, forever, to one that just hasn't been applied yet.
func checkConsumeReferences(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) {
	var dangling []string
	for _, loc := range allConsumeRefs(cr) {
		var producer depsv1alpha1.AppDependencies
		key := client.ObjectKey{Namespace: loc.ref.Namespace, Name: loc.ref.Name}
		err := k8sClient.Get(ctx, key, &producer)
		if apierrors.IsNotFound(err) {
			dangling = append(dangling, fmt.Sprintf(
				"%s consumes %s/%s's %q, whose producer CR does not currently exist",
				loc.resourceType, loc.ref.Namespace, loc.ref.Name, loc.ref.ResourceName,
			))
			continue
		}
		// Any other Get error (a transient API-server issue) is skipped,
		// same as checkSharedWithReferences - this must never report a
		// false positive from a single hiccuped Get.
	}

	if len(dangling) == 0 {
		status.SetSectionCondition(
			&cr.Status.Conditions, conditionTypeConsumeReferences, metav1.ConditionTrue,
			"AllProducersExist", "every consumes entry's producer CR currently exists",
			cr.Generation, sectionTypes,
		)
		return
	}

	reason := reasonProducerNotFoundYet
	message := strings.Join(dangling, "; ")
	if existing := apimeta.FindStatusCondition(cr.Status.Conditions, conditionTypeConsumeReferences); existing != nil &&
		existing.Status == metav1.ConditionFalse &&
		time.Since(existing.LastTransitionTime.Time) > consumeReferenceDanglingThreshold {
		reason = reasonProducerLikelyMisconfigured
		message = fmt.Sprintf("%s (unresolved for over %s - this looks like a real misconfiguration, e.g. a typo in the consumer's namespace/name, rather than a startup-ordering race)",
			message, consumeReferenceDanglingThreshold)
	}
	status.SetSectionCondition(
		&cr.Status.Conditions, conditionTypeConsumeReferences, metav1.ConditionFalse,
		reason, message, cr.Generation, sectionTypes,
	)
}

// allConsumeRefs flattens every consumes entry across every implemented
// resource section into one list.
func allConsumeRefs(cr *depsv1alpha1.AppDependencies) []consumeRefLocation {
	var refs []consumeRefLocation
	if cr.Spec.SQS != nil {
		for _, c := range cr.Spec.SQS.Consumes {
			refs = append(refs, consumeRefLocation{resourceTypeSQS, c})
		}
	}
	if cr.Spec.SNS != nil {
		for _, c := range cr.Spec.SNS.Consumes {
			refs = append(refs, consumeRefLocation{resourceTypeSNS, c})
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, c := range cr.Spec.DynamoDB.Consumes {
			refs = append(refs, consumeRefLocation{resourceTypeDynamoDB, c})
		}
	}
	if cr.Spec.S3 != nil {
		for _, c := range cr.Spec.S3.Consumes {
			refs = append(refs, consumeRefLocation{resourceTypeS3, c})
		}
	}
	return refs
}
