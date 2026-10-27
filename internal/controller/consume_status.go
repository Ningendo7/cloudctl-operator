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
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// conditionTypeConsumeReferences is the consumer-side mirror of
// conditionTypeSharedWithReferences - deliberately not in sectionTypes,
// for the same reason: IAMReady already gates on a consumes entry that
// can't yet be resolved (see iam.skippedToError), so this condition exists
// purely to make "how long has this been true" visible, not to add a
// second gate on the same fact.
const conditionTypeConsumeReferences = "ConsumeReferencesValid"

// consumeReferenceDanglingThreshold is how long a consumes entry's
// producer CR can be missing before the message stops reading like an
// ordinary startup-ordering race (a GitOps apply of producer+consumer in
// the same batch, no ordering guarantee) and starts flagging itself as
// more likely a real misconfiguration (wrong namespace/name, or a producer
// that's gone for good). Long enough that normal apply/sync delays never
// trip it, short enough to still be useful to a human debugging it.
const consumeReferenceDanglingThreshold = 30 * time.Minute

// consumeRefLocation is one reference to another CR's resource: the type of
// resource it points at, the spec field it is written in (for the message),
// and the reference itself.
type consumeRefLocation struct {
	resourceType string
	field        string
	ref          depsv1alpha1.ConsumeRef
}

// checkConsumeReferences flags references from this CR to another CR's
// resources that do not currently resolve: the CR does not exist, does not
// declare the resource, or has not listed this CR in sharedWith. Unlike the
// per-reconcile skip each section applies (which correctly self-resolves and
// retries), this walks the CR's own prior condition to report how long a
// missing producer has been missing, since a CR name that will simply never
// exist (a typo, wrong namespace) looks otherwise identical, forever, to one
// that just hasn't been applied yet.
//
// It asks the same question IAM policy derivation asks (iam.CheckConsumeReference),
// so a reference reported as resolved is one that is actually granted. A
// failed read is reported as unknown, never as resolved.
func checkConsumeReferences(ctx context.Context, k8sClient client.Client, cr *depsv1alpha1.AppDependencies) {
	var missing, undeclared, unauthorized, unreadable []string
	for _, loc := range allConsumeRefs(cr) {
		problem, err := iam.CheckConsumeReference(ctx, k8sClient, cr, loc.resourceType, loc.ref)
		target := fmt.Sprintf("%s %s/%s's %q", loc.field, loc.ref.Namespace, loc.ref.Name, loc.ref.ResourceName)
		switch {
		case err != nil:
			unreadable = append(unreadable, fmt.Sprintf("%s: %v", target, err))
		case problem == iam.ReferenceProducerNotFound:
			missing = append(missing, target+": that CR does not currently exist")
		case problem == iam.ReferenceResourceNotDeclared:
			undeclared = append(undeclared, target+": that CR does not declare it")
		case problem == iam.ReferenceNotAuthorized:
			unauthorized = append(unauthorized, target+": that CR has not listed this CR in sharedWith")
		}
	}

	problems := append(append(append([]string{}, unauthorized...), undeclared...), missing...)
	switch {
	case len(problems) == 0 && len(unreadable) == 0:
		status.SetSectionCondition(
			&cr.Status.Conditions, conditionTypeConsumeReferences, metav1.ConditionTrue,
			reasons.AllProducersExist, "every reference to another CR's resource resolves",
			cr.Generation, sectionTypes,
		)
	case len(problems) == 0:
		status.SetSectionCondition(
			&cr.Status.Conditions, conditionTypeConsumeReferences, metav1.ConditionUnknown,
			reasons.ReferencesUnknown, status.JoinCapped(unreadable, "; "),
			cr.Generation, sectionTypes,
		)
	default:
		reason, message := consumeProblemReason(cr, unauthorized, undeclared, status.JoinCapped(append(problems, unreadable...), "; "))
		status.SetSectionCondition(
			&cr.Status.Conditions, conditionTypeConsumeReferences, metav1.ConditionFalse,
			reason, message, cr.Generation, sectionTypes,
		)
	}
}

// consumeProblemReason picks the reason to report, most actionable first. A
// producer that stays missing past consumeReferenceDanglingThreshold stops
// being described as a startup race.
func consumeProblemReason(cr *depsv1alpha1.AppDependencies, unauthorized, undeclared []string, message string) (reason, text string) {
	switch {
	case len(unauthorized) > 0:
		return reasons.ConsumeNotAuthorized, message
	case len(undeclared) > 0:
		return reasons.ConsumedResourceNotDeclared, message
	}
	existing := apimeta.FindStatusCondition(cr.Status.Conditions, conditionTypeConsumeReferences)
	if existing != nil && existing.Status == metav1.ConditionFalse &&
		time.Since(existing.LastTransitionTime.Time) > consumeReferenceDanglingThreshold {
		return reasons.ProducerLikelyMisconfigured, fmt.Sprintf(
			"%s (unresolved for over %s - this looks like a real misconfiguration, e.g. a typo in the consumer's namespace/name, rather than a startup-ordering race)",
			message, consumeReferenceDanglingThreshold)
	}
	return reasons.ProducerNotFoundYet, message
}

// allConsumeRefs flattens every reference to another CR's resource across the
// spec into one list: each section's consumes, the queues' subscribesTo, and
// the alarms' snsTopicRef.
func allConsumeRefs(cr *depsv1alpha1.AppDependencies) []consumeRefLocation {
	var refs []consumeRefLocation
	add := func(resourceType, field string, list ...depsv1alpha1.ConsumeRef) {
		for _, ref := range list {
			refs = append(refs, consumeRefLocation{resourceType, field, ref})
		}
	}
	if cr.Spec.SQS != nil {
		add(resourceTypeSQS, "sqs.consumes", cr.Spec.SQS.Consumes...)
		for _, q := range cr.Spec.SQS.Resources {
			add(resourceTypeSNS, fmt.Sprintf("sqs %q subscribesTo", q.Name), q.SubscribesTo...)
		}
	}
	if cr.Spec.SNS != nil {
		add(resourceTypeSNS, "sns.consumes", cr.Spec.SNS.Consumes...)
	}
	if cr.Spec.DynamoDB != nil {
		add(resourceTypeDynamoDB, "dynamodb.consumes", cr.Spec.DynamoDB.Consumes...)
	}
	if cr.Spec.S3 != nil {
		add(resourceTypeS3, "s3.consumes", cr.Spec.S3.Consumes...)
	}
	if cr.Spec.RDS != nil {
		add(resourceTypeRDS, "rds.consumes", cr.Spec.RDS.Consumes...)
	}
	if cr.Spec.Alarms != nil && cr.Spec.Alarms.SnsTopicRef != nil {
		add(resourceTypeSNS, "alarms.snsTopicRef", *cr.Spec.Alarms.SnsTopicRef)
	}
	return refs
}
