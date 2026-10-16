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
	"fmt"
	"strings"
	"unicode/utf8"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionTypeReady is the aggregate condition computed from every
// currently-required per-section condition (e.g. SQSReady, S3Ready).
const ConditionTypeReady = "Ready"

// ConditionTypeDeletionBlocked reports a CR stuck mid-deletion because at
// least one owned resource is still waiting on its non-empty guard (or, for
// KMS, its deletion quiet window) before it can actually be removed. Not a
// per-section condition - it only ever exists during the deletion reconcile
// path, never the normal one.
const ConditionTypeDeletionBlocked = "DeletionBlocked"

// MaxMessageLength caps every condition message. The API server rejects a
// message over 32768 bytes and, with it, the whole status patch, ledger
// checkpoints included, so a long joined error could leave a CR unwritable.
// A few KiB is plenty to read, and well under the limit.
const MaxMessageLength = 4096

const truncatedMarker = "… (truncated)"

// CapMessage shortens msg to MaxMessageLength bytes, on a character
// boundary, saying that it was cut.
func CapMessage(msg string) string {
	if len(msg) <= MaxMessageLength {
		return msg
	}
	cut := MaxMessageLength - len(truncatedMarker)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + truncatedMarker
}

// JoinCapped joins items with sep as long as the result fits, ending with
// "… and N more" for the rest, so the reader knows the list is longer.
func JoinCapped(items []string, sep string) string {
	whole := strings.Join(items, sep)
	if len(whole) <= MaxMessageLength {
		return whole
	}
	// Room for the longest possible ending, "… and 999999 more".
	budget := MaxMessageLength - len(sep) - len("… and 999999 more")
	var b strings.Builder
	shown := 0
	for _, item := range items {
		if b.Len()+len(item) > budget {
			break
		}
		if shown > 0 {
			b.WriteString(sep)
		}
		b.WriteString(item)
		shown++
	}
	if shown > 0 {
		b.WriteString(sep)
	}
	fmt.Fprintf(&b, "… and %d more", len(items)-shown)
	return b.String()
}

// SetSectionCondition sets a per-section readiness condition and
// recomputes the aggregate Ready condition from it. sectionTypes lists
// every section condition type that should currently exist given the CR's
// spec — sections not declared in spec aren't required to be Ready.
func SetSectionCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64, sectionTypes []string) {
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            CapMessage(message),
		ObservedGeneration: observedGeneration,
	})
	recomputeReady(conditions, sectionTypes, observedGeneration)
}

func recomputeReady(conditions *[]metav1.Condition, sectionTypes []string, observedGeneration int64) {
	readyStatus := metav1.ConditionTrue
	reason := "AllSectionsReady"
	message := "all declared sections are ready"

	for _, t := range sectionTypes {
		c := apimeta.FindStatusCondition(*conditions, t)
		switch {
		case c == nil || c.Status != metav1.ConditionTrue:
			readyStatus = metav1.ConditionFalse
			reason = "SectionsNotReady"
			message = fmt.Sprintf("%s is not ready", t)
		case c.ObservedGeneration != observedGeneration:
			// ensureDesiredState checkpoints status after every section, so
			// this runs once per section per reconcile - a section further
			// along in sectionTypes than whichever one just ran hasn't been
			// touched yet this pass and still carries its condition from
			// the CR's previous generation. A stale True must never count
			// as proof the current generation's own reconcile actually
			// reached and passed that section - without this check, Ready
			// could flip True mid-reconcile the moment every section
			// happens to be True for ANY generation, not the current one.
			readyStatus = metav1.ConditionFalse
			reason = "SectionsNotReady"
			message = fmt.Sprintf("%s has not been reconciled for the current generation yet", t)
		default:
			continue
		}
		break
	}

	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             readyStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}

// SetDeletionBlocked records why a CR's deletion hasn't completed yet and
// pulls the aggregate Ready condition down with it - a CR stuck mid-deletion
// is not a healthy, steady state, and kubectl get's Ready column must say so
// rather than keep showing whatever Ready last said before deletion started
// (the per-section conditions aren't touched by the deletion path at all, so
// nothing else would ever correct it).
func SetDeletionBlocked(conditions *[]metav1.Condition, observedGeneration int64, reasons []string) {
	message := JoinCapped(reasons, "; ")
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionTypeDeletionBlocked,
		Status:             metav1.ConditionTrue,
		Reason:             "ResourcesNotDrained",
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             "DeletionBlocked",
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}
