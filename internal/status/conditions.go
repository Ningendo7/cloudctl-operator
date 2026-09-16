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

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionTypeReady is the aggregate condition computed from every
// currently-required per-section condition (e.g. SQSReady, S3Ready).
const ConditionTypeReady = "Ready"

// SetSectionCondition sets a per-section readiness condition and
// recomputes the aggregate Ready condition from it. sectionTypes lists
// every section condition type that should currently exist given the CR's
// spec — sections not declared in spec aren't required to be Ready.
func SetSectionCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64, sectionTypes []string) {
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
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
		if c == nil || c.Status != metav1.ConditionTrue {
			readyStatus = metav1.ConditionFalse
			reason = "SectionsNotReady"
			message = fmt.Sprintf("%s is not ready", t)
			break
		}
	}

	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             readyStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}
