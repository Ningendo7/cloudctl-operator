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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// conditionTypeEncryptionMatchesSpec is not in sectionTypes: the resource
// works as it did, so Ready must not change. It makes visible that spec and
// AWS disagree.
const conditionTypeEncryptionMatchesSpec = "EncryptionMatchesSpec"

// checkEncryptionMatchesSpec flags resources the spec no longer asks to
// encrypt with a KMS key but that still are. Dropping encryption from an
// entry is never applied: re-encrypting in place is not possible for every
// service, and silently dropping the key could strand data encrypted with it.
// Without this the entry reads Ready and nothing says spec and reality
// differ. A Warning event is recorded on each pass, like other persistent
// mismatches.
func checkEncryptionMatchesSpec(cr *depsv1alpha1.AppDependencies, recordEvent status.EventRecorder) {
	var stillEncrypted []string
	check := func(resourceType, name string, encryption *depsv1alpha1.EncryptionSpec) {
		if wantsKey(encryption) {
			return
		}
		entry, found := status.FindManagedResource(cr.Status.ManagedResources, resourceType, name)
		if found && kms.PinsAKey(cr.Status.ManagedResources, resourceType, entry) {
			stillEncrypted = append(stillEncrypted, fmt.Sprintf("%s %q", resourceType, name))
		}
	}
	if cr.Spec.SQS != nil {
		for _, q := range cr.Spec.SQS.Resources {
			check(resourceTypeSQS, q.Name, q.Encryption)
		}
	}
	if cr.Spec.SNS != nil {
		for _, t := range cr.Spec.SNS.Resources {
			check(resourceTypeSNS, t.Name, t.Encryption)
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, tbl := range cr.Spec.DynamoDB.Resources {
			check(resourceTypeDynamoDB, tbl.Name, tbl.Encryption)
		}
	}
	if cr.Spec.S3 != nil {
		for _, b := range cr.Spec.S3.Resources {
			check(resourceTypeS3, b.Name, b.Encryption)
		}
	}

	if len(stillEncrypted) == 0 {
		status.SetSectionCondition(
			&cr.Status.Conditions, conditionTypeEncryptionMatchesSpec, metav1.ConditionTrue,
			reasons.EncryptionAsDeclared, "no resource is encrypted with a KMS key the spec no longer asks for",
			cr.Generation, sectionTypes,
		)
		return
	}
	message := fmt.Sprintf(
		"%s: the spec does not ask for KMS encryption, but the resource is still encrypted with its key. Removing encryption is not applied; "+
			"set it back, or remove the entry, wait for it to be deleted and add it again",
		status.JoinCapped(stillEncrypted, ", "))
	status.SetSectionCondition(
		&cr.Status.Conditions, conditionTypeEncryptionMatchesSpec, metav1.ConditionFalse,
		reasons.EncryptionRemovalIgnored, message, cr.Generation, sectionTypes,
	)
	if recordEvent != nil {
		recordEvent("Warning", reasons.EncryptionRemovalIgnored, status.CapEventNote(message))
	}
}

// wantsKey reports whether the spec asks for a KMS key, a dedicated one
// (enabled) or a named one (kmsKeyRef).
func wantsKey(e *depsv1alpha1.EncryptionSpec) bool {
	return e != nil && (e.Enabled || e.KMSKeyRef != "")
}
