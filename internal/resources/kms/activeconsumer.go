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

package kms

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// listAllCRs fetches every AppDependencies CR cluster-wide once - callers
// reuse the result across multiple HasActiveConsumer checks in one pass
// (e.g. Cleanup's own loop over several keys) rather than re-listing per
// check.
func listAllCRs(ctx context.Context, k8sClient client.Client) ([]depsv1alpha1.AppDependencies, error) {
	var all depsv1alpha1.AppDependenciesList
	if err := k8sClient.List(ctx, &all); err != nil {
		return nil, err
	}
	return all.Items, nil
}

// HasActiveConsumer reports whether any CR in crs currently declares an
// encryption.kmsKeyRef pointing at this exact key (producerNamespace/
// producerCRName/keyName), across every resource type that supports
// kmsKeyRef. Cleanup must never call ScheduleKeyDeletion while this is
// true - deletion is irreversible past AWS's own window, and would make
// a completely different, still-live resource's data permanently
// unreadable, not just this key's own.
func HasActiveConsumer(crs []depsv1alpha1.AppDependencies, producerNamespace, producerCRName, keyName string) bool {
	refMatches := func(ref *depsv1alpha1.ConsumeRef) bool {
		return ref != nil && ref.Namespace == producerNamespace && ref.Name == producerCRName && ref.ResourceName == keyName
	}

	for i := range crs {
		cr := &crs[i]
		if cr.Spec.SQS != nil {
			for _, r := range cr.Spec.SQS.Resources {
				if r.Encryption != nil && refMatches(r.Encryption.KMSKeyRef) {
					return true
				}
			}
		}
		if cr.Spec.SNS != nil {
			for _, r := range cr.Spec.SNS.Resources {
				if r.Encryption != nil && refMatches(r.Encryption.KMSKeyRef) {
					return true
				}
			}
		}
		if cr.Spec.DynamoDB != nil {
			for _, r := range cr.Spec.DynamoDB.Resources {
				if r.Encryption != nil && refMatches(r.Encryption.KMSKeyRef) {
					return true
				}
			}
		}
		if cr.Spec.S3 != nil {
			for _, r := range cr.Spec.S3.Resources {
				if r.Encryption != nil && refMatches(r.Encryption.KMSKeyRef) {
					return true
				}
			}
		}
		if cr.Spec.RDS != nil {
			for _, r := range cr.Spec.RDS.Resources {
				if r.Encryption != nil && refMatches(r.Encryption.KMSKeyRef) {
					return true
				}
			}
		}
	}
	return false
}
