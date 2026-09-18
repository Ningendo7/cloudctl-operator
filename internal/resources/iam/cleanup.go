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

package iam

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// Cleanup removes the IAM role and its inline policy once this CR no
// longer needs one — either because every resource that required it was
// removed from spec (deleting=false, but collectGrants now comes back
// empty) or because the whole CR is being deleted (deleting=true, which
// skips the grants recheck entirely). Unlike every other resource type,
// there's no retain/orphan option and no non-empty guard here: the role
// holds no data of its own, so there's no reason to leave one behind once
// nothing needs it, and nothing about deleting it risks losing anything.
func Cleanup(
	ctx context.Context,
	iamClient iamAPI,
	k8sClient client.Client,
	cr *depsv1alpha1.AppDependencies,
	ledger []depsv1alpha1.ManagedResource,
	deleting bool,
) (updatedLedger []depsv1alpha1.ManagedResource, err error) {
	entry := status.FindManagedResource(ledger, resourceType, roleLedgerName)
	if entry == nil {
		return ledger, nil // never created, nothing to do
	}

	if !deleting {
		grants, _ := collectGrants(ctx, k8sClient, cr)
		if len(grants) > 0 {
			return ledger, nil // still needed
		}
	}

	name, nameErr := roleNameFromARN(entry.ARN)
	if nameErr != nil {
		return ledger, nameErr
	}

	tags, tErr := listAllRoleTags(ctx, iamClient, name)
	if tErr != nil {
		var notFound *types.NoSuchEntityException
		if errors.As(tErr, &notFound) {
			updatedLedger = ledger
			status.RemoveManagedResource(&updatedLedger, resourceType, roleLedgerName)
			return updatedLedger, nil
		}
		return ledger, wrapAWSError(tErr, fmt.Sprintf("re-verifying ownership of IAM role %q before delete", name))
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), cr.Namespace, cr.Name, string(cr.UID)) {
		return ledger, fmt.Errorf("IAM role %q no longer verified as owned by this CR — refusing to delete it", name)
	}

	if _, err := iamClient.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{
		RoleName:   &name,
		PolicyName: strPtr(roleInlinePolicyName),
	}); err != nil {
		var notFound *types.NoSuchEntityException
		if !errors.As(err, &notFound) {
			return ledger, wrapAWSError(err, fmt.Sprintf("deleting inline policy on role %q", name))
		}
	}

	if _, err := iamClient.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: &name}); err != nil {
		var notFound *types.NoSuchEntityException
		if !errors.As(err, &notFound) {
			return ledger, wrapAWSError(err, fmt.Sprintf("deleting IAM role %q", name))
		}
	}

	updatedLedger = ledger
	status.RemoveManagedResource(&updatedLedger, resourceType, roleLedgerName)
	return updatedLedger, nil
}

// roleNameFromARN extracts the role name from a stored
// arn:aws:iam::account:role/name ARN instead of recomputing it from
// namespace/crName, so cleanup doesn't depend on spec context that may
// already be gone.
func roleNameFromARN(arn string) (string, error) {
	idx := strings.LastIndex(arn, "/")
	if idx == -1 || idx == len(arn)-1 {
		return "", fmt.Errorf("unexpected role ARN format: %s", arn)
	}
	return arn[idx+1:], nil
}
