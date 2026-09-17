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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// iamAPI is the subset of the IAM client this package needs. It's a type
// alias to the exported cloudctlaws.IAMClient rather than its own separate
// interface, since that interface now has two consumers — this package's
// own unit tests, and controller-level envtest suites that need to fake
// the whole AWS backend to exercise Reconcile() end-to-end. Aliasing keeps
// every reference to iamAPI in this file unchanged (same pattern as
// sqsAPI/snsAPI/dynamodbAPI/s3API).
type iamAPI = cloudctlaws.IAMClient

const resourceType = "iam"
const roleLedgerName = "role"
const roleInlinePolicyName = "cloudctl-derived-policy"
const iamRoleNameMaxLen = 64

// roleName derives this CR's IAM role name. Namespace is folded in for the
// same reason SQS/SNS/DynamoDB/S3's naming does — IAM role names are
// unique per AWS account, not per Kubernetes namespace, so two same-named
// CRs in different namespaces would otherwise collide on one shared role.
func roleName(namespace, crName string) string {
	return cloudctlaws.ResourceName(namespace, crName, "role")
}

// Ensure derives this CR's least-privilege IAM policy from its owned and
// consumed resources and reconciles a role carrying it. Takes the whole CR
// (unlike every other resource package's Ensure, which takes individual
// fields) since deriving a policy genuinely needs to read across every
// section's spec and ledger at once, not just one section's own declared
// list.
//
// Returns ("", ledger, nil) with no role created when this CR declares no
// resources needing IAM at all (nothing owned, nothing consumed) — an
// empty CR shouldn't get an empty, pointless role.
//
// Kept as a thin orchestrator over named ensure*/build* steps (mirroring
// ensureRole below, and the same convention forge-operator's resource
// reconcilers use) rather than one long function body, so a failure's
// error message says which concern broke instead of requiring a reader to
// find their place in a longer sequence of AWS calls.
func Ensure(
	ctx context.Context,
	iamClient iamAPI,
	k8sClient client.Client,
	oidcProviderARN, oidcProviderURL string,
	cr *depsv1alpha1.AppDependencies,
	ledger []depsv1alpha1.ManagedResource,
) (updatedLedger []depsv1alpha1.ManagedResource, roleARN string, err error) {
	grants, skipped := collectGrants(ctx, k8sClient, cr)
	if len(grants) == 0 {
		return ledger, "", skippedToError(skipped)
	}

	name, err := validatedRoleName(cr)
	if err != nil {
		return ledger, "", err
	}

	trustPolicy, err := ensureTrustPolicy(oidcProviderARN, oidcProviderURL, cr)
	if err != nil {
		return ledger, "", err
	}

	permissionsPolicy, err := ensurePermissionsPolicy(grants)
	if err != nil {
		return ledger, "", err
	}

	arn, err := ensureRole(ctx, iamClient, cr.Namespace, cr.Name, string(cr.UID), name, trustPolicy)
	if err != nil {
		return ledger, "", err
	}

	if err := ensureRolePolicy(ctx, iamClient, name, permissionsPolicy); err != nil {
		return ledger, "", err
	}

	updatedLedger = recordVerified(ledger, arn)
	return updatedLedger, arn, skippedToError(skipped)
}

// validatedRoleName derives this CR's IAM role name and checks it against
// AWS's length limit up front, before any AWS call is made on its behalf.
func validatedRoleName(cr *depsv1alpha1.AppDependencies) (string, error) {
	name := roleName(cr.Namespace, cr.Name)
	if err := cloudctlaws.ValidateNameLength(name, iamRoleNameMaxLen, "IAM role"); err != nil {
		return "", err
	}
	return name, nil
}

// ensureTrustPolicy builds the IRSA trust policy this CR's role needs,
// after confirming the operator was actually configured with the OIDC
// provider details every trust policy is scoped to — checked here rather
// than at startup since a deployment that never uses sharedWith/consumes
// (and so never needs a role) shouldn't be forced to set them.
func ensureTrustPolicy(oidcProviderARN, oidcProviderURL string, cr *depsv1alpha1.AppDependencies) (string, error) {
	if oidcProviderARN == "" || oidcProviderURL == "" {
		return "", errors.New("this CR needs an IAM role but --oidc-provider-arn/--oidc-provider-url were not configured for this operator at startup")
	}

	saName := cr.Spec.ServiceAccountName
	if saName == "" {
		saName = cr.Name
	}
	return buildTrustPolicy(oidcProviderARN, oidcProviderURL, cr.Namespace, saName)
}

// ensurePermissionsPolicy builds the least-privilege permissions document
// from this CR's collected grants.
func ensurePermissionsPolicy(grants []grant) (string, error) {
	policy, err := buildPolicyDocument(grants)
	if err != nil {
		return "", fmt.Errorf("deriving IAM permissions policy: %w", err)
	}
	return policy, nil
}

// ensureRolePolicy attaches (or overwrites) the derived permissions policy
// on the role as its single inline policy.
func ensureRolePolicy(ctx context.Context, iamClient iamAPI, roleName, policyDocument string) error {
	if _, err := iamClient.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:       &roleName,
		PolicyName:     strPtr(roleInlinePolicyName),
		PolicyDocument: &policyDocument,
	}); err != nil {
		return wrapAWSError(err, "attaching derived policy to role")
	}
	return nil
}

func skippedToError(skipped []string) error {
	if len(skipped) == 0 {
		return nil
	}
	return fmt.Errorf("some consumed resources could not be granted yet (self-resolves once available): %s", strings.Join(skipped, "; "))
}

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

// ensureRole creates the role if it doesn't exist, or verifies ownership
// and corrects trust-policy drift if it does. Unlike every other resource
// type, there's no adopt:true escape hatch here — this role's name is
// entirely derived, never user-supplied, so finding it already exists
// under different ownership means something else in this account claimed
// the exact deterministic name this CR needs, not a resource this CR could
// ever legitimately want to take over.
func ensureRole(ctx context.Context, client iamAPI, namespace, crName, crUID, name, trustPolicy string) (string, error) {
	getOut, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: &name})

	var notFound *types.NoSuchEntityException
	if errors.As(err, &notFound) {
		createOut, cErr := client.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 &name,
			AssumeRolePolicyDocument: &trustPolicy,
			Tags:                     mapToTags(ownerTags(namespace, crName, crUID)),
		})
		if cErr != nil {
			return "", wrapAWSError(cErr, "creating IAM role")
		}
		return *createOut.Role.Arn, nil
	}
	if err != nil {
		return "", wrapAWSError(err, "looking up IAM role")
	}

	tags, tErr := listAllRoleTags(ctx, client, name)
	if tErr != nil {
		return "", wrapAWSError(tErr, "reading role tags")
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, crUID) {
		return "", fmt.Errorf("IAM role %q already exists and is not owned by this CR — this looks like a naming collision", name)
	}

	if getOut.Role.AssumeRolePolicyDocument == nil || !trustPolicyEquivalent(*getOut.Role.AssumeRolePolicyDocument, trustPolicy) {
		if _, err := client.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
			RoleName:       &name,
			PolicyDocument: &trustPolicy,
		}); err != nil {
			return "", wrapAWSError(err, "correcting IAM role trust policy")
		}
	}

	return *getOut.Role.Arn, nil
}

// trustPolicyEquivalent compares a freshly-built trust policy against
// GetRole's response. IAM's own docs confirm policies returned by Get*
// calls are URL-encoded (RFC 3986) — comparing the raw encoded string
// against plain JSON would always mismatch and call
// UpdateAssumeRolePolicy on every single reconcile regardless of whether
// anything actually changed.
func trustPolicyEquivalent(currentEncoded, desired string) bool {
	current, err := url.QueryUnescape(currentEncoded)
	if err != nil {
		// Can't decode - treat as different. Safe direction: this
		// triggers a correction (idempotent, harmless if nothing actually
		// changed) rather than silently skipping a real one.
		return false
	}
	return current == desired
}

// buildTrustPolicy constructs the IRSA trust policy: this cluster's OIDC
// provider may assume the role, scoped to exactly the ServiceAccount this
// CR's workload runs as.
func buildTrustPolicy(oidcProviderARN, oidcProviderURL, namespace, serviceAccountName string) (string, error) {
	host := strings.TrimPrefix(oidcProviderURL, "https://")
	host = strings.TrimSuffix(host, "/")

	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":    "Allow",
				"Principal": map[string]string{"Federated": oidcProviderARN},
				"Action":    "sts:AssumeRoleWithWebIdentity",
				"Condition": map[string]any{
					"StringEquals": map[string]string{
						host + ":sub": fmt.Sprintf("system:serviceaccount:%s:%s", namespace, serviceAccountName),
						host + ":aud": "sts.amazonaws.com",
					},
				},
			},
		},
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("encoding trust policy: %w", err)
	}
	return string(encoded), nil
}

func listAllRoleTags(ctx context.Context, client iamAPI, roleName string) ([]types.Tag, error) {
	var all []types.Tag
	var marker *string
	for {
		out, err := client.ListRoleTags(ctx, &iam.ListRoleTagsInput{RoleName: &roleName, Marker: marker})
		if err != nil {
			return nil, err
		}
		all = append(all, out.Tags...)
		if !out.IsTruncated {
			return all, nil
		}
		marker = out.Marker
	}
}

func recordVerified(ledger []depsv1alpha1.ManagedResource, arn string) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	if existing := status.FindManagedResource(ledger, resourceType, roleLedgerName); existing != nil {
		createdAt = existing.CreatedAt
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type: resourceType,
		Name: roleLedgerName,
		ARN:  arn,
		// The IAM role holds no data of its own — always managed
		// (Delete-equivalent) regardless of any other resource's
		// retention policy. A leftover role tied to a now-gone CR's
		// identity serves no purpose once that CR is gone.
		DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
		State:          depsv1alpha1.ManagedResourceStateVerified,
	})
	return ledger
}

func ownerTags(namespace, crName, crUID string) map[string]string {
	return map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}
}

func mapToTags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		k, v := k, v
		tags = append(tags, types.Tag{Key: &k, Value: &v})
	}
	return tags
}

func tagsToMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func strPtr(s string) *string { return &s }

func wrapAWSError(err error, context string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", context, err),
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
