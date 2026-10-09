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

package rds

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

const credentialsSecretNameSuffix = "-rds-credentials"

// secretFieldOwner is deliberately its own field manager, separate from
// configmap's - the two are unrelated objects (a Secret vs. a ConfigMap),
// so there's no ownership overlap to coordinate, but naming it after this
// package specifically (rather than reusing a generic name) keeps a future
// `kubectl get --show-managed-fields` trace unambiguous about which
// package wrote it.
const secretFieldOwner = client.FieldOwner("cloudctl-operator-rds-secret")

// CredentialsSecretName returns the deterministic Secret name for a CR's
// mirrored RDS credentials.
func CredentialsSecretName(crName string) string {
	return crName + credentialsSecretNameSuffix
}

// EnsureCredentialsSecret mirrors AWS's own managed master-user secret
// (one per owned or authorized-consumed RDS instance) into a single
// Kubernetes Secret this operator owns, keyed the same way the connection
// ConfigMap keys non-secret connection info - split into two objects
// rather than one, since this project treats "only credentials go in a
// Secret" as a hard rule for every resource type, not just RDS. Refuses to
// touch a same-named Secret this CR doesn't already own, deletes the
// Secret entirely once there's nothing left to mirror, and regenerates it
// every reconcile so it can never drift from status - all the same rules
// configmap.Ensure already follows, deliberately kept in lockstep with it.
func EnsureCredentialsSecret(
	ctx context.Context,
	secretsManagerClient cloudctlaws.SecretsManagerClient,
	awsClient rdsAPI,
	k8sClient client.Client,
	cr *depsv1alpha1.AppDependencies,
) error {
	data, err := buildCredentialsData(ctx, secretsManagerClient, awsClient, k8sClient, cr)
	if err != nil {
		return err
	}

	name := CredentialsSecretName(cr.Name)
	existing := &corev1.Secret{}
	getErr := k8sClient.Get(ctx, client.ObjectKey{Namespace: cr.Namespace, Name: name}, existing)
	exists := getErr == nil
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return getErr
	}

	if exists {
		if owner := metav1.GetControllerOf(existing); owner == nil || owner.UID != cr.UID {
			return fmt.Errorf("Secret %q already exists and is not owned by this CR - refusing to manage it", name)
		}
	}

	if len(data) == 0 {
		if !exists {
			return nil
		}
		return client.IgnoreNotFound(k8sClient.Delete(ctx, existing))
	}

	gvk, err := apiutil.GVKForObject(cr, k8sClient.Scheme())
	if err != nil {
		return err
	}

	byteData := make(map[string][]byte, len(data))
	for k, v := range data {
		byteData[k] = []byte(v)
	}

	apply := applycorev1.Secret(name, cr.Namespace).
		WithType(corev1.SecretTypeOpaque).
		WithData(byteData).
		WithOwnerReferences(applymetav1.OwnerReference().
			WithAPIVersion(gvk.GroupVersion().String()).
			WithKind(gvk.Kind).
			WithName(cr.Name).
			WithUID(cr.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))

	return k8sClient.Apply(ctx, apply, secretFieldOwner, client.ForceOwnership)
}

// buildCredentialsData walks this CR's own owned instances and its
// authorized-consumed ones, producing one flat map ready to become a
// Secret's StringData.
func buildCredentialsData(
	ctx context.Context,
	secretsManagerClient cloudctlaws.SecretsManagerClient,
	awsClient rdsAPI,
	k8sClient client.Client,
	cr *depsv1alpha1.AppDependencies,
) (map[string]string, error) {
	data := map[string]string{}
	if cr.Spec.RDS == nil {
		return data, nil
	}

	for _, r := range cr.Spec.RDS.Resources {
		entry := findLedgerEntryByType(cr.Status.ManagedResources, resourceType, r.Name)
		if entry == nil {
			continue
		}
		if err := addCredentials(ctx, secretsManagerClient, awsClient, data, entry.ARN, r.Name, ""); err != nil {
			return nil, err
		}
	}

	for _, ref := range cr.Spec.RDS.Consumes {
		var producer depsv1alpha1.AppDependencies
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &producer); err != nil {
			continue // forward reference not resolved yet - self-resolves later
		}
		if !IsConsumerAuthorized(&producer, ref.ResourceName, cr.Namespace, cr.Name) {
			continue
		}
		entry := findLedgerEntryByType(producer.Status.ManagedResources, resourceType, ref.ResourceName)
		if entry == nil {
			continue
		}
		if err := addCredentials(ctx, secretsManagerClient, awsClient, data, entry.ARN, ref.ResourceName, ref.Name); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// addCredentials resolves and adds one instance's username/password into
// data. A not-yet-provisioned managed secret (ResolveConnectionInfo's ok
// is false, or CredentialsSecretARN is still empty) is skipped silently,
// not errored - it self-resolves once AWS finishes provisioning it.
func addCredentials(
	ctx context.Context,
	secretsManagerClient cloudctlaws.SecretsManagerClient,
	awsClient rdsAPI,
	data map[string]string,
	instanceARN, resourceName, producerCRName string,
) error {
	info, ok, err := ResolveConnectionInfo(ctx, awsClient, instanceARN)
	if err != nil {
		return err
	}
	if !ok || info.CredentialsSecretARN == "" {
		return nil
	}

	out, err := secretsManagerClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &info.CredentialsSecretARN})
	if err != nil {
		return wrapAWSError(err, fmt.Sprintf("reading managed credentials secret for %q", resourceName))
	}

	var parsed struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(out.SecretString)), &parsed); err != nil {
		return fmt.Errorf("parsing managed credentials secret for %q: %w", resourceName, err)
	}

	data[credentialsKey(resourceName, producerCRName, "USERNAME")] = parsed.Username
	data[credentialsKey(resourceName, producerCRName, "PASSWORD")] = parsed.Password
	return nil
}

// findLedgerEntryByType is a plain linear lookup, deliberately not
// status.FindManagedResource: that helper is tuned to the common case of
// a single resource name per lookup already known ahead of time, which
// this package's own rds.go also uses directly - this one exists purely
// so secret.go and configmap-facing code don't need to import the status
// package just to do the same walk this file already repeats twice above.
func findLedgerEntryByType(ledger []depsv1alpha1.ManagedResource, ledgerType, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Type == ledgerType && ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

var invalidEnvChars = regexp.MustCompile(`[^A-Z0-9_]`)

// credentialsKey mirrors configmap.go's own envKey scheme exactly (same
// uppercase-underscore shape, same owned-vs-consumed key shape) without
// importing that package - doing so would create an import cycle, since
// configmap.go will need to call into this package for connection info.
func credentialsKey(resourceName, producerCRName, suffix string) string {
	parts := []string{resourceType, resourceName, suffix}
	if producerCRName != "" {
		parts = []string{resourceType, producerCRName, resourceName, suffix}
	}
	joined := strings.ToUpper(strings.Join(parts, "_"))
	return invalidEnvChars.ReplaceAllString(joined, "_")
}
