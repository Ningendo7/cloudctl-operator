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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
	"github.com/Ningendo7/cloudctl-operator/internal/status"
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

// EnsureCredentialsSecret mirrors AWS's own managed master-user secret for
// each RDS instance this CR owns into a single Kubernetes Secret this
// operator owns. Consumers never receive credentials: the master user can
// do anything in the database, which is far more than the network access
// sharedWith grants, so the owner creates per-app database users instead.
// Credentials stay out of the connection ConfigMap, since this project
// keeps only credentials in a Secret. Refuses to touch a same-named Secret
// this CR doesn't already own, deletes the Secret once there's nothing left
// to mirror, and regenerates it every reconcile so it can't drift from
// status.
func EnsureCredentialsSecret(
	ctx context.Context,
	secretsManagerClient cloudctlaws.SecretsManagerClient,
	awsClient rdsAPI,
	k8sClient client.Client,
	apiReader client.Reader,
	cr *depsv1alpha1.AppDependencies,
) error {
	data, err := buildCredentialsData(ctx, secretsManagerClient, awsClient, cr)
	if err != nil {
		return err
	}

	name := CredentialsSecretName(cr.Name)
	existing := &corev1.Secret{}
	key := client.ObjectKey{Namespace: cr.Namespace, Name: name}
	getErr := k8sClient.Get(ctx, key, existing)
	if apierrors.IsNotFound(getErr) {
		getErr = apiReader.Get(ctx, key, existing)
	}
	exists := getErr == nil
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return getErr
	}

	if exists {
		if owner := metav1.GetControllerOf(existing); owner == nil || owner.UID != cr.UID {
			return fmt.Errorf("secret %q already exists and is not owned by this CR - refusing to manage it", name)
		}
	}

	if len(data) == 0 {
		if !exists {
			return nil
		}
		return client.IgnoreNotFound(k8sClient.Delete(ctx, existing))
	}

	byteData := make(map[string][]byte, len(data))
	for k, v := range data {
		byteData[k] = []byte(v)
	}
	// Ownership was checked above; skip an apply that would change nothing.
	if exists && maps.EqualFunc(existing.Data, byteData, bytes.Equal) &&
		existing.Labels[depsv1alpha1.ManagedByLabelKey] == depsv1alpha1.ManagedByLabelValue &&
		status.AppliedBy(existing, string(secretFieldOwner)) {
		return nil
	}

	gvk, err := apiutil.GVKForObject(cr, k8sClient.Scheme())
	if err != nil {
		return err
	}

	apply := applycorev1.Secret(name, cr.Namespace).
		WithLabels(map[string]string{depsv1alpha1.ManagedByLabelKey: depsv1alpha1.ManagedByLabelValue}).
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

// buildCredentialsData collects the credentials of the instances this CR
// owns into one flat map, ready to become a Secret's data.
func buildCredentialsData(
	ctx context.Context,
	secretsManagerClient cloudctlaws.SecretsManagerClient,
	awsClient rdsAPI,
	cr *depsv1alpha1.AppDependencies,
) (map[string]string, error) {
	data := NewKeyedData()
	if cr.Spec.RDS == nil {
		return data.Values(), nil
	}

	for _, r := range cr.Spec.RDS.Resources {
		entry, found := status.FindManagedResource(cr.Status.ManagedResources, resourceType, r.Name)
		if !found {
			continue
		}
		if err := addCredentials(ctx, secretsManagerClient, awsClient, data, entry.ARN, r.Name); err != nil {
			return nil, err
		}
	}
	return data.Values(), nil
}

// addCredentials resolves and adds one instance's username/password into
// data. A not-yet-provisioned managed secret (ResolveConnectionInfo's ok
// is false, or CredentialsSecretARN is still empty) is skipped silently,
// not errored - it self-resolves once AWS finishes provisioning it.
func addCredentials(
	ctx context.Context,
	secretsManagerClient cloudctlaws.SecretsManagerClient,
	awsClient rdsAPI,
	data *KeyedData,
	instanceARN, resourceName string,
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

	source := fmt.Sprintf("RDS instance %q", resourceName)
	if err := data.Put(credentialsKey(resourceName, "USERNAME"), parsed.Username, source); err != nil {
		return err
	}
	return data.Put(credentialsKey(resourceName, "PASSWORD"), parsed.Password, source)
}

var invalidEnvChars = regexp.MustCompile(`[^A-Z0-9_]`)

// ReasonKeyCollision marks two different sources that map to one key.
const ReasonKeyCollision = "ConnectionKeyCollision"

// KeyedData is the flat map behind a ConfigMap or Secret, remembering which
// source wrote each key. Keys are upper-cased with every other character
// turned into '_', so different names can fold into one key (Orders and
// orders, a-b and a_b, team-a/orders and team/a-orders), and the second
// write would silently replace the first and point the app at the wrong
// resource. The configmap package uses it too, since it imports this one.
type KeyedData struct {
	values  map[string]string
	sources map[string]string
}

func NewKeyedData() *KeyedData {
	return &KeyedData{values: map[string]string{}, sources: map[string]string{}}
}

// Put stores value under key for source, or refuses when a different source
// already owns the key. The same source writing a key again is fine.
func (d *KeyedData) Put(key, value, source string) error {
	if prior, taken := d.sources[key]; taken && prior != source {
		return &cloudctlaws.ReconcileError{
			Err:    fmt.Errorf("%s and %s would both be written to the key %s; rename one of them so their keys differ", prior, source, key),
			Reason: ReasonKeyCollision,
		}
	}
	d.values[key] = value
	d.sources[key] = source
	return nil
}

func (d *KeyedData) Values() map[string]string { return d.values }

// credentialsKey uses configmap.go's owned-resource key shape without
// importing that package, which would create an import cycle.
func credentialsKey(resourceName, suffix string) string {
	joined := strings.ToUpper(strings.Join([]string{resourceType, resourceName, suffix}, "_"))
	return invalidEnvChars.ReplaceAllString(joined, "_")
}
