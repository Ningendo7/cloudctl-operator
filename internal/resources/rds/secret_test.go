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
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// ownedCRWithCredentials builds a CR owning one RDS instance whose fake
// AWS-side state is wired up for a successful credentials mirror: an
// endpoint, a managed secret ARN, and a Secrets Manager entry holding the
// matching username/password JSON.
func ownedCRWithCredentials(t *testing.T, rdsClient *fakeRDS, smClient *fakeSecretsManager, namespace, crName, resourceName, username, password string) *depsv1alpha1.AppDependencies {
	t.Helper()
	ledger := setupInstance(t, rdsClient, namespace, crName, resourceName, depsv1alpha1.DeletionPolicyRetain)
	entry := findLedgerEntry(t, ledger, resourceType, resourceName)
	instance := rdsClient.findByARN(entry.ARN)
	instance.endpointAddress = resourceName + ".abc123.us-east-1.rds.amazonaws.com"
	instance.endpointPort = 5432
	instanceID, err := instanceIDFromARN(entry.ARN)
	if err != nil {
		t.Fatalf("instanceIDFromARN(%q): %v", entry.ARN, err)
	}
	secretARN := "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds!db-" + instanceID
	instance.masterUserSecretARN = secretARN
	smClient.secrets[secretARN] = `{"username":"` + username + `","password":"` + password + `"}`

	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName, UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: resourceName},
		}}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: ledger},
	}
}

func newFakeK8sClientWithScheme(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()
}

func TestEnsureCredentialsSecret_OwnedInstance_HappyPath(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}

	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret: %v", err)
	}
	if string(secret.Data["RDS_ORDERS_DB_USERNAME"]) != "cloudctl_admin" {
		t.Errorf("username = %q, want cloudctl_admin", secret.Data["RDS_ORDERS_DB_USERNAME"])
	}
	if string(secret.Data["RDS_ORDERS_DB_PASSWORD"]) != "s3cr3t-pw" {
		t.Errorf("password = %q, want s3cr3t-pw", secret.Data["RDS_ORDERS_DB_PASSWORD"])
	}
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want Opaque", secret.Type)
	}
	if owner := metav1.GetControllerOf(&secret); owner == nil || owner.UID != cr.UID {
		t.Error("expected the Secret to carry an owner reference back to the CR")
	}
}

func TestEnsureCredentialsSecret_NilRDSSpec_NoSecretCreated(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service", UID: "uid-1"}}
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret)
	if err == nil {
		t.Error("expected no Secret to be created for a CR with no RDS spec")
	}
}

func TestEnsureCredentialsSecret_SecretNotYetProvisioned_SkipsSilently(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	ledger := setupInstance(t, rdsClient, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	// Endpoint set, but masterUserSecretARN deliberately left unset - AWS
	// hasn't finished provisioning the managed secret yet.
	entry := findLedgerEntry(t, ledger, resourceType, "orders-db")
	rdsClient.findByARN(entry.ARN).endpointAddress = "orders-db.abc123.us-east-1.rds.amazonaws.com"

	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service", UID: "uid-1"},
		Spec:       depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "orders-db"}}}},
		Status:     depsv1alpha1.AppDependenciesStatus{ManagedResources: ledger},
	}
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret); err == nil {
		t.Error("expected no Secret while the managed credentials secret isn't provisioned yet")
	}
}

func TestEnsureCredentialsSecret_DeletesSecretOnceEmpty(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	k8sClient := newFakeK8sClientWithScheme(t, cr)
	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("first EnsureCredentialsSecret() error = %v", err)
	}

	// Instance removed from spec entirely - the Secret must be deleted,
	// not left around with stale credentials for a database that's no
	// longer declared.
	cr.Spec.RDS = nil
	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("second EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret)
	if err == nil {
		t.Error("expected the Secret to be deleted once nothing is left to mirror")
	}
}

func TestEnsureCredentialsSecret_RefusesForeignSecret(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: CredentialsSecretName("checkout-service")}}
	k8sClient := newFakeK8sClientWithScheme(t, cr, foreign)

	err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr)
	if err == nil {
		t.Fatal("expected EnsureCredentialsSecret to refuse a same-named Secret it doesn't own")
	}
}

func TestEnsureCredentialsSecret_RefusesForeignSecretHiddenFromCache(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	foreign := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: CredentialsSecretName("checkout-service")},
		Data:       map[string][]byte{"UNRELATED": []byte("keep-me")},
	}
	cached := newFakeK8sClientWithScheme(t, cr)
	apiServer := newFakeK8sClientWithScheme(t, cr, foreign)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, cached, apiServer, cr); err == nil {
		t.Fatal("expected EnsureCredentialsSecret to refuse a foreign Secret the label-filtered cache can't see")
	}
	var secret corev1.Secret
	if err := apiServer.Get(context.Background(), client.ObjectKeyFromObject(foreign), &secret); err != nil {
		t.Fatalf("get foreign Secret: %v", err)
	}
	if string(secret.Data["UNRELATED"]) != "keep-me" {
		t.Errorf("foreign Secret was modified: %v", secret.Data)
	}
}

func TestEnsureCredentialsSecret_LabelsSecretAsManaged(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret); err != nil {
		t.Fatalf("get Secret: %v", err)
	}
	for key, want := range map[string]string{
		depsv1alpha1.ManagedLabelKey:   depsv1alpha1.ManagedLabelValue,
		depsv1alpha1.ManagedByLabelKey: depsv1alpha1.ManagedByLabelValue,
	} {
		if got := secret.Labels[key]; got != want {
			t.Errorf("label %s = %q, want %q", key, got, want)
		}
	}
}

func authorizedConsumerOf(producer *depsv1alpha1.AppDependencies) *depsv1alpha1.AppDependencies {
	producer.Spec.RDS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{
		{Namespace: "fulfillment", Name: "fulfillment-service"},
	}
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service", UID: "uid-2"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		}}},
	}
}

func TestEnsureCredentialsSecret_ConsumerNeverReceivesProducersCredentials(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	producer := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	consumer := authorizedConsumerOf(producer)
	k8sClient := newFakeK8sClientWithScheme(t, producer, consumer)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "fulfillment", Name: CredentialsSecretName("fulfillment-service")}, &secret)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected no Secret for an authorized consumer, got err=%v data=%v", err, secret.Data)
	}
}

func TestEnsureCredentialsSecret_ConsumerWithOwnInstance_GetsOnlyItsOwn(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	producer := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "producer-user", "producer-pass")
	consumer := ownedCRWithCredentials(t, rdsClient, smClient, "fulfillment", "fulfillment-service", "ledger-db", "own-user", "own-pass")
	consumer.Spec.RDS.Consumes = authorizedConsumerOf(producer).Spec.RDS.Consumes
	k8sClient := newFakeK8sClientWithScheme(t, producer, consumer)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "fulfillment", Name: CredentialsSecretName("fulfillment-service")}, &secret); err != nil {
		t.Fatalf("getting Secret: %v", err)
	}
	if string(secret.Data["RDS_LEDGER_DB_USERNAME"]) != "own-user" {
		t.Errorf("expected the consumer's own credentials, got %v", secret.Data)
	}
	for k := range secret.Data {
		if strings.Contains(k, "ORDERS_DB") {
			t.Errorf("the producer's credentials leaked into the consumer's Secret: %s", k)
		}
	}
}

func TestEnsureCredentialsSecret_RemovesCredentialsAnEarlierVersionDeliveredToAConsumer(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	producer := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	consumer := authorizedConsumerOf(producer)
	delivered := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fulfillment", Name: CredentialsSecretName("fulfillment-service"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: depsv1alpha1.GroupVersion.String(), Kind: "AppDependencies",
				Name: consumer.Name, UID: consumer.UID, Controller: ptrTo(true),
			}},
		},
		Data: map[string][]byte{"RDS_DEFAULT_CHECKOUT_SERVICE_ORDERS_DB_PASSWORD": []byte("s3cr3t-pw")},
	}
	k8sClient := newFakeK8sClientWithScheme(t, producer, consumer, delivered)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(delivered), &secret)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected the previously delivered Secret to be removed, got err=%v", err)
	}
}

func ptrTo[T any](v T) *T { return &v }

func TestEnsureCredentialsSecret_PropagatesGetSecretValueFailure(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	smClient.getSecretValueErr = &fakeAWSError{code: "ThrottlingException"}
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err == nil {
		t.Fatal("expected GetSecretValue's failure to propagate")
	}
}

func TestEnsureCredentialsSecret_MalformedSecretJSON_ReturnsError(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	entry := findLedgerEntry(t, cr.Status.ManagedResources, resourceType, "orders-db")
	secretARN := rdsClient.findByARN(entry.ARN).masterUserSecretARN
	smClient.secrets[secretARN] = "not-json"
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err == nil {
		t.Fatal("expected a malformed managed-secret payload to be surfaced as an error")
	}
}

func TestEnsureCredentialsSecret_MultipleOwnedInstances(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	ledger1 := setupInstance(t, rdsClient, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	entry1 := findLedgerEntry(t, ledger1, resourceType, "orders-db")
	i1 := rdsClient.findByARN(entry1.ARN)
	i1.endpointAddress, i1.endpointPort = "orders-db.host", 5432
	i1.masterUserSecretARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds!db-orders-db"
	smClient.secrets[i1.masterUserSecretARN] = `{"username":"u1","password":"p1"}`

	ledger2, err := createInstance(context.Background(), rdsClient, "default", "checkout-service", "uid-1",
		cloudctlaws.ResourceName("default", "checkout-service", resourceType, "invoices-db", 63), "invoices-db",
		instanceOptions{deletionPolicy: depsv1alpha1.DeletionPolicyRetain, engine: "mysql", engineVersion: "8.0", instanceClass: "db.t4g.micro", dbSubnetGroupName: "sg", securityGroupID: "sg-2"},
		ledger1, nil)
	if err != nil {
		t.Fatalf("setup second createInstance() error = %v", err)
	}
	ledger2, err = ensureInstance(context.Background(), rdsClient, "default", "checkout-service", "uid-1", "invoices-db",
		instanceOptions{deletionPolicy: depsv1alpha1.DeletionPolicyRetain, engine: "mysql", engineVersion: "8.0", instanceClass: "db.t4g.micro", dbSubnetGroupName: "sg", securityGroupID: "sg-2"},
		ledger2, nil)
	if err != nil {
		t.Fatalf("setup second ensureInstance() error = %v", err)
	}
	entry2 := findLedgerEntry(t, ledger2, resourceType, "invoices-db")
	i2 := rdsClient.findByARN(entry2.ARN)
	i2.endpointAddress, i2.endpointPort = "invoices-db.host", 3306
	i2.masterUserSecretARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds!db-invoices-db"
	smClient.secrets[i2.masterUserSecretARN] = `{"username":"u2","password":"p2"}`

	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db"}, {Name: "invoices-db"},
		}}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: ledger2},
	}
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret: %v", err)
	}
	if string(secret.Data["RDS_ORDERS_DB_USERNAME"]) != "u1" || string(secret.Data["RDS_INVOICES_DB_USERNAME"]) != "u2" {
		t.Errorf("expected both instances' credentials present, got %v", secret.Data)
	}
}

// TestEnsureCredentialsSecret_RotationFlowsThroughOnNextReconcile proves
// the mirrored Secret is always freshly re-derived from Secrets Manager,
// never cached independently: when AWS rotates the managed secret
// out-of-band (exactly what ManageMasterUserPassword:true means AWS does
// on its own schedule, with no action from this operator), the very next
// reconcile's Secret reflects the new value - Secrets Manager remains the
// sole source of truth throughout, and this operator never originates or
// retains a credential value of its own.
func TestEnsureCredentialsSecret_RotationFlowsThroughOnNextReconcile(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "old-password")
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("first EnsureCredentialsSecret() error = %v", err)
	}
	secretName := CredentialsSecretName("checkout-service")
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: secretName}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret: %v", err)
	}
	if string(secret.Data["RDS_ORDERS_DB_PASSWORD"]) != "old-password" {
		t.Fatalf("password = %q, want old-password", secret.Data["RDS_ORDERS_DB_PASSWORD"])
	}

	// AWS rotates the managed secret entirely on its own - this operator
	// never calls PutSecretValue/UpdateSecret itself (SecretsManagerClient
	// only ever exposes GetSecretValue), so the only way the fake's state
	// changes here is simulating what AWS itself would do.
	entry := findLedgerEntry(t, cr.Status.ManagedResources, resourceType, "orders-db")
	secretARN := rdsClient.findByARN(entry.ARN).masterUserSecretARN
	smClient.secrets[secretARN] = `{"username":"cloudctl_admin","password":"new-rotated-password"}`

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("second EnsureCredentialsSecret() error = %v", err)
	}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: secretName}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret after rotation: %v", err)
	}
	if string(secret.Data["RDS_ORDERS_DB_PASSWORD"]) != "new-rotated-password" {
		t.Errorf("expected the mirrored Secret to reflect Secrets Manager's rotated value, got %q", secret.Data["RDS_ORDERS_DB_PASSWORD"])
	}
}

// TestEnsureCredentialsSecret_TransientFailureNeverOverwritesWithFabricatedData
// proves a GetSecretValue failure never causes this operator to write
// empty, zero-value, or otherwise fabricated credentials over the
// last-known-good mirror - the failure must propagate as an error instead,
// leaving the existing Secret exactly as it was. Overwriting on failure
// would effectively make this operator an independent (and in this case
// wrong) source of truth for the brief window of an AWS hiccup.
func TestEnsureCredentialsSecret_TransientFailureNeverOverwritesWithFabricatedData(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "good-password")
	k8sClient := newFakeK8sClientWithScheme(t, cr)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
		t.Fatalf("first EnsureCredentialsSecret() error = %v", err)
	}

	smClient.getSecretValueErr = &fakeAWSError{code: "ThrottlingException"}
	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err == nil {
		t.Fatal("expected the transient GetSecretValue failure to propagate as an error")
	}

	secretName := CredentialsSecretName("checkout-service")
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: secretName}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret after the failed reconcile: %v", err)
	}
	if string(secret.Data["RDS_ORDERS_DB_PASSWORD"]) != "good-password" {
		t.Errorf("expected the last-known-good password to survive a transient failure untouched, got %q", secret.Data["RDS_ORDERS_DB_PASSWORD"])
	}
}

func TestCredentialsKey(t *testing.T) {
	for in, want := range map[string]string{
		"orders-db": "RDS_ORDERS_DB_USERNAME",
		"a.b":       "RDS_A_B_USERNAME",
	} {
		if got := credentialsKey(in, "USERNAME"); got != want {
			t.Errorf("credentialsKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKeyedData_RefusesASecondSourceForAKey(t *testing.T) {
	d := NewKeyedData()
	if err := d.Put("RDS_ORDERS_HOST", "a", "instance orders"); err != nil {
		t.Fatalf("first Put() error = %v", err)
	}
	if err := d.Put("RDS_ORDERS_HOST", "a", "instance orders"); err != nil {
		t.Errorf("the same source writing its key again is not a collision: %v", err)
	}
	err := d.Put("RDS_ORDERS_HOST", "b", "instance Orders")
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != ReasonKeyCollision || reconcileErr.Retryable {
		t.Fatalf("expected a non-retryable %s error, got %v", ReasonKeyCollision, err)
	}
	for _, name := range []string{"instance orders", "instance Orders", "RDS_ORDERS_HOST"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the error should name %q: %v", name, err)
		}
	}
	if d.Values()["RDS_ORDERS_HOST"] != "a" {
		t.Error("the first source's value must be kept")
	}
}

// Two instances whose names differ only in case or in - versus _ would
// share a key, and the app would read one database's password as the
// other's.
func TestEnsureCredentialsSecret_RefusesInstancesWhoseKeysCollide(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	first := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "u1", "p1")
	second := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "Orders_DB", "u2", "p2")
	first.Spec.RDS.Resources = append(first.Spec.RDS.Resources, second.Spec.RDS.Resources...)
	first.Status.ManagedResources = append(first.Status.ManagedResources, second.Status.ManagedResources...)
	k8sClient := newFakeK8sClientWithScheme(t, first)

	err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, first)
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != ReasonKeyCollision {
		t.Fatalf("expected %s, got %v", ReasonKeyCollision, err)
	}
	var secret corev1.Secret
	if getErr := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret); getErr == nil {
		t.Error("no Secret should be written when two instances map to one key")
	}
}

// countingApplies returns a fake client that records managedFields, as the
// API server does, and counts the applies it receives.
func countingApplies(t *testing.T, objs ...client.Object) (client.WithWatch, *int) {
	t.Helper()
	applies := 0
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).WithReturnManagedFields().
		WithInterceptorFuncs(interceptor.Funcs{
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				applies++
				return c.Apply(ctx, obj, opts...)
			},
		}).Build()
	return c, &applies
}

func TestEnsureCredentialsSecret_AppliesOnlyWhenTheCredentialsChange(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	k8sClient, applies := countingApplies(t, cr)
	ensure := func() {
		t.Helper()
		if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
			t.Fatalf("EnsureCredentialsSecret() error = %v", err)
		}
	}

	ensure()
	ensure()
	if *applies != 1 {
		t.Fatalf("applies = %d after two passes with nothing changed, want 1", *applies)
	}

	for arn := range smClient.secrets {
		smClient.secrets[arn] = `{"username":"cloudctl_admin","password":"rotated"}`
	}
	ensure()
	if *applies != 2 {
		t.Fatalf("applies = %d after a rotation, want 2", *applies)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret: %v", err)
	}
	if string(secret.Data["RDS_ORDERS_DB_PASSWORD"]) != "rotated" {
		t.Errorf("password = %q, want the rotated one", secret.Data["RDS_ORDERS_DB_PASSWORD"])
	}
}

func TestEnsureCredentialsSecret_LabelsDecideWhetherItIsApplied(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	cr := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	k8sClient, applies := countingApplies(t, cr)
	key := client.ObjectKey{Namespace: "default", Name: CredentialsSecretName("checkout-service")}
	ensure := func() {
		t.Helper()
		if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, cr); err != nil {
			t.Fatalf("EnsureCredentialsSecret() error = %v", err)
		}
	}
	relabel := func(mutate func(map[string]string)) {
		t.Helper()
		var secret corev1.Secret
		if err := k8sClient.Get(context.Background(), key, &secret); err != nil {
			t.Fatal(err)
		}
		mutate(secret.Labels)
		if err := k8sClient.Update(context.Background(), &secret); err != nil {
			t.Fatal(err)
		}
	}

	ensure()
	relabel(func(l map[string]string) { l[depsv1alpha1.ManagedByLabelKey] = "Helm" })
	before := *applies
	ensure()
	if *applies != before {
		t.Errorf("applies = %d after the managed-by label was rewritten, want none", *applies-before)
	}

	relabel(func(l map[string]string) { delete(l, depsv1alpha1.ManagedLabelKey) })
	ensure()
	if *applies != before+1 {
		t.Errorf("applies = %d after the managed label was removed, want exactly one", *applies-before)
	}
}
