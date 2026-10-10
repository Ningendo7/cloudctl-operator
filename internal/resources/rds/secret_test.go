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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
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
	if got := secret.Labels[depsv1alpha1.ManagedByLabelKey]; got != depsv1alpha1.ManagedByLabelValue {
		t.Errorf("label %s = %q, want %q", depsv1alpha1.ManagedByLabelKey, got, depsv1alpha1.ManagedByLabelValue)
	}
}

func TestEnsureCredentialsSecret_ConsumedInstance_RequiresAuthorization(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	producer := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	// Producer declares no sharedWith at all - the consumer must get
	// nothing.
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service", UID: "uid-2"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		}}},
	}
	k8sClient := newFakeK8sClientWithScheme(t, producer, consumer)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "fulfillment", Name: CredentialsSecretName("fulfillment-service")}, &secret)
	if err == nil {
		t.Error("expected no Secret for a consumer the producer never authorized via sharedWith")
	}
}

func TestEnsureCredentialsSecret_ConsumedInstance_AuthorizedGetsCredentials(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	producer := ownedCRWithCredentials(t, rdsClient, smClient, "default", "checkout-service", "orders-db", "cloudctl_admin", "s3cr3t-pw")
	producer.Spec.RDS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{
		{Namespace: "fulfillment", Name: "fulfillment-service"},
	}
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service", UID: "uid-2"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		}}},
	}
	k8sClient := newFakeK8sClientWithScheme(t, producer, consumer)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "fulfillment", Name: CredentialsSecretName("fulfillment-service")}, &secret); err != nil {
		t.Fatalf("getting consumer's mirrored Secret: %v", err)
	}
	wantKey := "RDS_DEFAULT_CHECKOUT_SERVICE_ORDERS_DB_USERNAME"
	if string(secret.Data[wantKey]) != "cloudctl_admin" {
		t.Errorf("secret data = %v, missing/wrong %s", secret.Data, wantKey)
	}
}

// TestEnsureCredentialsSecret_ConsumedFromSameNamedProducersInDifferentNamespaces
// guards against two different identities producing the same Secret key.
func TestEnsureCredentialsSecret_ConsumedFromSameNamedProducersInDifferentNamespaces(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	producerA := ownedCRWithCredentials(t, rdsClient, smClient, "team-a", "checkout-service", "orders-db", "user-a", "pass-a")
	producerA.Spec.RDS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: "platform", Name: "analytics"}}
	producerB := ownedCRWithCredentials(t, rdsClient, smClient, "team-b", "checkout-service", "orders-db", "user-b", "pass-b")
	producerB.Spec.RDS.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: "platform", Name: "analytics"}}

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "analytics", UID: "uid-consumer"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "team-a", Name: "checkout-service", ResourceName: "orders-db"},
			{Namespace: "team-b", Name: "checkout-service", ResourceName: "orders-db"},
		}}},
	}
	k8sClient := newFakeK8sClientWithScheme(t, producerA, producerB, consumer)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("EnsureCredentialsSecret() error = %v", err)
	}
	var secret corev1.Secret
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "platform", Name: CredentialsSecretName("analytics")}, &secret); err != nil {
		t.Fatalf("getting mirrored Secret: %v", err)
	}
	gotA := string(secret.Data["RDS_TEAM_A_CHECKOUT_SERVICE_ORDERS_DB_USERNAME"])
	gotB := string(secret.Data["RDS_TEAM_B_CHECKOUT_SERVICE_ORDERS_DB_USERNAME"])
	if gotA != "user-a" {
		t.Errorf("team-a username = %q, want %q (data: %v)", gotA, "user-a", secret.Data)
	}
	if gotB != "user-b" {
		t.Errorf("team-b username = %q, want %q (data: %v)", gotB, "user-b", secret.Data)
	}
}

func TestEnsureCredentialsSecret_ConsumedInstance_ProducerNotFoundYet(t *testing.T) {
	rdsClient := newFakeRDS()
	smClient := newFakeSecretsManager()
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "fulfillment", Name: "fulfillment-service", UID: "uid-2"},
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "default", Name: "checkout-service", ResourceName: "orders-db"},
		}}},
	}
	k8sClient := newFakeK8sClientWithScheme(t, consumer)

	if err := EnsureCredentialsSecret(context.Background(), smClient, rdsClient, k8sClient, k8sClient, consumer); err != nil {
		t.Fatalf("expected no error for an unresolved forward reference, got %v", err)
	}
}

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
		"default-checkout-service-invoices-db", "invoices-db",
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
	tests := []struct {
		name              string
		resourceName      string
		producerNamespace string
		producerCRName    string
		suffix            string
		want              string
	}{
		{name: "owned", resourceName: "orders-db", producerCRName: "", suffix: "USERNAME", want: "RDS_ORDERS_DB_USERNAME"},
		{name: "consumed", resourceName: "orders-db", producerNamespace: "default", producerCRName: "checkout-service", suffix: "PASSWORD", want: "RDS_DEFAULT_CHECKOUT_SERVICE_ORDERS_DB_PASSWORD"},
		{name: "hyphens become underscores", resourceName: "orders-db", producerCRName: "", suffix: "USERNAME", want: "RDS_ORDERS_DB_USERNAME"},
		{
			name: "same producer name, different namespace, must not collide", resourceName: "orders-db",
			producerNamespace: "team-a", producerCRName: "checkout-service", suffix: "USERNAME",
			want: "RDS_TEAM_A_CHECKOUT_SERVICE_ORDERS_DB_USERNAME",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := credentialsKey(tc.resourceName, tc.producerNamespace, tc.producerCRName, tc.suffix); got != tc.want {
				t.Errorf("credentialsKey() = %q, want %q", got, tc.want)
			}
		})
	}
}
