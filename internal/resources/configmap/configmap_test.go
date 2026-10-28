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

package configmap

import (
	"context"
	"errors"
	"strings"
	"testing"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("depsv1alpha1.AddToScheme: %v", err)
	}
	return scheme
}

func getConfigMap(t *testing.T, c client.Client, namespace, name string) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
		t.Fatalf("get ConfigMap %s/%s: %v", namespace, name, err)
	}
	return cm
}

func TestEnsure_PopulatesOwnedResourceKeys(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
			SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}},
			DynamoDB: &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
				{Name: "sessions", PartitionKey: "id"},
			}},
			S3: &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
				{Type: "sns", Name: "events", ARN: "arn:aws:sns:us-east-1:123456789012:team-a-checkout-service-events"},
				{Type: "dynamodb", Name: "sessions", ARN: "arn:aws:dynamodb:us-east-1:123456789012:table/team-a-checkout-service-sessions"},
				{Type: "s3", Name: "receipts", ARN: "arn:aws:s3:::team-a-checkout-service-receipts-ab12cd34"},
			},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	want := map[string]string{
		"SQS_ORDERS_URL":               "https://sqs.us-east-1.amazonaws.com/123456789012/team-a-checkout-service-orders",
		"SNS_EVENTS_ARN":               "arn:aws:sns:us-east-1:123456789012:team-a-checkout-service-events",
		"DYNAMODB_SESSIONS_TABLE_NAME": "team-a-checkout-service-sessions",
		"S3_RECEIPTS_BUCKET":           "team-a-checkout-service-receipts-ab12cd34",
	}
	for k, v := range want {
		if cm.Data[k] != v {
			t.Errorf("Data[%q] = %q, want %q", k, cm.Data[k], v)
		}
	}
}

func TestEnsure_MirrorsAuthorizedConsumedResource(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "shared-cache", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "team-a", Name: "checkout-service"},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "shared-cache", ARN: "arn:aws:sqs:us-east-1:123456789012:team-b-platform-service-shared-cache"},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-cache"},
			}},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	wantKey := "SQS_TEAM_B_PLATFORM_SERVICE_SHARED_CACHE_URL"
	wantValue := "https://sqs.us-east-1.amazonaws.com/123456789012/team-b-platform-service-shared-cache"
	if cm.Data[wantKey] != wantValue {
		t.Errorf("Data[%q] = %q, want %q", wantKey, cm.Data[wantKey], wantValue)
	}
}

// TestEnsure_ConsumedResourcesFromSameNamedProducersInDifferentNamespaces
// guards against two different identities producing the same ConfigMap key.
func TestEnsure_ConsumedResourcesFromSameNamedProducersInDifferentNamespaces(t *testing.T) {
	newProducer := func(namespace string) *depsv1alpha1.AppDependencies {
		return &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "checkout-service"},
			Spec: depsv1alpha1.AppDependenciesSpec{
				SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
					{Name: "orders", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "platform", Name: "analytics"}}},
				}},
			},
			Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:" + namespace + "-checkout-service-orders"},
			}},
		}
	}
	producerA := newProducer("team-a")
	producerB := newProducer("team-b")
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producerA, producerB).Build()

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "analytics", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "team-a", Name: "checkout-service", ResourceName: "orders"},
				{Namespace: "team-b", Name: "checkout-service", ResourceName: "orders"},
			}},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "platform", ConfigMapName("analytics"))
	wantA := "https://sqs.us-east-1.amazonaws.com/123456789012/team-a-checkout-service-orders"
	wantB := "https://sqs.us-east-1.amazonaws.com/123456789012/team-b-checkout-service-orders"
	if got := cm.Data["SQS_TEAM_A_CHECKOUT_SERVICE_ORDERS_URL"]; got != wantA {
		t.Errorf("team-a's key = %q, want %q (data: %v)", got, wantA, cm.Data)
	}
	if got := cm.Data["SQS_TEAM_B_CHECKOUT_SERVICE_ORDERS_URL"]; got != wantB {
		t.Errorf("team-b's key = %q, want %q (data: %v)", got, wantB, cm.Data)
	}
}

func TestConnectionKV(t *testing.T) {
	tests := []struct {
		name              string
		producerNamespace string
		producerCRName    string
		wantKey           string
	}{
		{name: "owned", wantKey: "SNS_EVENTS_ARN"},
		{name: "consumed", producerNamespace: "team-b", producerCRName: "platform-service", wantKey: "SNS_TEAM_B_PLATFORM_SERVICE_EVENTS_ARN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, _, err := connectionKV(resourceTypeSNS, "events", "arn:aws:sns:us-east-1:123456789012:events", testRegion, testAccountID, tc.producerNamespace, tc.producerCRName)
			if err != nil {
				t.Fatalf("connectionKV() error = %v", err)
			}
			if key != tc.wantKey {
				t.Errorf("connectionKV() key = %q, want %q", key, tc.wantKey)
			}
		})
	}
}

func TestSetRDSConnectionKeys(t *testing.T) {
	info := rds.ConnectionInfo{Host: "host.example", Port: 5432, Engine: "postgres", DBInstanceIdentifier: "id"}

	owned := rds.NewKeyedData()
	if err := setRDSConnectionKeys(owned, "orders-db", "", "", info); err != nil {
		t.Fatalf("owned: %v", err)
	}
	if _, ok := owned.Values()["RDS_ORDERS_DB_HOST"]; !ok {
		t.Errorf("owned: expected RDS_ORDERS_DB_HOST, got %v", owned.Values())
	}

	consumed := rds.NewKeyedData()
	if err := setRDSConnectionKeys(consumed, "orders-db", "team-a", "checkout-service", info); err != nil {
		t.Fatalf("consumed: %v", err)
	}
	if _, ok := consumed.Values()["RDS_TEAM_A_CHECKOUT_SERVICE_ORDERS_DB_HOST"]; !ok {
		t.Errorf("consumed: expected RDS_TEAM_A_CHECKOUT_SERVICE_ORDERS_DB_HOST, got %v", consumed.Values())
	}
}

func TestEnsure_OmitsUnauthorizedConsumedResource(t *testing.T) {
	// Same as above, but the producer never granted this consumer access.
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "shared-cache"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "shared-cache", ARN: "arn:aws:sqs:us-east-1:123456789012:team-b-platform-service-shared-cache"},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build()

	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
				{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-cache"},
			}},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Nothing owned and nothing authorized to consume: the ConfigMap exists
	// and is empty, so a pod that reads it starts and gets no values, rather
	// than failing to start.
	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if len(cm.Data) != 0 {
		t.Fatalf("an unauthorized consume must not leak a value, got %v", cm.Data)
	}
}

func TestEnsure_SetsOwnerReferenceForNativeGC(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	owner := metav1.GetControllerOf(cm)
	if owner == nil || owner.Name != "checkout-service" || owner.Kind != "AppDependencies" {
		t.Errorf("expected an owner reference to the CR, got %+v", owner)
	}
}

func TestEnsure_UpdatesExistingConfigMapOnDataChange(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Simulate the queue having been recreated with a new logical ARN
	// (contrived, but exercises the update path deterministically).
	cr.Status.ManagedResources[0].ARN = "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders-v2"
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure (update): %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	want := "https://sqs.us-east-1.amazonaws.com/123456789012/team-a-checkout-service-orders-v2"
	if cm.Data["SQS_ORDERS_URL"] != want {
		t.Errorf("Data[SQS_ORDERS_URL] = %q, want %q", cm.Data["SQS_ORDERS_URL"], want)
	}
}

func TestEnsure_SanitizesKeysForEnvVarCompatibility(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			S3: &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "old-invoices"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "s3", Name: "old-invoices", ARN: "arn:aws:s3:::team-a-checkout-service-old-invoices-ab12cd34"},
			},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if _, ok := cm.Data["S3_OLD_INVOICES_BUCKET"]; !ok {
		t.Errorf("expected hyphens in the resource name to be sanitized to underscores in the key, got keys: %v", keysOf(cm.Data))
	}
}

func TestEnsure_KeepsAnEmptyConfigMapWhenNothingRemainsToReport(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Everything removed from spec and the ledger, as ordinary Cleanup
	// would leave things once the queue itself is actually gone.
	cr.Spec.SQS = nil
	cr.Status.ManagedResources = nil
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure (now empty): %v", err)
	}

	// Kept, empty: deleting it would make a pod that reads it with envFrom
	// fail with CreateContainerConfigError at its next restart.
	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if len(cm.Data) != 0 {
		t.Errorf("expected the ConfigMap to be emptied, got %v", cm.Data)
	}
}

func TestEnsure_ACRWithNothingToReportYetStillGetsAConfigMap(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec:       depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}},
		// No ledger entry yet: the queue is not created.
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if owner := metav1.GetControllerOf(cm); owner == nil || owner.UID != cr.UID {
		t.Errorf("the empty ConfigMap must be owned by the CR so it goes with it, got %+v", owner)
	}
}

// TestEnsure_PreExistingForeignConfigMap_Refused guards against silently
// overwriting a same-named ConfigMap this CR doesn't own.
func TestEnsure_PreExistingForeignConfigMap_Refused(t *testing.T) {
	foreign := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-a",
			Name:      ConfigMapName("checkout-service"),
		},
		Data: map[string]string{"UNRELATED": "keep-me"},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(foreign).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}

	err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr)
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.NameTaken {
		t.Fatalf("expected a %s refusal for a ConfigMap it doesn't own, got %v", reasons.NameTaken, err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if cm.Data["UNRELATED"] != "keep-me" || len(cm.Data) != 1 {
		t.Errorf("expected the foreign ConfigMap's data untouched, got %v", cm.Data)
	}
}

// TestEnsure_PreExistingForeignConfigMap_EmptyData_NotDeleted covers the
// same guard on the delete path, which is more severe than an overwrite.
func TestEnsure_ForeignConfigMapHiddenFromCache_Refused(t *testing.T) {
	foreign := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: ConfigMapName("checkout-service")},
		Data:       map[string]string{"UNRELATED": "keep-me"},
	}
	cached := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	apiServer := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(foreign).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}

	if err := Ensure(context.Background(), cached, apiServer, nil, testRegion, testAccountID, cr); err == nil {
		t.Fatal("expected Ensure to refuse a foreign ConfigMap the label-filtered cache can't see")
	}
	if cm := getConfigMap(t, apiServer, "team-a", ConfigMapName("checkout-service")); cm.Data["UNRELATED"] != "keep-me" {
		t.Errorf("foreign ConfigMap was modified: %v", cm.Data)
	}
}

func TestEnsure_LabelsConfigMapAsManaged(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	for key, want := range map[string]string{
		depsv1alpha1.ManagedLabelKey:   depsv1alpha1.ManagedLabelValue,
		depsv1alpha1.ManagedByLabelKey: depsv1alpha1.ManagedByLabelValue,
	} {
		if got := cm.Labels[key]; got != want {
			t.Errorf("label %s = %q, want %q", key, got, want)
		}
	}
}

func TestEnsure_PreExistingForeignConfigMap_EmptyData_NotDeleted(t *testing.T) {
	foreign := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-a",
			Name:      ConfigMapName("checkout-service"),
		},
		Data: map[string]string{"UNRELATED": "keep-me"},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(foreign).Build()
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
	}

	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err == nil {
		t.Fatal("expected Ensure to refuse deleting a ConfigMap it doesn't own")
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if cm.Data["UNRELATED"] != "keep-me" {
		t.Error("expected the foreign ConfigMap to survive untouched")
	}
}

func keysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// withFailingCRReadsCM wraps c so that reading any AppDependencies fails, as
// a cache or API-server hiccup would, once *failing is true.
func withFailingCRReadsCM(c client.WithWatch, failing *bool) client.Client {
	return interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isCR := obj.(*depsv1alpha1.AppDependencies); isCR && *failing {
				return errors.New("injected: API server unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
}

// Dropping a consumed resource's key on a failed read would take its env
// var away from pods that restart before the next pass.
func TestEnsure_KeepsConsumedKeysWhenAProducerCannotBeRead(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
			{Name: "shared-cache", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: "team-a", Name: "checkout-service"}}},
		}}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: "sqs", Name: "shared-cache", ARN: "arn:aws:sqs:us-east-1:123456789012:team-b-platform-service-shared-cache"},
		}},
	}
	failing := false
	c := withFailingCRReadsCM(fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(producer).Build(), &failing)
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-cache"},
		}}},
	}
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}

	failing = true
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer); err == nil {
		t.Fatal("expected an error when the producer can't be read")
	}
	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if cm.Data["SQS_TEAM_B_PLATFORM_SERVICE_SHARED_CACHE_URL"] == "" {
		t.Error("the consumed queue's key was removed because the producer couldn't be read")
	}
}

func sqsProducer(namespace, crName, queue string, shareWith ...depsv1alpha1.SharedWithEntry) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
			{Name: queue, SharedWith: shareWith},
		}}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: "sqs", Name: queue, ARN: "arn:aws:sqs:us-east-1:123456789012:" + namespace + "-" + crName + "-" + queue},
		}},
	}
}

func assertCollision(t *testing.T, err error, sources ...string) {
	t.Helper()
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.ConnectionKeyCollision {
		t.Fatalf("expected %s, got %v", reasons.ConnectionKeyCollision, err)
	}
	for _, s := range sources {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("the error should name %q: %v", s, err)
		}
	}
}

// Different producers can fold into one key: team-a/orders and team/a-orders
// both give SQS_TEAM_A_ORDERS_Q_URL. The app would connect to the wrong
// queue, so nothing is written.
func TestEnsure_RefusesConsumedResourcesWhoseKeysCollide(t *testing.T) {
	share := depsv1alpha1.SharedWithEntry{Namespace: "consumer-ns", Name: "app"}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(
		sqsProducer("team-a", "orders", "q", share), sqsProducer("team", "a-orders", "q", share),
	).Build()
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "consumer-ns", Name: "app", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "team-a", Name: "orders", ResourceName: "q"},
			{Namespace: "team", Name: "a-orders", ResourceName: "q"},
		}}},
	}

	err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer)
	assertCollision(t, err, "team-a/orders", "team/a-orders")
	if getErr := c.Get(context.Background(), client.ObjectKey{Namespace: "consumer-ns", Name: ConfigMapName("app")}, &corev1.ConfigMap{}); !apierrors.IsNotFound(getErr) {
		t.Errorf("no ConfigMap should be written on a collision, got %v", getErr)
	}
}

func TestEnsure_RefusesAnOwnedResourceWhoseKeyCollidesWithAConsumedOne(t *testing.T) {
	share := depsv1alpha1.SharedWithEntry{Namespace: "consumer-ns", Name: "app"}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(sqsProducer("team-a", "orders", "q", share)).Build()
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "consumer-ns", Name: "app", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{
			Resources: []depsv1alpha1.SQSQueueSpec{{Name: "team-a-orders-q"}},
			Consumes:  []depsv1alpha1.ConsumeRef{{Namespace: "team-a", Name: "orders", ResourceName: "q"}},
		}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: "sqs", Name: "team-a-orders-q", ARN: "arn:aws:sqs:us-east-1:123456789012:consumer-ns-app-team-a-orders-q"},
		}},
	}

	assertCollision(t, Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer), "team-a-orders-q", "team-a/orders")
}

// A collision must not take down a CR whose keys are all distinct, or the
// same resource listed twice.
func TestEnsure_DistinctAndRepeatedConsumedResourcesAreNotCollisions(t *testing.T) {
	share := depsv1alpha1.SharedWithEntry{Namespace: "consumer-ns", Name: "app"}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(
		sqsProducer("team-a", "orders", "q", share), sqsProducer("team-b", "orders", "q", share),
	).Build()
	consumer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "consumer-ns", Name: "app", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{
			{Namespace: "team-a", Name: "orders", ResourceName: "q"},
			{Namespace: "team-b", Name: "orders", ResourceName: "q"},
			{Namespace: "team-a", Name: "orders", ResourceName: "q"},
		}}},
	}
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	cm := getConfigMap(t, c, "consumer-ns", ConfigMapName("app"))
	if len(cm.Data) != 2 {
		t.Errorf("expected the two distinct queues, got %v", cm.Data)
	}
}

// A failed read of the producer says nothing about whether it still shares
// the instance, so it must not drop the keys of a consumed instance.
func TestAddConsumedRDS_AFailedReadOfTheProducerIsAnError(t *testing.T) {
	failing := true
	c := withFailingCRReadsCM(fake.NewClientBuilder().WithScheme(newScheme(t)).Build(), &failing)
	consumer := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "consumer-ns", Name: "app"}}
	ref := depsv1alpha1.ConsumeRef{Namespace: "team-a", Name: "orders", ResourceName: "db"}

	if err := addConsumedRDS(context.Background(), c, nil, rds.NewKeyedData(), consumer, ref); err == nil {
		t.Fatal("expected an error, not a silent skip, when the producer can't be read")
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

func TestEnsure_AppliesOnlyWhenSomethingDiffers(t *testing.T) {
	c, applies := countingApplies(t)
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}
	ensure := func() {
		t.Helper()
		if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
	}

	ensure()
	ensure()
	if *applies != 1 {
		t.Fatalf("applies = %d after two passes with nothing changed, want 1", *applies)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	cm.Data["SQS_ORDERS_URL"] = "hand-edited"
	if err := c.Update(context.Background(), cm); err != nil {
		t.Fatalf("Update: %v", err)
	}
	ensure()
	if *applies != 2 {
		t.Fatalf("applies = %d after a hand edit, want 2", *applies)
	}
	if got := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service")).Data["SQS_ORDERS_URL"]; got == "hand-edited" {
		t.Error("the hand edit was not repaired")
	}

	cr.Status.ManagedResources[0].ARN += "-v2"
	ensure()
	if *applies != 3 {
		t.Errorf("applies = %d after the data changed, want 3", *applies)
	}
}

func TestEnsure_AppliesWhenMatchingDataWasWrittenByAnotherManager(t *testing.T) {
	// Taking the fields over matters: a later apply that drops a key only
	// removes it if this operator's manager owns it.
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}
	isController := true
	restored := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-a", Name: ConfigMapName("checkout-service"),
			Labels: map[string]string{depsv1alpha1.ManagedLabelKey: depsv1alpha1.ManagedLabelValue},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: depsv1alpha1.GroupVersion.String(), Kind: "AppDependencies",
				Name: "checkout-service", UID: "uid-1", Controller: &isController,
			}},
		},
		Data: map[string]string{"SQS_ORDERS_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/team-a-checkout-service-orders"},
	}
	c, applies := countingApplies(t, restored)
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if *applies != 1 {
		t.Errorf("applies = %d, want 1", *applies)
	}
}

func connectionCR() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: "sqs", Name: "orders", ARN: "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders"},
			},
		},
	}
}

// Admission policies rewrite the standard managed-by label. That must neither
// hide the ConfigMap from the cache nor make the operator apply it again on
// every pass, because only its own label counts.
func TestEnsure_ARewrittenManagedByLabelIsNotRepairedOverAndOver(t *testing.T) {
	c, applies := countingApplies(t)
	cr := connectionCR()
	ensure := func() {
		t.Helper()
		if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
	}
	ensure()

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	cm.Labels[depsv1alpha1.ManagedByLabelKey] = "Helm"
	if err := c.Update(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	before := *applies
	ensure()
	if *applies != before {
		t.Errorf("applies = %d after the managed-by label was rewritten, want none", *applies-before)
	}
}

// A ConfigMap written before the operator's own label existed carries only
// the standard one, so the cache can no longer see it; the next pass finds it
// through the API reader and puts the new label on.
func TestEnsure_AConfigMapWithOnlyTheOldLabelGetsTheNewOne(t *testing.T) {
	c, applies := countingApplies(t)
	cr := connectionCR()
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	delete(cm.Labels, depsv1alpha1.ManagedLabelKey)
	if err := c.Update(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	before := *applies
	if err := Ensure(context.Background(), c, c, nil, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if *applies != before+1 {
		t.Errorf("applies = %d, want exactly one", *applies-before)
	}
	if got := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service")).Labels[depsv1alpha1.ManagedLabelKey]; got != depsv1alpha1.ManagedLabelValue {
		t.Errorf("label %s = %q, want %q", depsv1alpha1.ManagedLabelKey, got, depsv1alpha1.ManagedLabelValue)
	}
}
