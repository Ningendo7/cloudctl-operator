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
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
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

	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
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

	if err := Ensure(context.Background(), c, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	wantKey := "SQS_PLATFORM_SERVICE_SHARED_CACHE_URL"
	wantValue := "https://sqs.us-east-1.amazonaws.com/123456789012/team-b-platform-service-shared-cache"
	if cm.Data[wantKey] != wantValue {
		t.Errorf("Data[%q] = %q, want %q", wantKey, cm.Data[wantKey], wantValue)
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

	if err := Ensure(context.Background(), c, testRegion, testAccountID, consumer); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Nothing owned and nothing authorized to consume - no ConfigMap
	// should even be created, same "no pointless empty artifact"
	// philosophy as IAM skipping an empty role.
	cm := &corev1.ConfigMap{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "team-a", Name: ConfigMapName("checkout-service")}, cm)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no ConfigMap to be created for an unauthorized consume, got err=%v", err)
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

	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
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
	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Simulate the queue having been recreated with a new logical ARN
	// (contrived, but exercises the update path deterministically).
	cr.Status.ManagedResources[0].ARN = "arn:aws:sqs:us-east-1:123456789012:team-a-checkout-service-orders-v2"
	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
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

	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cm := getConfigMap(t, c, "team-a", ConfigMapName("checkout-service"))
	if _, ok := cm.Data["S3_OLD_INVOICES_BUCKET"]; !ok {
		t.Errorf("expected hyphens in the resource name to be sanitized to underscores in the key, got keys: %v", keysOf(cm.Data))
	}
}

func TestEnsure_DeletesConfigMapWhenNothingRemainsToReport(t *testing.T) {
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
	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Everything removed from spec and the ledger, as ordinary Cleanup
	// would leave things once the queue itself is actually gone.
	cr.Spec.SQS = nil
	cr.Status.ManagedResources = nil
	if err := Ensure(context.Background(), c, testRegion, testAccountID, cr); err != nil {
		t.Fatalf("Ensure (now empty): %v", err)
	}

	cm := &corev1.ConfigMap{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "team-a", Name: ConfigMapName("checkout-service")}, cm)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected the ConfigMap to be deleted once nothing remains to report, got err=%v", err)
	}
}

func keysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
