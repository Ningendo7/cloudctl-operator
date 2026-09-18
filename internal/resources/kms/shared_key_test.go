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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func newSchemeForSharedKeyTest(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

func TestResolveSharedKeyARN_ResolvesWhenAuthorized(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
				{Name: "shared-key", SharedWith: []depsv1alpha1.SharedWithEntry{
					{Namespace: "team-a", Name: "checkout-service"},
				}},
			}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: resourceType, Name: "shared-key", ARN: "arn:aws:kms:us-east-1:123456789012:key/shared-id"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"}
	arn, ok := ResolveSharedKeyARN(context.Background(), k8sClient, "team-a", "checkout-service", ref)
	if !ok {
		t.Fatal("expected the shared key to resolve")
	}
	if arn != "arn:aws:kms:us-east-1:123456789012:key/shared-id" {
		t.Errorf("arn = %q, want the producer's key ARN", arn)
	}
}

func TestResolveSharedKeyARN_NotAuthorizedWithoutSharedWith(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "platform-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "shared-key"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: resourceType, Name: "shared-key", ARN: "arn:aws:kms:us-east-1:123456789012:key/shared-id"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).WithObjects(producer).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"}
	_, ok := ResolveSharedKeyARN(context.Background(), k8sClient, "team-a", "checkout-service", ref)
	if ok {
		t.Fatal("expected resolution to fail without a matching sharedWith grant")
	}
}

func TestResolveSharedKeyARN_NotYetResolvableWhenProducerMissing(t *testing.T) {
	k8sClient := fake.NewClientBuilder().WithScheme(newSchemeForSharedKeyTest(t)).Build()

	ref := depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform-service", ResourceName: "shared-key"}
	_, ok := ResolveSharedKeyARN(context.Background(), k8sClient, "team-a", "checkout-service", ref)
	if ok {
		t.Fatal("expected resolution to fail when the producer CR doesn't exist yet")
	}
}
