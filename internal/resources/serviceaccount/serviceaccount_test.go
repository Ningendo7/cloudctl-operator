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

package serviceaccount

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

func newCR(namespace, name, serviceAccountName string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       "test-uid",
		},
		Spec: depsv1alpha1.AppDependenciesSpec{
			ServiceAccountName: serviceAccountName,
		},
	}
}

func getSA(t *testing.T, c client.Client, namespace, name string) *corev1.ServiceAccount {
	t.Helper()
	sa := &corev1.ServiceAccount{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, sa); err != nil {
		t.Fatalf("get ServiceAccount %s/%s: %v", namespace, name, err)
	}
	return sa
}

func TestEnsure_NoRoleARN_IsNoop(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")
	cr.Status.ServiceAccountName = "whatever"

	got, err := Ensure(context.Background(), c, cr, "")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got != "whatever" {
		t.Errorf("expected status name to pass through unchanged, got %q", got)
	}
}

func TestEnsure_DefaultName_CreatesOwnedServiceAccount(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")

	got, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/ns-checkout-role")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got != "checkout" {
		t.Errorf("expected default name %q, got %q", "checkout", got)
	}

	sa := getSA(t, c, "ns", "checkout")
	if sa.Annotations[RoleARNAnnotation] != "arn:aws:iam::123456789012:role/ns-checkout-role" {
		t.Errorf("role-arn annotation = %q", sa.Annotations[RoleARNAnnotation])
	}
	owner := metav1.GetControllerOf(sa)
	if owner == nil || owner.Name != "checkout" || owner.Kind != "AppDependencies" {
		t.Errorf("expected owner reference to the CR on an operator-created default-named ServiceAccount, got %+v", owner)
	}
}

func TestEnsure_ExplicitName_CreatesUnownedServiceAccount(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "custom-sa")

	got, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/ns-checkout-role")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got != "custom-sa" {
		t.Errorf("expected explicit name %q, got %q", "custom-sa", got)
	}

	sa := getSA(t, c, "ns", "custom-sa")
	if sa.Annotations[RoleARNAnnotation] == "" {
		t.Error("expected role-arn annotation to be set")
	}
	// Never take ownership of a user-named target, even one this call had
	// to create because it didn't exist yet — its lifecycle belongs to
	// whatever the user's own manifests do with it.
	if owner := metav1.GetControllerOf(sa); owner != nil {
		t.Errorf("expected no owner reference on an explicitly user-named ServiceAccount, got %+v", owner)
	}
}

func TestEnsure_PreExisting_MergesWithoutClobberingOtherAnnotations(t *testing.T) {
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "checkout",
			Annotations: map[string]string{"team.example.com/owner": "payments"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(existing).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	sa := getSA(t, c, "ns", "checkout")
	if sa.Annotations["team.example.com/owner"] != "payments" {
		t.Errorf("expected pre-existing unrelated annotation to survive, got %q", sa.Annotations["team.example.com/owner"])
	}
	if sa.Annotations[RoleARNAnnotation] == "" {
		t.Error("expected role-arn annotation to be merged in")
	}
	// A pre-existing object is never adopted, even at the default name.
	if owner := metav1.GetControllerOf(sa); owner != nil {
		t.Errorf("expected no owner reference on a pre-existing ServiceAccount, got %+v", owner)
	}
}

// TestEnsure_Rename_CleansUpPreviousTarget deliberately establishes the old
// target's role-arn field via a real prior Ensure call (rather than
// pre-seeding it directly onto a hand-built fixture) — server-side apply's
// per-field ownership is recorded in that object's managedFields, which
// only a real Apply populates, and the "release only removes what we
// actually own" behavior this test exists to prove wouldn't be exercised
// honestly otherwise.
func TestEnsure_Rename_CleansUpPreviousTarget(t *testing.T) {
	old := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "old-sa",
			Annotations: map[string]string{"unrelated": "keep-me"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(old).Build()

	crOld := newCR("ns", "checkout", "old-sa")
	if _, err := Ensure(context.Background(), c, crOld, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure (establish old target): %v", err)
	}

	crNew := newCR("ns", "checkout", "new-sa")
	crNew.Status.ServiceAccountName = "old-sa"
	got, err := Ensure(context.Background(), c, crNew, "arn:aws:iam::123456789012:role/x")
	if err != nil {
		t.Fatalf("Ensure (rename): %v", err)
	}
	if got != "new-sa" {
		t.Errorf("expected new target %q, got %q", "new-sa", got)
	}

	oldSA := getSA(t, c, "ns", "old-sa")
	if _, ok := oldSA.Annotations[RoleARNAnnotation]; ok {
		t.Error("expected role-arn annotation released from the previous target")
	}
	if oldSA.Annotations["unrelated"] != "keep-me" {
		t.Error("expected unrelated annotation on the previous target to survive cleanup")
	}

	newSA := getSA(t, c, "ns", "new-sa")
	if newSA.Annotations[RoleARNAnnotation] == "" {
		t.Error("expected role-arn annotation set on the new target")
	}
}

// TestCleanup_StripsOwnedFieldOnly follows the same real-prior-Apply setup
// as the rename test above, for the same reason.
func TestCleanup_StripsOwnedFieldOnly(t *testing.T) {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "checkout",
			Annotations: map[string]string{"team.example.com/owner": "payments"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(sa).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure (establish claim): %v", err)
	}
	cr.Status.ServiceAccountName = "checkout"

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	got := getSA(t, c, "ns", "checkout")
	if _, ok := got.Annotations[RoleARNAnnotation]; ok {
		t.Error("expected role-arn annotation removed")
	}
	if got.Annotations["team.example.com/owner"] != "payments" {
		t.Error("expected unrelated annotation to survive")
	}
}

func TestCleanup_NoStatusName_IsNoop(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
}

func TestCleanup_ServiceAccountAlreadyGone_IsNoop(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")
	cr.Status.ServiceAccountName = "checkout"

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
}

func TestEnsure_GetErrorOtherThanNotFound_IsReturned(t *testing.T) {
	c := &erroringClient{Client: fake.NewClientBuilder().WithScheme(newScheme(t)).Build()}
	cr := newCR("ns", "checkout", "new-sa")
	cr.Status.ServiceAccountName = "previous"

	_, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x")
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

// erroringClient forces every Get to fail with a non-NotFound error, to
// exercise the "something genuinely went wrong talking to the API server"
// path (inside release, when a rename requires checking the previous
// target still exists) distinctly from "the object doesn't exist yet".
type erroringClient struct {
	client.Client
}

func (e *erroringClient) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return apierrors.NewInternalError(errBoom{})
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }
