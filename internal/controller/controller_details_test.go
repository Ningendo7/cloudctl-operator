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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func newFinalizerTestClient(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&depsv1alpha1.AppDependencies{}).WithInterceptorFuncs(funcs).Build()
}

func queueCR() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service", UID: "uid-1"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
	}
}

func TestEnsureFinalizer_SendsOnlyTheFinalizerListAndTheResourceVersion(t *testing.T) {
	var sent string
	c := newFinalizerTestClient(t, interceptor.Funcs{
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			return errors.New("a full update sends the whole object, and can write back a field this version does not know")
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			data, err := patch.Data(obj)
			if err != nil {
				return err
			}
			sent = string(data)
			return c.Patch(ctx, obj, patch, opts...)
		},
	}, queueCR())
	cr := &depsv1alpha1.AppDependencies{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "checkout-service"}, cr); err != nil {
		t.Fatal(err)
	}

	if err := ensureFinalizer(context.Background(), c, cr); err != nil {
		t.Fatalf("ensureFinalizer() error = %v", err)
	}

	if !strings.Contains(sent, finalizerName) || !strings.Contains(sent, "resourceVersion") || strings.Contains(sent, "spec") {
		t.Errorf("the patch should carry the finalizers and the resource version only, got %s", sent)
	}
	if !hasFinalizer(cr) {
		t.Error("the finalizer should be on the object afterwards")
	}
}

func TestRemoveFinalizer_AConcurrentFinalizerIsNotDropped(t *testing.T) {
	cr := queueCR()
	cr.Finalizers = []string{finalizerName}
	c := newFinalizerTestClient(t, interceptor.Funcs{}, cr)

	stale := &depsv1alpha1.AppDependencies{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), stale); err != nil {
		t.Fatal(err)
	}
	// Someone else adds a finalizer after we read the object.
	other := stale.DeepCopy()
	other.Finalizers = append(other.Finalizers, "example.com/other")
	if err := c.Update(context.Background(), other); err != nil {
		t.Fatal(err)
	}

	err := removeFinalizer(context.Background(), c, stale)
	if !apierrors.IsConflict(err) {
		t.Fatalf("removeFinalizer() error = %v, want a conflict, not a patch that replaces the other finalizer", err)
	}
	got := &depsv1alpha1.AppDependencies{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), got); err != nil {
		t.Fatal(err)
	}
	if len(got.Finalizers) != 2 {
		t.Errorf("finalizers = %v, both should remain", got.Finalizers)
	}
}

func TestStatusPatch_WithoutABaselineSendsTheWholeStatus(t *testing.T) {
	cr := queueCR()
	cr.Status.ManagedResources = []depsv1alpha1.ManagedResource{{Type: "sqs", Name: "orders"}}

	data, err := statusPatch(nil, cr).Data(cr)
	if err != nil {
		t.Fatalf("statusPatch(nil, cr) error = %v", err)
	}
	if !strings.Contains(string(data), "orders") {
		t.Errorf("a patch with no baseline should carry the whole status, got %s", data)
	}
}

func failStatusPatches(r *AppDependenciesReconciler) {
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			return errors.New("the API server is unavailable")
		},
	})
}

func TestReconcileDelete_AFailedStatusWriteIsReturnedWhileResourcesRemain(t *testing.T) {
	cr := newStuckDeletionCR()
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	seedOwnedNonEmptyQueue(r.AWSClients.SQS.(*fakeSQSClient), cr)
	failStatusPatches(r)

	_, err := r.reconcileDelete(context.Background(), cr)
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("reconcileDelete() error = %v, want the failed status write, so it is retried", err)
	}
	if !hasFinalizer(cr) {
		t.Error("the finalizer must stay while resources remain")
	}
}

func TestReconcileDelete_AFailedStatusWriteDoesNotHoldUpAFinishedDeletion(t *testing.T) {
	cr := newStuckDeletionCR()
	cr.Annotations = map[string]string{ForceDeleteAllAnnotation: "true"}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	seedOwnedNonEmptyQueue(r.AWSClients.SQS.(*fakeSQSClient), cr)
	failStatusPatches(r)

	if _, err := r.reconcileDelete(context.Background(), cr); err != nil {
		t.Fatalf("reconcileDelete() error = %v, a status nobody will read must not block the finalizer", err)
	}
	if hasFinalizer(cr) {
		t.Error("the finalizer should be removed once everything is gone")
	}
}

func TestReconcileDelete_AnUnchangedStatusIsNotWritten(t *testing.T) {
	cr := newStuckDeletionCR()
	cr.Status = depsv1alpha1.AppDependenciesStatus{}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)

	if _, err := r.reconcileDelete(context.Background(), cr); err != nil {
		t.Fatalf("reconcileDelete() error = %v", err)
	}
	if patchCount != 0 {
		t.Errorf("a deletion pass that changed no status wrote it %d times", patchCount)
	}
	if hasFinalizer(cr) {
		t.Error("with nothing to delete the finalizer should be removed")
	}
}
