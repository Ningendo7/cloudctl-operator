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
	"testing"

	"github.com/aws/smithy-go"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// logCall is one recorded Info/Error call from fakeLogSink.
type logCall struct {
	isError bool
	err     error
	msg     string
	kvs     []any
}

func (c logCall) kv(key string) (any, bool) {
	for i := 0; i+1 < len(c.kvs); i += 2 {
		if c.kvs[i] == key {
			return c.kvs[i+1], true
		}
	}
	return nil, false
}

// fakeLogSink is a minimal logr.LogSink recording every Info/Error call, so
// tests can assert on what got logged without a real logging backend.
type fakeLogSink struct {
	calls *[]logCall
}

func newFakeLogger() (logr.Logger, *[]logCall) {
	calls := &[]logCall{}
	return logr.New(&fakeLogSink{calls: calls}), calls
}

func (f *fakeLogSink) Init(_ logr.RuntimeInfo) {}
func (f *fakeLogSink) Enabled(_ int) bool      { return true }
func (f *fakeLogSink) Info(_ int, msg string, kvs ...any) {
	*f.calls = append(*f.calls, logCall{msg: msg, kvs: kvs})
}
func (f *fakeLogSink) Error(err error, msg string, kvs ...any) {
	*f.calls = append(*f.calls, logCall{isError: true, err: err, msg: msg, kvs: kvs})
}
func (f *fakeLogSink) WithValues(_ ...any) logr.LogSink { return f }
func (f *fakeLogSink) WithName(_ string) logr.LogSink   { return f }

func TestSetSectionCondition_LogsErrorWithSectionAndReason(t *testing.T) {
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	cr := &depsv1alpha1.AppDependencies{}

	setSectionCondition(ctx, cr, "S3Ready", errors.New("boom"))

	if len(*calls) != 1 {
		t.Fatalf("expected exactly one log call, got %d: %+v", len(*calls), *calls)
	}
	c := (*calls)[0]
	if !c.isError {
		t.Fatalf("expected an Error-level log call, got %+v", c)
	}
	if section, _ := c.kv("section"); section != "S3Ready" {
		t.Errorf("expected section=S3Ready, got %v", section)
	}
	if reason, _ := c.kv("reason"); reason != "Error" {
		t.Errorf("expected reason=Error for a plain error, got %v", reason)
	}
}

func TestSetSectionCondition_LogsPermissionDeniedReason(t *testing.T) {
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	cr := &depsv1alpha1.AppDependencies{}

	setSectionCondition(ctx, cr, "IAMReady", &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient})

	if reason, _ := (*calls)[0].kv("reason"); reason != "PermissionDenied" {
		t.Errorf("expected reason=PermissionDenied, got %v", reason)
	}
}

func TestSetSectionCondition_LogsTransientErrorReason(t *testing.T) {
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	cr := &depsv1alpha1.AppDependencies{}

	err := &cloudctlaws.ReconcileError{Err: errors.New("throttled"), Retryable: true}
	setSectionCondition(ctx, cr, "SQSReady", err)

	if reason, _ := (*calls)[0].kv("reason"); reason != "TransientError" {
		t.Errorf("expected reason=TransientError, got %v", reason)
	}
}

func TestSetSectionCondition_NoLogOnSuccess(t *testing.T) {
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	cr := &depsv1alpha1.AppDependencies{}

	setSectionCondition(ctx, cr, "S3Ready", nil)

	if len(*calls) != 0 {
		t.Errorf("expected no log calls on success, got %+v", *calls)
	}
}

func newSQSBackedCR() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
	}
}

func TestReconcileNormal_SuccessPath_LogsStartAndSuccessNoErrors(t *testing.T) {
	cr := newSQSBackedCR()
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var sawStart, sawSuccess bool
	for _, c := range *calls {
		if c.isError {
			t.Errorf("expected no Error-level logs on a successful reconcile, got %+v", c)
		}
		switch c.msg {
		case "Starting reconcile":
			sawStart = true
		case "Reconcile succeeded":
			sawSuccess = true
		}
	}
	if !sawStart || !sawSuccess {
		t.Errorf("expected both 'Starting reconcile' and 'Reconcile succeeded' logs, got %+v", *calls)
	}
}

// A retryable (transient) failure is requeued, not surfaced as a hard
// error — logging it via log.Info rather than log.Error was a deliberate
// choice to avoid false alerting on something that's expected to
// self-resolve on the next reconcile.
func TestReconcileNormal_RetryableError_LogsInfoNotError(t *testing.T) {
	cr := newSQSBackedCR()
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	r.AWSClients.SQS.(*fakeSQSClient).createQueueErr = &fakeAWSError{code: "InternalError", fault: smithy.FaultServer}
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile() returned an error for a retryable failure, want nil error + RequeueAfter: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a RequeueAfter for a retryable failure, got zero")
	}

	// The section itself still logs its own failure at Error level (tagged
	// reason=TransientError) - that's section-level diagnostics, not the
	// thing being avoided here. What must NOT happen is reconcileNormal's
	// own top-level "Reconcile failed" log, which is reserved for hard
	// failures the requeue won't self-resolve.
	var sawRequeueLog, sawTopLevelFailureLog bool
	for _, c := range *calls {
		switch c.msg {
		case "Reconcile hit a transient error, requeuing":
			sawRequeueLog = true
		case "Reconcile failed":
			sawTopLevelFailureLog = true
		}
	}
	if !sawRequeueLog {
		t.Errorf("expected an Info log for the transient-error requeue, got %+v", *calls)
	}
	if sawTopLevelFailureLog {
		t.Errorf("expected no top-level 'Reconcile failed' log for a retryable failure, got %+v", *calls)
	}
}

// A non-retryable failure (e.g. permissions) is a hard error and does get
// logged at Error level, unlike the transient case above.
func TestReconcileNormal_HardError_LogsError(t *testing.T) {
	cr := newSQSBackedCR()
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	r.AWSClients.SQS.(*fakeSQSClient).createQueueErr = &fakeAWSError{code: "AccessDenied", fault: smithy.FaultClient}
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("expected Reconcile() to return an error for a non-retryable failure")
	}

	var sawReconcileFailed bool
	for _, c := range *calls {
		if c.isError && c.msg == "Reconcile failed" {
			sawReconcileFailed = true
		}
	}
	if !sawReconcileFailed {
		t.Errorf("expected an Error log for 'Reconcile failed', got %+v", *calls)
	}
}

func TestReconcileDelete_NoFinalizer_LogsNothing(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service"},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)

	if _, err := r.reconcileDelete(ctx, cr); err != nil {
		t.Fatalf("reconcileDelete() error = %v", err)
	}

	if len(*calls) != 0 {
		t.Errorf("expected no log calls when there's no finalizer to run (a no-op), got %+v", *calls)
	}
}

func TestReconcileDelete_Success_LogsStartAndFinalizerRemoved(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "checkout-service",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
		},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)

	if _, err := r.reconcileDelete(ctx, cr); err != nil {
		t.Fatalf("reconcileDelete() error = %v", err)
	}

	var sawStart, sawDone bool
	for _, c := range *calls {
		if c.isError {
			t.Errorf("expected no Error-level logs on a successful deletion, got %+v", c)
		}
		switch c.msg {
		case "Starting deletion reconcile":
			sawStart = true
		case "Deletion reconcile succeeded, finalizer removed":
			sawDone = true
		}
	}
	if !sawStart || !sawDone {
		t.Errorf("expected both 'Starting deletion reconcile' and completion logs, got %+v", *calls)
	}
}

// Mirrors TestReconcileNormal_RetryableError_LogsInfoNotError for the
// deletion path: a transient failure during cleanup is also requeued via
// Info, not Error, for the same false-alerting reason.
func TestReconcileDelete_RetryableError_LogsInfoNotError(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "checkout-service",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
		},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{
			ManagedResources: []depsv1alpha1.ManagedResource{
				{
					Type:           "sqs",
					Name:           "orders",
					ARN:            "arn:aws:sqs:us-east-1:123456789012:orders",
					State:          depsv1alpha1.ManagedResourceStateVerified,
					DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
				},
			},
		},
	}
	patchCount := 0
	r := newCountingReconciler(t, &patchCount, cr)
	r.AWSClients.SQS.(*fakeSQSClient).getQueueUrlErr = &fakeAWSError{code: "InternalError", fault: smithy.FaultServer}
	logger, calls := newFakeLogger()
	ctx := logf.IntoContext(context.Background(), logger)

	result, err := r.reconcileDelete(ctx, cr)
	if err != nil {
		t.Fatalf("reconcileDelete() returned an error for a retryable failure, want nil error + RequeueAfter: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a RequeueAfter for a retryable failure during deletion, got zero")
	}

	var sawRequeueLog, sawTopLevelFailureLog bool
	for _, c := range *calls {
		switch c.msg {
		case "Deletion reconcile hit a transient error, requeuing":
			sawRequeueLog = true
		case "Deletion reconcile failed":
			sawTopLevelFailureLog = true
		}
	}
	if !sawRequeueLog {
		t.Errorf("expected an Info log for the transient-error requeue during deletion, got %+v", *calls)
	}
	if sawTopLevelFailureLog {
		t.Errorf("expected no top-level 'Deletion reconcile failed' log for a retryable failure, got %+v", *calls)
	}
}
