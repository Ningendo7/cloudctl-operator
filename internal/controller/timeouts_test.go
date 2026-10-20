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
	"fmt"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func timeoutOf(ctx context.Context, t *testing.T) time.Duration {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a deadline on the returned context")
	}
	return time.Until(deadline)
}

func TestSectionContext_UsesBaseTimeoutWithNothingDeclaredOrInLedger(t *testing.T) {
	ctx, cancel := sectionContext(context.Background(), nil, "sqs", 0)
	defer cancel()

	got := timeoutOf(ctx, t)
	if got > baseSectionTimeout || got < baseSectionTimeout-time.Second {
		t.Errorf("expected roughly baseSectionTimeout (%v), got %v", baseSectionTimeout, got)
	}
}

func TestSectionContext_ScalesWithDeclaredCount(t *testing.T) {
	ctx, cancel := sectionContext(context.Background(), nil, "sqs", 10)
	defer cancel()

	want := baseSectionTimeout + 10*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v for 10 declared resources, got %v", want, got)
	}
}

func TestSectionContext_UsesLedgerCountWhenLargerThanDeclared(t *testing.T) {
	// Mirrors the finalize path: nothing is "declared" during deletion, so
	// the ledger count - not the (irrelevant) declared count - must drive
	// the timeout, or a CR with many resources still in the ledger would
	// get a timeout sized as if it had none.
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "a"},
		{Type: "sqs", Name: "b"},
		{Type: "sqs", Name: "c"},
	}
	ctx, cancel := sectionContext(context.Background(), ledger, "sqs", 0)
	defer cancel()

	want := baseSectionTimeout + 3*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v driven by ledger count, got %v", want, got)
	}
}

func TestSectionContext_OnlyCountsLedgerEntriesOfTheGivenResourceType(t *testing.T) {
	ledger := []depsv1alpha1.ManagedResource{
		{Type: "sqs", Name: "a"},
		{Type: "sns", Name: "b"},
		{Type: "sns", Name: "c"},
		{Type: "dynamodb", Name: "d"},
	}
	ctx, cancel := sectionContext(context.Background(), ledger, "sns", 0)
	defer cancel()

	want := baseSectionTimeout + 2*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v counting only sns entries, got %v", want, got)
	}
}

func TestSectionContext_UsesDeclaredCountWhenLargerThanLedger(t *testing.T) {
	// The normal reconcile path: a CR that just added several new
	// resources has more declared than are in the ledger yet.
	ledger := []depsv1alpha1.ManagedResource{{Type: "dynamodb", Name: "a"}}
	ctx, cancel := sectionContext(context.Background(), ledger, "dynamodb", 5)
	defer cancel()

	want := baseSectionTimeout + 5*perResourceTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v driven by declared count, got %v", want, got)
	}
}

func TestSectionDeletionContext_UsesTheLargerDeletionBudget(t *testing.T) {
	// Deletion does strictly more work per resource than creation (S3's
	// per-object-version cleanup being the extreme case) - its budget must
	// never be reused from, or smaller than, provisioning's.
	if baseSectionDeletionTimeout <= baseSectionTimeout {
		t.Errorf("baseSectionDeletionTimeout (%v) must be greater than baseSectionTimeout (%v)", baseSectionDeletionTimeout, baseSectionTimeout)
	}
	if perResourceDeletionTimeout <= perResourceTimeout {
		t.Errorf("perResourceDeletionTimeout (%v) must be greater than perResourceTimeout (%v)", perResourceDeletionTimeout, perResourceTimeout)
	}

	ledger := []depsv1alpha1.ManagedResource{
		{Type: "s3", Name: "a"},
		{Type: "s3", Name: "b"},
	}
	ctx, cancel := sectionDeletionContext(context.Background(), ledger, "s3", 0)
	defer cancel()

	want := baseSectionDeletionTimeout + 2*perResourceDeletionTimeout
	got := timeoutOf(ctx, t)
	if got > want || got < want-time.Second {
		t.Errorf("expected roughly %v driven by ledger count under the deletion budget, got %v", want, got)
	}

	provisioningWant := baseSectionTimeout + 2*perResourceTimeout
	if got <= provisioningWant {
		t.Errorf("expected the deletion timeout (%v) to exceed the equivalent provisioning timeout (%v)", got, provisioningWant)
	}
}

func TestIAMWorkloadCount(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{Spec: depsv1alpha1.AppDependenciesSpec{
		SQS: &depsv1alpha1.SQSSpec{
			Resources: []depsv1alpha1.SQSQueueSpec{
				{Name: "plain"},
				{Name: "dedicated-key", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
				{Name: "shared-key", Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}},
				{Name: "encryption-off", Encryption: &depsv1alpha1.EncryptionSpec{}},
			},
			Consumes: []depsv1alpha1.ConsumeRef{{Namespace: "a", Name: "b", ResourceName: "q"}},
		},
		S3: &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{
			{Name: "bucket", Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
		}},
		RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "db"}}},
	}}

	// 4 queues + 1 consume + 2 encrypted queues, 1 bucket + 1 encrypted
	// bucket; the RDS instance never feeds IAM.
	if got, want := iamWorkloadCount(cr), 9; got != want {
		t.Errorf("iamWorkloadCount() = %d, want %d", got, want)
	}
}

// The connection-info step runs after every section, and calls RDS and
// Secrets Manager, so it needs a deadline of its own.
func TestConnectionInfoContext_HasADeadlineThatScalesWithRDS(t *testing.T) {
	cr := &depsv1alpha1.AppDependencies{}
	ctx, cancel := connectionInfoContext(context.Background(), cr)
	defer cancel()
	base := timeoutOf(ctx, t)

	cr.Spec.RDS = &depsv1alpha1.RDSSpec{Resources: make([]depsv1alpha1.RDSInstanceSpec, 3)}
	ctx, cancel = connectionInfoContext(context.Background(), cr)
	defer cancel()
	if got := timeoutOf(ctx, t); got <= base {
		t.Errorf("timeout with 3 RDS instances = %v, want more than %v", got, base)
	}
}

func TestControllerOptions_BacksOffFromSecondsUpToTheDriftInterval(t *testing.T) {
	opts := controllerOptions(3)
	if opts.MaxConcurrentReconciles != 3 {
		t.Errorf("MaxConcurrentReconciles = %d, want 3", opts.MaxConcurrentReconciles)
	}
	if opts.RateLimiter == nil {
		t.Fatal("expected a RateLimiter: controller-runtime's default retries a failing CR after 5 ms")
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cr"}}
	if first := opts.RateLimiter.When(req); first < 2*time.Second {
		t.Errorf("first retry after %v, want at least 2s", first)
	}
	var last time.Duration
	for range 30 {
		last = opts.RateLimiter.When(req)
	}
	if last > DriftDetectionInterval {
		t.Errorf("retry delay grew to %v, want at most the drift interval %v", last, DriftDetectionInterval)
	}
}

// The reconcile timeout is a backstop for a pass stuck outside every
// section's own deadline, so it must not cut off a legitimate one: every
// section deleting the CRD's maximum of 20 resources.
func TestControllerOptions_ReconcileTimeoutSitsAboveEverySectionBudget(t *testing.T) {
	opts := controllerOptions(1)
	worst := time.Duration(len(sectionTypes)) * (baseSectionDeletionTimeout + 20*perResourceDeletionTimeout)
	if opts.ReconciliationTimeout <= worst {
		t.Errorf("ReconciliationTimeout = %v, want more than the worst-case section budgets %v", opts.ReconciliationTimeout, worst)
	}
}

func TestIsRetryable_FallsBackToTheAWSClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"throttling error without a ReconcileError", fmt.Errorf("getting queue URL: %w", &smithy.GenericAPIError{Code: "ThrottlingException"}), true},
		{"section deadline", fmt.Errorf("sqs: %w", context.DeadlineExceeded), true},
		{"a ReconcileError's own verdict wins", &cloudctlaws.ReconcileError{Err: context.DeadlineExceeded, Retryable: false}, false},
		{"ReconcileError marked retryable", &cloudctlaws.ReconcileError{Err: errors.New("busy"), Retryable: true}, true},
		{"plain error", errors.New("invalid name"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Errorf("isRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestStatusWriteContext_OutlivesACancelledPass(t *testing.T) {
	pass, cancel := context.WithCancel(context.Background())
	cancel()

	ctx, cancelWrite := statusWriteContext(pass)
	defer cancelWrite()
	if ctx.Err() != nil {
		t.Fatalf("status write context is already done: %v", ctx.Err())
	}
	if got := timeoutOf(ctx, t); got > statusWriteTimeout {
		t.Errorf("timeout = %v, want at most %v", got, statusWriteTimeout)
	}
}

// withinJitter reports whether got is want give or take the requeue jitter.
func withinJitter(got, want time.Duration) bool {
	return got >= time.Duration(float64(want)*(1-driftRequeueJitterFraction)) &&
		got <= time.Duration(float64(want)*(1+driftRequeueJitterFraction))
}

func throttledReconciler(t *testing.T, cr *depsv1alpha1.AppDependencies) (*AppDependenciesReconciler, *fakeSQSClient) {
	t.Helper()
	patches := 0
	r := newCountingReconciler(t, &patches, cr)
	sqsFake := r.AWSClients.SQS.(*fakeSQSClient)
	return r, sqsFake
}

func transientCR() *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout-service", UID: "uid-1", Finalizers: []string{finalizerName}},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
	}
}

// A transient failure keeps being retried, but further apart each time, up
// to a cap: during a long AWS problem a CR shouldn't re-run all its sections
// every 30 seconds, and the cap keeps a waiting state (an instance still
// creating) from being noticed more than a couple of minutes late.
func TestReconcile_TransientFailuresBackOffUpToACap(t *testing.T) {
	cr := transientCR()
	r, sqsFake := throttledReconciler(t, cr)
	sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	want := []time.Duration{transientRequeueInterval, 2 * transientRequeueInterval, 4 * transientRequeueInterval, transientMaxDelay, transientMaxDelay}
	for i, w := range want {
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("pass %d: Reconcile() error = %v, want a requeue instead of an error", i+1, err)
		}
		if !withinJitter(res.RequeueAfter, w) {
			t.Errorf("pass %d: RequeueAfter = %v, want about %v", i+1, res.RequeueAfter, w)
		}
	}
}

func TestReconcile_ASuccessfulPassStartsTheBackoffOver(t *testing.T) {
	cr := transientCR()
	r, sqsFake := throttledReconciler(t, cr)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}
	for range 3 {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
	}

	sqsFake.getQueueUrlErr = nil
	if res, err := r.Reconcile(context.Background(), req); err != nil || res.RequeueAfter < time.Minute {
		t.Fatalf("expected a healthy pass to requeue at the drift interval, got %v, %v", res.RequeueAfter, err)
	}

	sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}
	res, err := r.Reconcile(context.Background(), req)
	if err != nil || !withinJitter(res.RequeueAfter, transientRequeueInterval) {
		t.Errorf("after a success the first failure should wait about %v again, got %v, %v", transientRequeueInterval, res.RequeueAfter, err)
	}
}

func TestReconcile_EachCRBacksOffOnItsOwn(t *testing.T) {
	cr := transientCR()
	r, sqsFake := throttledReconciler(t, cr)
	sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}

	failing := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}
	for range 3 {
		if _, err := r.Reconcile(context.Background(), failing); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
	}
	other := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "billing-service", UID: "uid-2", Finalizers: []string{finalizerName}},
		Spec:       transientCR().Spec,
	}
	if err := r.Create(context.Background(), other); err != nil {
		t.Fatalf("creating the second CR: %v", err)
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: other.Namespace, Name: other.Name}})
	if err != nil || !withinJitter(res.RequeueAfter, transientRequeueInterval) {
		t.Errorf("a CR's first failure should wait about %v whatever another CR has been through, got %v, %v", transientRequeueInterval, res.RequeueAfter, err)
	}
}

// CRs that failed together shouldn't all come back at the same instant.
func TestTransientDelay_SpreadsCRsThatFailedTogether(t *testing.T) {
	r := &AppDependenciesReconciler{}
	seen := map[time.Duration]bool{}
	for i := range 20 {
		d := r.transientDelay(types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("cr-%d", i)})
		if !withinJitter(d, transientRequeueInterval) {
			t.Fatalf("first delay %v is not about %v", d, transientRequeueInterval)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Errorf("20 CRs got only %d distinct first delays, want them spread out", len(seen))
	}
}

// End to end through Reconcile: a CR restored with a new UID finds its
// queue tagged with the old one, and takes it over only when annotated.
func TestReconcile_ARestoredCRTakesOverItsResourcesWhenAnnotated(t *testing.T) {
	for name, tc := range map[string]struct {
		annotations map[string]string
		transferred bool
	}{
		"annotated with the previous UID": {map[string]string{cloudctlaws.AdoptFromUIDAnnotation: "old-uid"}, true},
		"not annotated":                   {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			cr := transientCR()
			cr.Annotations = tc.annotations
			r, sqsFake := throttledReconciler(t, cr)
			queueName := cloudctlaws.ResourceName(cr.Namespace, cr.Name, "sqs", "orders", 80)
			sqsFake.queues[queueName] = &fakeQueue{
				url: "https://sqs.us-east-1.amazonaws.com/123456789012/" + queueName, arn: "arn:aws:sqs:us-east-1:123456789012:" + queueName,
				tags: map[string]string{
					cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(cr.Namespace, cr.Name),
					cloudctlaws.OwnerUIDTagKey: "old-uid",
				},
			}

			_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}})

			got := sqsFake.queues[queueName].tags[cloudctlaws.OwnerUIDTagKey]
			if tc.transferred && got != string(cr.UID) {
				t.Errorf("owner-uid = %q, want the CR's UID %q", got, cr.UID)
			}
			if !tc.transferred && got != "old-uid" {
				t.Errorf("owner-uid = %q, want the old one left alone", got)
			}
		})
	}
}

// A CR waiting for a grant or a producer's topic is woken by the watch on it,
// so it needn't be polled at the fast transient pace.
func TestReconcile_AWaitAWatchEndsIsRetriedAtTheDriftPace(t *testing.T) {
	cr := transientCR()
	cr.Spec.Alarms = &depsv1alpha1.AlarmsSpec{
		Enabled:     true,
		SnsTopicRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform", ResourceName: "events"},
	}
	r, _ := throttledReconciler(t, cr)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want a requeue", err)
	}
	if !withinJitter(res.RequeueAfter, DriftDetectionInterval) {
		t.Errorf("RequeueAfter = %v, want about %v", res.RequeueAfter, DriftDetectionInterval)
	}

	t.Run("another section failing transiently is not held back by it", func(t *testing.T) {
		cr := transientCR()
		cr.Spec.Alarms = &depsv1alpha1.AlarmsSpec{
			Enabled:     true,
			SnsTopicRef: &depsv1alpha1.ConsumeRef{Namespace: "team-b", Name: "platform", ResourceName: "events"},
		}
		r, sqsFake := throttledReconciler(t, cr)
		sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("Reconcile() error = %v, want a requeue", err)
		}
		if !withinJitter(res.RequeueAfter, transientRequeueInterval) {
			t.Errorf("RequeueAfter = %v, want about %v", res.RequeueAfter, transientRequeueInterval)
		}
	})
}

// A wait that outlasts any blip (an index building for hours, an instance
// that never comes up) is polled at the drift pace after a while, not every
// couple of minutes forever; the first stretch stays quick.
func TestReconcile_ALongWaitIsRetriedAtTheDriftPace(t *testing.T) {
	cr := transientCR()
	r, sqsFake := throttledReconciler(t, cr)
	sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	var last ctrl.Result
	for pass := 1; pass <= transientLongAfter; pass++ {
		var err error
		if last, err = r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("pass %d: Reconcile() error = %v", pass, err)
		}
	}
	if !withinJitter(last.RequeueAfter, transientMaxDelay) {
		t.Fatalf("pass %d: RequeueAfter = %v, want still about %v", transientLongAfter, last.RequeueAfter, transientMaxDelay)
	}

	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !withinJitter(res.RequeueAfter, DriftDetectionInterval) {
		t.Errorf("RequeueAfter = %v after %d failures, want about %v", res.RequeueAfter, transientLongAfter, DriftDetectionInterval)
	}

	sqsFake.getQueueUrlErr = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	sqsFake.getQueueUrlErr = &smithy.GenericAPIError{Code: "ThrottlingException"}
	res, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !withinJitter(res.RequeueAfter, transientRequeueInterval) {
		t.Errorf("RequeueAfter = %v after a success, want the quick pace again (about %v)", res.RequeueAfter, transientRequeueInterval)
	}
}

// A queue encrypted with a key nobody declared waits for the user to edit
// the spec, which wakes the CR at once, so polling it is pointless.
func TestReconcile_AnUndeclaredKMSKeyRefIsRetriedAtTheDriftPace(t *testing.T) {
	cr := transientCR()
	cr.Spec.SQS.Resources[0].Encryption = &depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}
	r, _ := throttledReconciler(t, cr)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}

	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want a requeue", err)
	}
	if !withinJitter(res.RequeueAfter, DriftDetectionInterval) {
		t.Errorf("RequeueAfter = %v, want about %v", res.RequeueAfter, DriftDetectionInterval)
	}
}
