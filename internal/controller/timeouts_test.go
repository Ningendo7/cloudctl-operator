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
	"k8s.io/apimachinery/pkg/types"
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
