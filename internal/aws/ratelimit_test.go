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

package aws

import (
	"context"
	"errors"
	"testing"
	"time"

	smithymiddleware "github.com/aws/smithy-go/middleware"
	"golang.org/x/time/rate"
)

// --- NewLimiter ---

func TestNewLimiter_FloorsNonPositiveQPS(t *testing.T) {
	if got := NewLimiter(0, 10).Limit(); got != rate.Limit(1) {
		t.Errorf("expected zero QPS to floor to 1, got %v", got)
	}
	if got := NewLimiter(-5, 10).Limit(); got != rate.Limit(1) {
		t.Errorf("expected negative QPS to floor to 1, got %v", got)
	}
}

func TestNewLimiter_FloorsNonPositiveBurst(t *testing.T) {
	if got := NewLimiter(10, 0).Burst(); got != 1 {
		t.Errorf("expected zero burst to floor to 1, got %d", got)
	}
	if got := NewLimiter(10, -3).Burst(); got != 1 {
		t.Errorf("expected negative burst to floor to 1, got %d", got)
	}
}

func TestNewLimiter_UsesProvidedValues(t *testing.T) {
	lim := NewLimiter(20, 40)
	if lim.Limit() != rate.Limit(20) {
		t.Errorf("expected QPS 20, got %v", lim.Limit())
	}
	if lim.Burst() != 40 {
		t.Errorf("expected burst 40, got %d", lim.Burst())
	}
}

// --- RateLimitMiddleware ---

// fakeFinalizeHandler is the next handler RateLimitMiddleware delegates to;
// tests assert on whether it was reached to distinguish "waited then
// proceeded" from "failed before proceeding".
type fakeFinalizeHandler struct {
	called bool
}

func (f *fakeFinalizeHandler) HandleFinalize(_ context.Context, _ smithymiddleware.FinalizeInput) (
	smithymiddleware.FinalizeOutput, smithymiddleware.Metadata, error,
) {
	f.called = true
	return smithymiddleware.FinalizeOutput{}, smithymiddleware.Metadata{}, nil
}

// registerRateLimitMiddleware builds a Stack and applies
// RateLimitMiddleware(limiter) to it exactly as NewClients does via
// Options.APIOptions, returning the registered middleware so tests can
// invoke it directly without standing up a real AWS client.
func registerRateLimitMiddleware(t *testing.T, limiter *rate.Limiter) smithymiddleware.FinalizeMiddleware {
	t.Helper()
	stack := smithymiddleware.NewStack("test", func() any { return struct{}{} })
	if err := RateLimitMiddleware(limiter)(stack); err != nil {
		t.Fatalf("RateLimitMiddleware returned error: %v", err)
	}
	mw, ok := stack.Finalize.Get("RateLimit")
	if !ok {
		t.Fatal(`expected a "RateLimit" middleware to be registered in the Finalize step`)
	}
	return mw
}

func TestRateLimitMiddleware_WaitsThenProceeds(t *testing.T) {
	mw := registerRateLimitMiddleware(t, rate.NewLimiter(rate.Inf, 1))
	next := &fakeFinalizeHandler{}

	if _, _, err := mw.HandleFinalize(context.Background(), smithymiddleware.FinalizeInput{}, next); err != nil {
		t.Fatalf("HandleFinalize returned error: %v", err)
	}
	if !next.called {
		t.Error("expected the next handler to be reached once a token was available")
	}
}

func TestRateLimitMiddleware_ContextCancelledStopsBeforeNext(t *testing.T) {
	// Plenty of capacity: proves the failure comes from ctx cancellation,
	// not from the limiter having no tokens to give.
	mw := registerRateLimitMiddleware(t, rate.NewLimiter(rate.Inf, 1))
	next := &fakeFinalizeHandler{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := mw.HandleFinalize(ctx, smithymiddleware.FinalizeInput{}, next)
	if err == nil {
		t.Fatal("expected an error when ctx is already cancelled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got %v", err)
	}
	if next.called {
		t.Error("expected the next handler NOT to be reached when the wait fails")
	}
}

func TestRateLimitMiddleware_NilLimiterIsNoOp(t *testing.T) {
	mw := registerRateLimitMiddleware(t, nil)
	next := &fakeFinalizeHandler{}

	// An already-cancelled ctx would fail Wait on any real limiter (see
	// TestRateLimitMiddleware_ContextCancelledStopsBeforeNext) - proceeding
	// anyway here proves Wait was skipped entirely, not merely fast.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := mw.HandleFinalize(ctx, smithymiddleware.FinalizeInput{}, next); err != nil {
		t.Errorf("expected a nil limiter to be a no-op, got error: %v", err)
	}
	if !next.called {
		t.Error("expected the next handler to be reached when limiter is nil")
	}
}

// --- RampUp ---

func TestRampUp_EndsAtOriginalTargetLimitAndBurst(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(100), 50)

	RampUp(context.Background(), limiter, 20*time.Millisecond)

	if limiter.Limit() != rate.Limit(100) {
		t.Errorf("expected limit to end back at target 100, got %v", limiter.Limit())
	}
	if limiter.Burst() != 50 {
		t.Errorf("expected burst to end back at target 50, got %d", limiter.Burst())
	}
}

func TestRampUp_LowersImmediatelyAndReturnsEarlyOnCancelledContext(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(100), 50)

	// RampUp applies its initial lowered values synchronously, before its
	// first select on ctx.Done() vs the ramp ticker - an already-cancelled
	// context deterministically wins that first select, so this exercises
	// both the initial lowering and the early-return path without relying
	// on real-time sleeps.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	RampUp(ctx, limiter, time.Hour)

	if got, want := limiter.Limit(), rate.Limit(100)*rampStartFraction; got != want {
		t.Errorf("expected limit lowered to start fraction %v, got %v", want, got)
	}
	if got, want := limiter.Burst(), int(50*rampStartFraction); got != want {
		t.Errorf("expected burst lowered to start fraction %d, got %d", want, got)
	}
}

func TestRampUp_FloorsBurstAtOneForSmallTargets(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1), 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	RampUp(ctx, limiter, time.Hour)

	if limiter.Burst() < 1 {
		t.Errorf("expected burst to never floor below 1, got %d", limiter.Burst())
	}
}

// --- envFloat / envInt ---

func TestEnvFloat(t *testing.T) {
	t.Setenv("CLOUDCTL_TEST_FLOAT", "12.5")
	if got := envFloat("CLOUDCTL_TEST_FLOAT", 1); got != 12.5 {
		t.Errorf("expected the set value 12.5, got %v", got)
	}
	if got := envFloat("CLOUDCTL_TEST_FLOAT_UNSET", 7); got != 7 {
		t.Errorf("expected the fallback for an unset var, got %v", got)
	}

	t.Setenv("CLOUDCTL_TEST_FLOAT_BAD", "not-a-number")
	if got := envFloat("CLOUDCTL_TEST_FLOAT_BAD", 3); got != 3 {
		t.Errorf("expected the fallback for an unparseable value, got %v", got)
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("CLOUDCTL_TEST_INT", "42")
	if got := envInt("CLOUDCTL_TEST_INT", 1); got != 42 {
		t.Errorf("expected the set value 42, got %v", got)
	}
	if got := envInt("CLOUDCTL_TEST_INT_UNSET", 9); got != 9 {
		t.Errorf("expected the fallback for an unset var, got %v", got)
	}

	t.Setenv("CLOUDCTL_TEST_INT_BAD", "not-a-number")
	if got := envInt("CLOUDCTL_TEST_INT_BAD", 5); got != 5 {
		t.Errorf("expected the fallback for an unparseable value, got %v", got)
	}
}

// --- newRateLimiters ---

func TestNewRateLimiters_OneLimiterPerService(t *testing.T) {
	limiters := newRateLimiters()
	for _, name := range []string{"sqs", "sns", "s3", "dynamodb", "kms", "iam", "cloudwatch"} {
		if limiters[name] == nil {
			t.Errorf("expected a limiter for %q, got none", name)
		}
	}
	if len(limiters) != 7 {
		t.Errorf("expected exactly 7 limiters, got %d", len(limiters))
	}
}

func TestNewRateLimiters_IAMIsTighterThanSQSByDefault(t *testing.T) {
	limiters := newRateLimiters()
	if limiters["iam"].Limit() >= limiters["sqs"].Limit() {
		t.Errorf("expected IAM's default QPS (%v) to be tighter than SQS's (%v)", limiters["iam"].Limit(), limiters["sqs"].Limit())
	}
}

func TestNewRateLimiters_RespectsEnvOverride(t *testing.T) {
	t.Setenv("AWS_SQS_RATE_LIMIT_QPS", "99")
	t.Setenv("AWS_SQS_RATE_LIMIT_BURST", "199")

	limiters := newRateLimiters()
	if limiters["sqs"].Limit() != rate.Limit(99) {
		t.Errorf("expected the env override to set QPS to 99, got %v", limiters["sqs"].Limit())
	}
	if limiters["sqs"].Burst() != 199 {
		t.Errorf("expected the env override to set burst to 199, got %d", limiters["sqs"].Burst())
	}
}
