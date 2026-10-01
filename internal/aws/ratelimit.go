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
	"fmt"
	"os"
	"strconv"
	"time"

	smithymiddleware "github.com/aws/smithy-go/middleware"
	"golang.org/x/time/rate"
)

// This file paces this operator's own outgoing AWS calls, independent of
// controller-runtime's reconcile concurrency - that bounds how many
// reconciles run in parallel, not how many AWS calls they collectively
// make per second. At small scale the difference is invisible; at fleet
// scale (a mass resync after a restart, a cluster-wide spec change
// touching every CR at once) a handful of concurrent reconciles can
// genuinely burst past a service's real rate limits, especially IAM's,
// which are far tighter than SQS/S3/SNS's.
//
// Each AWS service gets its own independent limiter rather than one
// shared budget - sharing one budget across services this different in
// capacity means the tightest one (IAM) throttles the others down to its
// own ceiling for no reason tied to their actual capacity.

// NewLimiter builds a token-bucket limiter from a QPS/burst pair. Values
// <= 0 fall back to 1 QPS / burst of 1 - a last-resort floor, not a
// recommended value, so a misconfigured input never produces a limiter
// that blocks forever (rate.Limit(0)) or rejects every request outright
// (burst 0).
func NewLimiter(qps float64, burst int) *rate.Limiter {
	if qps <= 0 {
		qps = 1
	}
	if burst <= 0 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(qps), burst)
}

// RateLimitMiddleware returns an aws-sdk-go-v2 middleware that blocks on
// limiter.Wait before letting a request proceed, until a token is
// available or the call's own context ends. A nil limiter is a no-op
// rather than a panic, the same defensive floor NewLimiter applies to a
// misconfigured QPS/burst.
func RateLimitMiddleware(limiter *rate.Limiter) func(*smithymiddleware.Stack) error {
	return func(stack *smithymiddleware.Stack) error {
		return stack.Finalize.Add(
			smithymiddleware.FinalizeMiddlewareFunc("RateLimit", func(
				ctx context.Context,
				in smithymiddleware.FinalizeInput,
				next smithymiddleware.FinalizeHandler,
			) (smithymiddleware.FinalizeOutput, smithymiddleware.Metadata, error) {
				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						return smithymiddleware.FinalizeOutput{}, smithymiddleware.Metadata{}, fmt.Errorf("rate limit wait: %w", err)
					}
				}
				return next.HandleFinalize(ctx, in)
			}),
			smithymiddleware.Before,
		)
	}
}

// rampSteps and rampStartFraction: a freshly-elected leader can have a
// large backlog of CRs to reconcile all at once (informer cache sync,
// then the workqueue populating) - starting at full burst lets that
// backlog fire as fast as the steady-state ceiling allows, right when a
// real AWS rate limit is most likely to actually be hit. RampUp starts a
// limiter at rampStartFraction of its already-configured target and
// raises it in rampSteps increments back to that same target over
// rampDuration.
const (
	rampSteps         = 20
	rampStartFraction = 0.1
)

// RampUp reads limiter's current Limit/Burst as the target (whatever
// NewLimiter already resolved them to), temporarily lowers it to
// rampStartFraction of that, then raises it back to the original target
// in rampSteps increments over rampDuration. Meant to be started once per
// limiter, right after a manager becomes leader - call it before anything
// else touches the limiter. Returns once fully ramped, or immediately if
// ctx is cancelled first.
func RampUp(ctx context.Context, limiter *rate.Limiter, rampDuration time.Duration) {
	targetLimit := limiter.Limit()
	targetBurst := limiter.Burst()
	stepDuration := rampDuration / rampSteps

	limiter.SetLimit(targetLimit * rampStartFraction)
	limiter.SetBurst(max(1, int(float64(targetBurst)*rampStartFraction)))

	ticker := time.NewTicker(stepDuration)
	defer ticker.Stop()

	for i := 1; i <= rampSteps; i++ {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			fraction := rampStartFraction + (1-rampStartFraction)*float64(i)/float64(rampSteps)
			limiter.SetLimitAt(now, targetLimit*rate.Limit(fraction))
			limiter.SetBurstAt(now, max(1, int(float64(targetBurst)*fraction)))
		}
	}

	now := time.Now()
	limiter.SetLimitAt(now, targetLimit)
	limiter.SetBurstAt(now, targetBurst)
}

// envFloat reads name as a float64, returning fallback if unset or
// unparseable.
func envFloat(name string, fallback float64) float64 {
	v, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

// envInt reads name as an int, returning fallback if unset or
// unparseable - same reasoning as envFloat.
func envInt(name string, fallback int) int {
	v, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}
