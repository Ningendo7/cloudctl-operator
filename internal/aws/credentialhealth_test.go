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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// fakeSTSClient lets tests script GetCallerIdentity's success/failure
// sequence without any real AWS calls.
type fakeSTSClient struct {
	// callerIdentityErrs is consumed one error per call, in order; once
	// exhausted, every further call succeeds. A nil entry means that call
	// succeeds.
	callerIdentityErrs []error
	calls              atomic.Int32
}

func (f *fakeSTSClient) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	i := f.calls.Add(1) - 1
	if int(i) < len(f.callerIdentityErrs) {
		if err := f.callerIdentityErrs[i]; err != nil {
			return nil, err
		}
	}
	return &sts.GetCallerIdentityOutput{}, nil
}

func TestNewCredentialHealth_StartsHealthy(t *testing.T) {
	h := NewCredentialHealth()
	if err := h.Check(nil); err != nil {
		t.Errorf("expected a freshly constructed CredentialHealth to start healthy, got error: %v", err)
	}
}

// The periodic-check tests run in a synctest bubble: time there is fake and
// only moves when every goroutine in the bubble is blocked, so a tick or a
// timeout lands exactly when the test says, however loaded the machine is.

// startCheck runs RunPeriodicCheck in the bubble until the test returns.
func startCheck(t *testing.T, stsClient STSClient, interval time.Duration) *CredentialHealth {
	t.Helper()
	h := NewCredentialHealth()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go RunPeriodicCheck(ctx, stsClient, interval, h)
	return h
}

// advance moves the bubble's clock by d and lets everything due run.
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

func TestRunPeriodicCheck_FlipsUnhealthyOnFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startCheck(t, &fakeSTSClient{callerIdentityErrs: []error{errors.New("AccessDenied")}}, time.Minute)

		advance(time.Minute)
		if h.Check(nil) == nil {
			t.Error("expected unhealthy after the first re-check failed")
		}
	})
}

func TestRunPeriodicCheck_RecoversOnLaterSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startCheck(t, &fakeSTSClient{callerIdentityErrs: []error{errors.New("AccessDenied")}}, time.Minute) // only the first call fails

		advance(time.Minute)
		if h.Check(nil) == nil {
			t.Fatal("expected unhealthy after the first re-check failed")
		}
		advance(time.Minute)
		if err := h.Check(nil); err != nil {
			t.Errorf("expected healthy again once the next re-check succeeded, got %v", err)
		}
	})
}

// hangingSTSClient never answers, like a call on a half-open connection.
type hangingSTSClient struct{}

func (hangingSTSClient) GetCallerIdentity(ctx context.Context, _ *sts.GetCallerIdentityInput, _ ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A hung call is cut off when the next check is due, so the result is never
// older than one interval.
func TestRunPeriodicCheck_AHungCallTimesOutAtTheNextInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startCheck(t, hangingSTSClient{}, 10*time.Second)

		advance(10*time.Second + 10*time.Second - time.Nanosecond)
		if err := h.Check(nil); err != nil {
			t.Fatalf("unhealthy before the hung call's deadline: %v", err)
		}
		advance(time.Nanosecond)
		if h.Check(nil) == nil {
			t.Error("expected unhealthy once the hung call hit its deadline")
		}
	})
}

// With a long interval the call is still cut off after credentialCheckTimeout,
// not left hanging until the next check.
func TestRunPeriodicCheck_AHungCallNeverOutlivesTheCheckTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startCheck(t, hangingSTSClient{}, time.Hour)

		advance(time.Hour + credentialCheckTimeout - time.Nanosecond)
		if err := h.Check(nil); err != nil {
			t.Fatalf("unhealthy before the check timeout: %v", err)
		}
		advance(time.Nanosecond)
		if h.Check(nil) == nil {
			t.Errorf("expected unhealthy %s after the call started", credentialCheckTimeout)
		}
	})
}

func TestRunPeriodicCheck_StopsOnContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			RunPeriodicCheck(ctx, &fakeSTSClient{}, time.Minute, NewCredentialHealth())
			close(done)
		}()

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("expected RunPeriodicCheck to return once its context is cancelled")
		}
	})
}

func TestRunPeriodicCheck_CallsOncePerInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := &fakeSTSClient{}
		startCheck(t, fake, time.Hour)

		advance(time.Hour - time.Nanosecond)
		if calls := fake.calls.Load(); calls != 0 {
			t.Fatalf("calls before the first interval = %d, want 0", calls)
		}
		advance(time.Nanosecond)
		if calls := fake.calls.Load(); calls != 1 {
			t.Fatalf("calls after one interval = %d, want 1", calls)
		}
		advance(2 * time.Hour)
		if calls := fake.calls.Load(); calls != 3 {
			t.Errorf("calls after three intervals = %d, want 3", calls)
		}
	})
}
