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
	calls              int32
}

func (f *fakeSTSClient) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	i := atomic.AddInt32(&f.calls, 1) - 1
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

func TestRunPeriodicCheck_FlipsUnhealthyOnFailure(t *testing.T) {
	fake := &fakeSTSClient{callerIdentityErrs: []error{errors.New("AccessDenied")}}
	h := NewCredentialHealth()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunPeriodicCheck(ctx, fake, 5*time.Millisecond, h)

	waitForCondition(t, func() bool { return h.Check(nil) != nil }, "CredentialHealth to flip unhealthy after a failed re-check")
}

func TestRunPeriodicCheck_RecoversOnLaterSuccess(t *testing.T) {
	fake := &fakeSTSClient{callerIdentityErrs: []error{errors.New("AccessDenied")}} // only the first call fails
	h := NewCredentialHealth()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunPeriodicCheck(ctx, fake, 5*time.Millisecond, h)

	waitForCondition(t, func() bool { return h.Check(nil) != nil }, "CredentialHealth to flip unhealthy after the first failed re-check")
	waitForCondition(t, func() bool { return h.Check(nil) == nil }, "CredentialHealth to recover once a later re-check succeeds")
}

func TestRunPeriodicCheck_StopsOnContextCancellation(t *testing.T) {
	fake := &fakeSTSClient{}
	h := NewCredentialHealth()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunPeriodicCheck(ctx, fake, 5*time.Millisecond, h)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected RunPeriodicCheck to return promptly once its context is cancelled")
	}
}

func TestRunPeriodicCheck_DoesNotCallBeforeFirstInterval(t *testing.T) {
	fake := &fakeSTSClient{}
	h := NewCredentialHealth()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunPeriodicCheck(ctx, fake, time.Hour, h)

	time.Sleep(50 * time.Millisecond)
	if calls := atomic.LoadInt32(&fake.calls); calls != 0 {
		t.Errorf("expected no calls before the first interval elapses (1h), got %d", calls)
	}
}

// waitForCondition polls cond until it's true or a short deadline passes,
// failing the test with msg otherwise - used instead of a fixed sleep so
// these tests run fast when healthy and still reliable under load.
func waitForCondition(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}
