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

package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func TestWaitForAWSClients_RetriesUntilSuccess(t *testing.T) {
	var attempts int32
	want := &cloudctlaws.Clients{Region: "us-east-1"}
	newClients := func(context.Context) (*cloudctlaws.Clients, error) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return nil, errors.New("SignatureDoesNotMatch")
		}
		return want, nil
	}

	got, err := waitForAWSClients(context.Background(), time.Millisecond, newClients)
	if err != nil {
		t.Fatalf("waitForAWSClients() error = %v", err)
	}
	if got != want {
		t.Errorf("expected the eventually-successful clients to be returned, got %v", got)
	}
	if attempts != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", attempts)
	}
}

func TestWaitForAWSClients_StopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	newClients := func(context.Context) (*cloudctlaws.Clients, error) {
		cancel() // simulate a shutdown signal arriving while still failing
		return nil, errors.New("SignatureDoesNotMatch")
	}

	_, err := waitForAWSClients(ctx, time.Millisecond, newClients)
	if err == nil {
		t.Fatal("expected waitForAWSClients() to return an error once its context is cancelled")
	}
}

func TestWaitForAWSClients_SucceedsOnFirstAttemptWithoutWaitingAFullInterval(t *testing.T) {
	newClients := func(context.Context) (*cloudctlaws.Clients, error) {
		return &cloudctlaws.Clients{}, nil
	}

	start := time.Now()
	if _, err := waitForAWSClients(context.Background(), time.Hour, newClients); err != nil {
		t.Fatalf("waitForAWSClients() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("expected an immediate first attempt, took %s against a 1h interval", elapsed)
	}
}
