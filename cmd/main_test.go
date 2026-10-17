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
	"os"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/observability"
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

func TestAWSBootstrap_BecomesReadyOnlyAfterSetupSucceeds(t *testing.T) {
	want := &cloudctlaws.Clients{Region: "us-east-1"}
	var got *cloudctlaws.Clients
	var attempts atomic.Int32
	b := &awsBootstrap{
		newClients: func(context.Context) (*cloudctlaws.Clients, error) {
			if attempts.Add(1) < 2 {
				return nil, errors.New("SignatureDoesNotMatch")
			}
			return want, nil
		},
		interval: time.Millisecond,
		onReady: func(_ context.Context, c *cloudctlaws.Clients) error {
			got = c
			return nil
		},
	}
	if b.readyCheck(nil) == nil {
		t.Fatal("expected readyz to fail before AWS clients are initialized")
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got != want {
		t.Errorf("onReady got %v, want the initialized clients", got)
	}
	if err := b.readyCheck(nil); err != nil {
		t.Errorf("expected readyz to pass after setup, got %v", err)
	}
}

func TestAWSBootstrap_ShutdownWhileRetrying_ReturnsNilAndStaysNotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &awsBootstrap{
		newClients: func(context.Context) (*cloudctlaws.Clients, error) {
			cancel()
			return nil, errors.New("SignatureDoesNotMatch")
		},
		interval: time.Millisecond,
		onReady: func(context.Context, *cloudctlaws.Clients) error {
			t.Error("onReady must not run when clients never initialized")
			return nil
		},
	}

	if err := b.Start(ctx); err != nil {
		t.Errorf("expected a clean nil return on shutdown, got %v", err)
	}
	if b.readyCheck(nil) == nil {
		t.Error("expected readyz to keep failing")
	}
}

func TestAWSBootstrap_SetupFailurePropagatesAndStaysNotReady(t *testing.T) {
	b := &awsBootstrap{
		newClients: func(context.Context) (*cloudctlaws.Clients, error) { return &cloudctlaws.Clients{}, nil },
		interval:   time.Millisecond,
		onReady:    func(context.Context, *cloudctlaws.Clients) error { return errors.New("controller setup failed") },
	}

	if err := b.Start(context.Background()); err == nil {
		t.Error("expected the setup error to propagate so the manager exits")
	}
	if b.readyCheck(nil) == nil {
		t.Error("expected readyz to keep failing")
	}
}

func TestAWSBootstrap_RunsOnStandbyReplicasToo(t *testing.T) {
	if (&awsBootstrap{}).NeedLeaderElection() {
		t.Error("expected NeedLeaderElection() = false so standby replicas become ready")
	}
}

func TestAWSBootstrap_ReportsClientsReadyMetric(t *testing.T) {
	observability.AWSClientsReady.Set(0)
	b := &awsBootstrap{
		newClients: func(context.Context) (*cloudctlaws.Clients, error) { return &cloudctlaws.Clients{}, nil },
		interval:   time.Millisecond,
		onReady:    func(context.Context, *cloudctlaws.Clients) error { return nil },
	}

	if got := clientsReadyGauge(t); got != 0 {
		t.Fatalf("expected the gauge to read 0 before the clients are up, got %v", got)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := clientsReadyGauge(t); got != 1 {
		t.Errorf("expected the gauge to read 1 once the clients are up, got %v", got)
	}
}

// Setup that fails after the clients came up is not ready: the gauge has to
// agree with the readiness probe.
func TestAWSBootstrap_SetupFailureLeavesClientsReadyMetricAtZero(t *testing.T) {
	observability.AWSClientsReady.Set(0)
	b := &awsBootstrap{
		newClients: func(context.Context) (*cloudctlaws.Clients, error) { return &cloudctlaws.Clients{}, nil },
		interval:   time.Millisecond,
		onReady:    func(context.Context, *cloudctlaws.Clients) error { return errors.New("setup failed") },
	}

	if err := b.Start(context.Background()); err == nil {
		t.Fatal("expected Start to return the setup error")
	}
	if got := clientsReadyGauge(t); got != 0 {
		t.Errorf("expected the gauge to stay 0 when setup failed, got %v", got)
	}
}

func clientsReadyGauge(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := observability.AWSClientsReady.Write(&m); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	return m.GetGauge().GetValue()
}

func TestParseSecurityGroupIDs(t *testing.T) {
	got, err := parseSecurityGroupIDs(" sg-0a1b2c3d4e5f60718 ,sg-12345678,, ")
	if err != nil || len(got) != 2 || got[0] != "sg-0a1b2c3d4e5f60718" || got[1] != "sg-12345678" {
		t.Errorf("got %v, %v; want the two IDs, trimmed, with blanks skipped", got, err)
	}
	if got, err := parseSecurityGroupIDs(""); err != nil || len(got) != 0 {
		t.Errorf("empty value: got %v, %v; want none", got, err)
	}
	for _, bad := range []string{"sg-xyz", "vpc-12345678", "sg-1234", "12345678"} {
		if _, err := parseSecurityGroupIDs(bad); err == nil {
			t.Errorf("parseSecurityGroupIDs(%q) accepted a value that is not a security group ID", bad)
		}
	}
}

func writeCgroupFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCgroupMemoryLimit(t *testing.T) {
	const mib = 1 << 20
	tests := []struct {
		name  string
		files map[string]string
		want  int64
	}{
		{"cgroup v2 with a limit", map[string]string{cgroupV2MemoryFile: "134217728\n"}, 128 * mib},
		{"cgroup v2 without a limit", map[string]string{cgroupV2MemoryFile: "max\n"}, 0},
		{"cgroup v1 with a limit", map[string]string{cgroupV1MemoryFile: "268435456\n"}, 256 * mib},
		{"cgroup v1 without a limit", map[string]string{cgroupV1MemoryFile: "9223372036854771712\n"}, 0},
		{"no cgroup files", nil, 0},
		{"unreadable value", map[string]string{cgroupV2MemoryFile: "lots"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for name, content := range tc.files {
				writeCgroupFile(t, root, name, content)
			}
			if got := cgroupMemoryLimit(root); got != tc.want {
				t.Errorf("cgroupMemoryLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLimitMemoryFromCgroup(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	root := t.TempDir()
	writeCgroupFile(t, root, cgroupV2MemoryFile, "134217728")

	t.Run("sets 90% of the container limit", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "")
		const want = 120795955 // 90% of 128 MiB, rounded down
		if got := limitMemoryFromCgroup(root); got != want {
			t.Fatalf("limitMemoryFromCgroup() = %d, want %d", got, want)
		}
		if got := debug.SetMemoryLimit(-1); got != want {
			t.Errorf("runtime memory limit = %d, want %d", got, want)
		}
	})

	t.Run("leaves an explicit GOMEMLIMIT alone", func(t *testing.T) {
		debug.SetMemoryLimit(1 << 40)
		t.Setenv("GOMEMLIMIT", "64MiB")
		if got := limitMemoryFromCgroup(root); got != 0 {
			t.Errorf("limitMemoryFromCgroup() = %d, want 0 when GOMEMLIMIT is set", got)
		}
		if got := debug.SetMemoryLimit(-1); got != 1<<40 {
			t.Errorf("runtime memory limit changed to %d", got)
		}
	})

	t.Run("sets nothing without a container limit", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "")
		if got := limitMemoryFromCgroup(t.TempDir()); got != 0 {
			t.Errorf("limitMemoryFromCgroup() = %d, want 0", got)
		}
	})
}
