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
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func TestCheckIAMRoleFlags(t *testing.T) {
	for name, tc := range map[string]struct {
		boundary, path string
		wantErr        bool
	}{
		"both unset":                 {},
		"a customer managed policy":  {boundary: "arn:aws:iam::123456789012:policy/cloudctl-boundary"},
		"a policy under a path":      {boundary: "arn:aws:iam::123456789012:policy/team/cloudctl-boundary"},
		"an AWS managed policy":      {boundary: "arn:aws:iam::aws:policy/PowerUserAccess"},
		"a GovCloud policy":          {boundary: "arn:aws-us-gov:iam::123456789012:policy/b"},
		"a role ARN, not a policy":   {boundary: "arn:aws:iam::123456789012:role/b", wantErr: true},
		"a policy name, not an ARN":  {boundary: "cloudctl-boundary", wantErr: true},
		"a path":                     {path: "/cloudctl/"},
		"a nested path":              {path: "/platform/cloudctl/"},
		"the root path":              {path: "/"},
		"a path without the slashes": {path: "cloudctl", wantErr: true},
		"a path missing the end":     {path: "/cloudctl", wantErr: true},
		"a path with a space":        {path: "/cloud ctl/", wantErr: true},
		"a path over 512 characters": {path: "/" + strings.Repeat("a", 511) + "/", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkIAMRoleFlags(tc.boundary, tc.path); (err != nil) != tc.wantErr {
				t.Errorf("checkIAMRoleFlags(%q, %q) = %v, want error %v", tc.boundary, tc.path, err, tc.wantErr)
			}
		})
	}
}

func TestCheckDeploymentFlags(t *testing.T) {
	for name, tc := range map[string]struct {
		deployment string
		unlabelled bool
		wantErr    bool
	}{
		"no name":                           {"", true, false},
		"a name":                            {"prod-use1", true, false},
		"a name, unlabelled left to others": {"prod-euw1", false, false},
		"no name and no unlabelled CRs":     {"", false, true},
		"not a label value":                 {"prod use1", true, true},
		"too long for a label":              {strings.Repeat("a", 64), true, true},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkDeploymentFlags(tc.deployment, tc.unlabelled); (err != nil) != tc.wantErr {
				t.Errorf("checkDeploymentFlags(%q, %v) = %v, want error %v", tc.deployment, tc.unlabelled, err, tc.wantErr)
			}
		})
	}
}

type fakeEKS struct {
	group string
	err   error
	calls int
}

func (f *fakeEKS) DescribeCluster(
	_ context.Context, in *eks.DescribeClusterInput, _ ...func(*eks.Options),
) (*eks.DescribeClusterOutput, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &eks.DescribeClusterOutput{Cluster: &ekstypes.Cluster{
		Name:               in.Name,
		ResourcesVpcConfig: &ekstypes.VpcConfigResponse{ClusterSecurityGroupId: &f.group},
	}}, nil
}

func TestPodBaseSecurityGroups(t *testing.T) {
	const configured, clusterGroup = "sg-0a1b2c3d4e5f60718", "sg-0aaaaaaaaaaaaaaaa"
	for name, tc := range map[string]struct {
		cluster    string
		configured []string
		eks        *fakeEKS
		want       []string
		wantErr    bool
		wantCalls  int
	}{
		"configured groups win": {
			"prod", []string{configured}, &fakeEKS{group: clusterGroup}, []string{configured}, false, 0,
		},
		"no cluster name, no lookup":  {"", nil, &fakeEKS{group: clusterGroup}, nil, false, 0},
		"the cluster's group":         {"prod", nil, &fakeEKS{group: clusterGroup}, []string{clusterGroup}, false, 1},
		"a failed lookup is an error": {"prod", nil, &fakeEKS{err: errors.New("AccessDenied")}, nil, true, 1},
		"a cluster without a group":   {"prod", nil, &fakeEKS{}, nil, true, 1},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := podBaseSecurityGroups(context.Background(), tc.eks, tc.cluster, tc.configured)
			if (err != nil) != tc.wantErr || !slices.Equal(got, tc.want) || tc.eks.calls != tc.wantCalls {
				t.Errorf("podBaseSecurityGroups() = %v, %v after %d calls; want %v, error %v, %d calls",
					got, err, tc.eks.calls, tc.want, tc.wantErr, tc.wantCalls)
			}
		})
	}
}

func TestEKSClusterNamePattern(t *testing.T) {
	for name, ok := range map[string]bool{
		"prod": true, "prod-v2_east": true, "-prod": false, "prod.v2": false, strings.Repeat("a", 101): false,
	} {
		if eksClusterNamePattern.MatchString(name) != ok {
			t.Errorf("eksClusterNamePattern.MatchString(%q) = %v, want %v", name, !ok, ok)
		}
	}
}

func TestKubeSystemUID(t *testing.T) {
	reader := fake.NewClientBuilder().WithObjects(&corev1.Namespace{Name: "kube-system", UID: "3b0f6c1e-cluster"}).Build()
	if got, err := kubeSystemUID(context.Background(), reader); err != nil || got != "3b0f6c1e-cluster" {
		t.Errorf("kubeSystemUID() = %q, %v; want the namespace's UID", got, err)
	}
	// Without it the cluster has no name, so startup waits rather than tag
	// resources with nothing.
	if _, err := kubeSystemUID(context.Background(), fake.NewClientBuilder().Build()); err == nil {
		t.Error("expected an error when kube-system can't be read")
	}
}

func TestParseDefaultTags(t *testing.T) {
	got, err := parseDefaultTags("env=prod, cost-center=12=34")
	if err != nil || len(got) != 2 || got["env"] != "prod" || got["cost-center"] != "12=34" {
		t.Errorf("parseDefaultTags() = %v, %v", got, err)
	}
	if got, err := parseDefaultTags(""); err != nil || got != nil {
		t.Errorf("parseDefaultTags(\"\") = %v, %v; want none", got, err)
	}
	many := make([]string, 0, maxDefaultTags+1)
	for i := range maxDefaultTags + 1 {
		many = append(many, fmt.Sprintf("k%d=v", i))
	}
	for _, bad := range []string{
		"env",                         // no value separator
		"=prod",                       // no key
		"cloudctl.io/owner=other/app", // would fake ownership
		"AWS:createdBy=me",            // reserved by AWS
		"env=prod,env=dev",            // twice
		"env=a;b",                     // a character AWS refuses
		strings.Join(many, ","),       // too many
	} {
		if _, err := parseDefaultTags(bad); err == nil {
			t.Errorf("parseDefaultTags(%q) accepted it", bad)
		}
	}
}
