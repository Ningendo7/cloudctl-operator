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

package kube_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
)

// writeKubeconfig writes a kubeconfig with two contexts: "prod" (namespace
// checkout) and "staging" (no namespace), current context "prod". The server
// is never contacted by these tests.
func writeKubeconfig(t *testing.T) string {
	t.Helper()
	return writeKubeconfigFor(t, "https://127.0.0.1:1")
}

// writeKubeconfigFor is writeKubeconfig with the API server at server.
func writeKubeconfigFor(t *testing.T, server string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{Server: server}
	cfg.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	cfg.Contexts["prod"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u", Namespace: "checkout"}
	cfg.Contexts["staging"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	cfg.CurrentContext = "prod"
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNew_ResolvesTheContextAndNamespace(t *testing.T) {
	path := writeKubeconfig(t)

	tests := map[string]struct {
		opts kube.Options
		want kube.Target
	}{
		"the current context and its namespace": {kube.Options{Kubeconfig: path}, kube.Target{Context: "prod", Namespace: "checkout"}},
		"another context without a namespace":   {kube.Options{Kubeconfig: path, Context: "staging"}, kube.Target{Context: "staging", Namespace: "default"}},
		"the flag beats the context":            {kube.Options{Kubeconfig: path, Namespace: "payments"}, kube.Target{Context: "prod", Namespace: "payments"}},
		"all namespaces leaves it empty":        {kube.Options{Kubeconfig: path, AllNamespaces: true}, kube.Target{Context: "prod", Namespace: ""}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reader, target, err := kube.New(tc.opts)

			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if reader == nil {
				t.Error("New() returned no reader")
			}
			if target != tc.want {
				t.Errorf("target = %+v, want %+v", target, tc.want)
			}
		})
	}
}

func TestNew_FailsClearlyOnABadKubeconfigOrContext(t *testing.T) {
	path := writeKubeconfig(t)

	if _, _, err := kube.New(kube.Options{Kubeconfig: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Error("a kubeconfig that does not exist should be an error")
	}
	if _, _, err := kube.New(kube.Options{Kubeconfig: path, Context: "nope"}); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Errorf("an unknown context should be an error naming it, got %v", err)
	}
	bad := filepath.Join(t.TempDir(), "bad")
	if err := os.WriteFile(bad, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := kube.New(kube.Options{Kubeconfig: bad}); err == nil {
		t.Error("an unparseable kubeconfig should be an error")
	}
}

func TestExplainForbidden(t *testing.T) {
	forbid := func(group, resource string) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: group, Resource: resource}, "x", errors.New("denied"))
	}

	t.Run("appdependencies names the role that grants it", func(t *testing.T) {
		got := kube.ExplainForbidden(forbid("deps.cloudctl.io", "appdependencies"))

		for _, want := range []string{"appdependencies", "get", "list", "appdependencies-viewer-role", "bind"} {
			if !strings.Contains(got, want) {
				t.Errorf("hint %q is missing %q", got, want)
			}
		}
	})
	t.Run("events says they are optional", func(t *testing.T) {
		got := kube.ExplainForbidden(forbid("events.k8s.io", "events"))

		if !strings.Contains(got, "events") || !strings.Contains(got, "optional") {
			t.Errorf("hint %q should name events and say they are optional", got)
		}
	})
	t.Run("a joined error is unwrapped", func(t *testing.T) {
		got := kube.ExplainForbidden(errors.Join(kube.ErrForbidden, forbid("deps.cloudctl.io", "appdependencies")))

		if !strings.Contains(got, "appdependencies-viewer-role") {
			t.Errorf("hint = %q", got)
		}
	})
	// The hint is printed under the error itself, so it names what is needed
	// instead of repeating the error.
	t.Run("anything else names the resource without repeating the error", func(t *testing.T) {
		got := kube.ExplainForbidden(forbid("apps", "deployments"))

		if !strings.Contains(got, "deployments.apps") || strings.Contains(got, "denied") || strings.Contains(got, "forbidden:") {
			t.Errorf("hint = %q, want it to name deployments.apps and not repeat the error", got)
		}
	})
	t.Run("a core resource is named without a group", func(t *testing.T) {
		got := kube.ExplainForbidden(forbid("", "namespaces"))

		if !strings.Contains(got, "namespaces") || strings.Contains(got, "namespaces.") {
			t.Errorf("hint = %q, want it to name namespaces alone", got)
		}
	})
	t.Run("a forbidden error with no details gives an empty hint", func(t *testing.T) {
		bare := &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden, Message: "no"}}

		if got := kube.ExplainForbidden(bare); got != "" {
			t.Errorf("hint = %q, want empty: there is nothing to add to the error", got)
		}
	})
	t.Run("a non-forbidden error gives an empty hint", func(t *testing.T) {
		if got := kube.ExplainForbidden(errors.New("boom")); got != "" {
			t.Errorf("hint = %q, want empty", got)
		}
	})
}

// A server that accepts the connection and never answers must not hang the CLI.
// Discovery takes no context, so only the client's own timeout bounds it.
func TestNew_TheTimeoutBoundsEveryRequest(t *testing.T) {
	stop := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(stop) })

	reader, _, err := kube.New(kube.Options{Kubeconfig: writeKubeconfigFor(t, server.URL), Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for name, call := range map[string]func() error{
		"Get": func() error {
			_, err := reader.Get(context.Background(), "checkout", "x")
			return err
		},
		"ServerVersions": func() error {
			_, err := reader.ServerVersions(context.Background())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- call() }()

			select {
			case err := <-done:
				if err == nil {
					t.Error("a server that never answers should be an error")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("still waiting after 10s; the timeout is not applied to the client")
			}
		})
	}
}
