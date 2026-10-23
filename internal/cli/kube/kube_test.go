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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:1"}
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

		for _, want := range []string{"appdependencies", "get", "list", "appdependencies-viewer-role"} {
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
	t.Run("anything else keeps the original message", func(t *testing.T) {
		got := kube.ExplainForbidden(forbid("apps", "deployments"))

		if !strings.Contains(got, "deployments") {
			t.Errorf("hint = %q, want the original error's text", got)
		}
	})
	t.Run("a non-forbidden error gives an empty hint", func(t *testing.T) {
		if got := kube.ExplainForbidden(errors.New("boom")); got != "" {
			t.Errorf("hint = %q, want empty", got)
		}
	})
}
