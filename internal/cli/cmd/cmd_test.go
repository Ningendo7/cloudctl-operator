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

package cmd_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/clitest"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/cmd"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/version"
)

type run struct {
	code        int
	stdout      string
	stderr      string
	opts        kube.Options
	factoryUsed bool
}

// exec runs the CLI as `progName args...` against reader, with the clock fixed
// at clitest.Now.
func exec(t *testing.T, reader kube.Reader, progName string, args ...string) run {
	t.Helper()
	var out, errOut bytes.Buffer
	r := run{}
	factory := func(o kube.Options) (kube.Reader, kube.Target, error) {
		r.opts, r.factoryUsed = o, true
		ns := o.Namespace
		if ns == "" && !o.AllNamespaces {
			ns = "default"
		}
		return reader, kube.Target{Context: "prod-eks", Namespace: ns}, nil
	}
	r.code = cmd.Execute(args, progName, cmd.Streams{Out: &out, Err: &errOut}, factory, func() time.Time { return clitest.Now })
	r.stdout, r.stderr = out.String(), errOut.String()
	return r
}

func readyCR() *depsv1alpha1.AppDependencies {
	return clitest.CR("checkout", "checkout-service",
		clitest.Condition("Ready", metav1.ConditionTrue, "AllSectionsReady", "all declared sections are ready", 12, 4*24*time.Hour),
		clitest.Condition("SQSReady", metav1.ConditionTrue, "Reconciled", "reconciled successfully", 12, 4*24*time.Hour),
		clitest.Entry(clitest.Res("sqs", "orders")),
		clitest.Entry(clitest.Res("s3", "receipts")),
	)
}

func notReadyCR() *depsv1alpha1.AppDependencies {
	return clitest.CR("checkout", "broken",
		clitest.Condition("Ready", metav1.ConditionFalse, "AdoptRequired", "S3Ready: bucket exists", 12, 12*time.Minute),
		clitest.Condition("S3Ready", metav1.ConditionFalse, "AdoptRequired", "bucket exists", 12, 12*time.Minute),
	)
}

func TestStatus_PrintsTheContextHeaderAndExitsZeroWhenReady(t *testing.T) {
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}}

	r := exec(t, reader, "kubectl-appdeps", "status", "checkout-service", "-n", "checkout")

	if r.code != cmd.ExitOK {
		t.Fatalf("exit = %d, stderr = %q", r.code, r.stderr)
	}
	if !strings.HasPrefix(r.stdout, "Context: prod-eks   Namespace: checkout   CR: checkout-service\n") {
		t.Errorf("stdout should start with the context line:\n%s", r.stdout)
	}
	if !strings.Contains(r.stdout, "Ready: True") {
		t.Errorf("stdout should say Ready: True:\n%s", r.stdout)
	}
	if r.stderr != "" {
		t.Errorf("stderr = %q, want empty", r.stderr)
	}
	if r.opts.Namespace != "checkout" || len(reader.Calls) != 1 || reader.Calls[0] != "get checkout/checkout-service" {
		t.Errorf("opts = %+v, calls = %v", r.opts, reader.Calls)
	}
}

func TestStatus_ExitsOneWhenNotReadyButStillPrints(t *testing.T) {
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{notReadyCR()}}

	r := exec(t, reader, "kubectl-appdeps", "status", "broken", "-n", "checkout")

	if r.code != cmd.ExitNotReady {
		t.Errorf("exit = %d, want %d", r.code, cmd.ExitNotReady)
	}
	if !strings.Contains(r.stdout, "S3Ready: AdoptRequired  (Failing)") {
		t.Errorf("stdout should explain the failing section:\n%s", r.stdout)
	}
}

func TestStatus_ExplainIsAnAlias(t *testing.T) {
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}}

	r := exec(t, reader, "kubectl-appdeps", "explain", "checkout-service", "-n", "checkout")

	if r.code != cmd.ExitOK || !strings.Contains(r.stdout, "Ready: True") {
		t.Errorf("exit = %d, stdout = %q", r.code, r.stdout)
	}
}

func TestStatus_JSONIsTheViewAndKeepsTheReadinessExitCode(t *testing.T) {
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{notReadyCR()}}

	r := exec(t, reader, "kubectl-appdeps", "status", "broken", "-n", "checkout", "-o", "json")

	if r.code != cmd.ExitNotReady {
		t.Errorf("exit = %d, want %d", r.code, cmd.ExitNotReady)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(r.stdout), &doc); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, r.stdout)
	}
	if doc["kind"] != "Status" || doc["name"] != "broken" || doc["ready"] != false {
		t.Errorf("document = %v", doc)
	}
}

func TestStatus_ShowSourceAddsTheSourceLines(t *testing.T) {
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}}

	r := exec(t, reader, "kubectl-appdeps", "status", "checkout-service", "-n", "checkout", "--show-source")

	if !strings.Contains(r.stdout, "  source: status.managedResources") {
		t.Errorf("--show-source should add the source lines:\n%s", r.stdout)
	}
}

func TestStatus_EventsAreShownWithTheirCaveat(t *testing.T) {
	reader := &clitest.FakeReader{
		CRs:       []*depsv1alpha1.AppDependencies{readyCR()},
		EventList: []kube.Event{{Type: "Warning", Reason: "AdoptRequired", Message: "S3Ready: bucket exists", Count: 3, Last: clitest.Now.Add(-2 * time.Minute)}},
	}

	r := exec(t, reader, "kubectl-appdeps", "status", "checkout-service", "-n", "checkout", "--events")

	if !strings.Contains(r.stdout, "Kubernetes keeps events for about an hour; this is not history.") ||
		!strings.Contains(r.stdout, "AdoptRequired") {
		t.Errorf("stdout should list the events with the caveat:\n%s", r.stdout)
	}
	if got := strings.Join(reader.Calls, ";"); !strings.Contains(got, "events checkout/checkout-service") {
		t.Errorf("calls = %s, want an events request", got)
	}
}

func TestStatus_ForbiddenEventsOnlyAddANote(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "events.k8s.io", Resource: "events"}, "", errors.New("no"))
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}, EventsErr: forbidden}

	r := exec(t, reader, "kubectl-appdeps", "status", "checkout-service", "-n", "checkout", "--events")

	if r.code != cmd.ExitOK || !strings.Contains(r.stdout, "Ready: True") {
		t.Errorf("the status should still print: exit = %d, stdout = %q", r.code, r.stdout)
	}
	if !strings.Contains(r.stderr, "events") {
		t.Errorf("stderr should explain that events could not be read: %q", r.stderr)
	}
}

func TestClusterErrors(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "deps.cloudctl.io", Resource: "appdependencies"}, "x", errors.New("no"))

	t.Run("not found", func(t *testing.T) {
		r := exec(t, &clitest.FakeReader{}, "kubectl-appdeps", "status", "missing", "-n", "checkout")

		if r.code != cmd.ExitCluster || r.stdout != "" || !strings.Contains(r.stderr, "not found") {
			t.Errorf("exit = %d, stdout = %q, stderr = %q", r.code, r.stdout, r.stderr)
		}
	})
	t.Run("forbidden carries the RBAC hint", func(t *testing.T) {
		wrapped := errors.Join(kube.ErrForbidden, forbidden)
		r := exec(t, &clitest.FakeReader{GetErr: wrapped}, "kubectl-appdeps", "status", "x", "-n", "checkout")

		if r.code != cmd.ExitCluster || !strings.Contains(r.stderr, kube.ExplainForbidden(forbidden)) {
			t.Errorf("exit = %d, stderr = %q, want the hint %q", r.code, r.stderr, kube.ExplainForbidden(forbidden))
		}
	})
	t.Run("an unreachable cluster", func(t *testing.T) {
		r := exec(t, &clitest.FakeReader{GetErr: errors.New("dial tcp: connection refused")}, "kubectl-appdeps", "status", "x", "-n", "checkout")

		if r.code != cmd.ExitCluster || !strings.Contains(r.stderr, "connection refused") {
			t.Errorf("exit = %d, stderr = %q", r.code, r.stderr)
		}
	})
	t.Run("the reader factory failing", func(t *testing.T) {
		var out, errOut bytes.Buffer
		failing := func(kube.Options) (kube.Reader, kube.Target, error) {
			return nil, kube.Target{}, errors.New("no kubeconfig found")
		}

		code := cmd.Execute([]string{"status", "x"}, "kubectl-appdeps", cmd.Streams{Out: &out, Err: &errOut}, failing, time.Now)

		if code != cmd.ExitCluster || !strings.Contains(errOut.String(), "no kubeconfig found") {
			t.Errorf("exit = %d, stderr = %q", code, errOut.String())
		}
	})
}

func TestUsageErrorsGoToStderrWithExitTwoAndNothingOnStdout(t *testing.T) {
	for name, args := range map[string][]string{
		"missing argument":      {"status"},
		"unknown command":       {"frobnicate"},
		"unknown flag":          {"status", "x", "--nope"},
		"bad output format":     {"status", "x", "-o", "xml"},
		"-A with status":        {"status", "x", "-A"},
		"-A with resources":     {"resources", "x", "-A"},
		"resources no argument": {"resources"},
	} {
		t.Run(name, func(t *testing.T) {
			reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}}

			r := exec(t, reader, "kubectl-appdeps", args...)

			if r.code != cmd.ExitUsage || r.stdout != "" || r.stderr == "" {
				t.Errorf("exit = %d, stdout = %q, stderr = %q", r.code, r.stdout, r.stderr)
			}
			if len(reader.Calls) != 0 {
				t.Errorf("a usage error must not reach the cluster, calls = %v", reader.Calls)
			}
		})
	}
}

func TestHelpUsesTheNameTheBinaryWasStartedUnder(t *testing.T) {
	plugin := exec(t, &clitest.FakeReader{}, "kubectl-appdeps", "--help")
	standalone := exec(t, &clitest.FakeReader{}, "appdeps", "--help")

	if plugin.code != cmd.ExitOK || !strings.Contains(plugin.stdout, "kubectl appdeps status") {
		t.Errorf("as a plugin: exit = %d, help:\n%s", plugin.code, plugin.stdout)
	}
	if standalone.code != cmd.ExitOK || !strings.Contains(standalone.stdout, "appdeps status") || strings.Contains(standalone.stdout, "kubectl appdeps") {
		t.Errorf("standalone: exit = %d, help:\n%s", standalone.code, standalone.stdout)
	}
}

func TestGlobalFlagsReachTheReaderFactory(t *testing.T) {
	reader := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}}

	r := exec(t, reader, "kubectl-appdeps", "status", "checkout-service", "--kubeconfig", "/tmp/kc", "--context", "staging", "-n", "checkout")

	if r.opts.Kubeconfig != "/tmp/kc" || r.opts.Context != "staging" || r.opts.Namespace != "checkout" || r.opts.AllNamespaces {
		t.Errorf("options = %+v", r.opts)
	}
}

func TestResources(t *testing.T) {
	reader := func() *clitest.FakeReader {
		return &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{readyCR()}}
	}

	t.Run("table with the context line", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "resources", "checkout-service", "-n", "checkout")

		if r.code != cmd.ExitOK ||
			!strings.HasPrefix(r.stdout, "Context: prod-eks   Namespace: checkout   CR: checkout-service\n") ||
			!strings.Contains(r.stdout, "TYPE") || !strings.Contains(r.stdout, "orders") || !strings.Contains(r.stdout, "receipts") {
			t.Errorf("exit = %d, stdout:\n%s", r.code, r.stdout)
		}
	})
	t.Run("ledger is an alias", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "ledger", "checkout-service", "-n", "checkout")

		if r.code != cmd.ExitOK || !strings.Contains(r.stdout, "orders") {
			t.Errorf("exit = %d, stdout = %q", r.code, r.stdout)
		}
	})
	t.Run("--type filters", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "resources", "checkout-service", "-n", "checkout", "--type", "sqs")

		if !strings.Contains(r.stdout, "orders") || strings.Contains(r.stdout, "receipts") {
			t.Errorf("stdout:\n%s", r.stdout)
		}
	})
	t.Run("--no-headers drops the header row only", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "resources", "checkout-service", "-n", "checkout", "--no-headers")

		if strings.Contains(r.stdout, "ON-DELETE") || !strings.Contains(r.stdout, "orders") || !strings.HasPrefix(r.stdout, "Context:") {
			t.Errorf("stdout:\n%s", r.stdout)
		}
	})
	t.Run("-o wide adds the ARN column", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "resources", "checkout-service", "-n", "checkout", "-o", "wide")

		if !strings.Contains(r.stdout, "ARN") {
			t.Errorf("stdout:\n%s", r.stdout)
		}
	})
	t.Run("json", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "resources", "checkout-service", "-n", "checkout", "-o", "json")

		var doc struct {
			Kind  string           `json:"kind"`
			Name  string           `json:"name"`
			Items []map[string]any `json:"items"`
		}
		if err := json.Unmarshal([]byte(r.stdout), &doc); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, r.stdout)
		}
		if doc.Kind != "Resources" || doc.Name != "checkout-service" || len(doc.Items) != 2 {
			t.Errorf("document = %+v", doc)
		}
	})
	t.Run("an empty ledger is an empty list", func(t *testing.T) {
		empty := &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{clitest.CR("checkout", "empty")}}

		r := exec(t, empty, "kubectl-appdeps", "resources", "empty", "-n", "checkout", "-o", "json")

		if !strings.Contains(r.stdout, `"items": []`) && !strings.Contains(r.stdout, `"items":[]`) {
			t.Errorf("items should be [] not null:\n%s", r.stdout)
		}
	})
}

func TestRetained(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service", clitest.Entry(clitest.Res("sqs", "orders")))
	other := clitest.CR("payments", "ledger-service", clitest.Entry(clitest.Res("s3", "audit")))
	reader := func() *clitest.FakeReader {
		return &clitest.FakeReader{CRs: []*depsv1alpha1.AppDependencies{cr, other}}
	}

	t.Run("one namespace", func(t *testing.T) {
		rd := reader()
		r := exec(t, rd, "kubectl-appdeps", "retained", "-n", "checkout")

		if r.code != cmd.ExitOK || !strings.HasPrefix(r.stdout, "Context: prod-eks   Namespace: checkout\n") ||
			!strings.Contains(r.stdout, "orders") || strings.Contains(r.stdout, "audit") {
			t.Errorf("exit = %d, stdout:\n%s", r.code, r.stdout)
		}
		if len(rd.Calls) != 1 || rd.Calls[0] != "list checkout" {
			t.Errorf("calls = %v", rd.Calls)
		}
	})
	t.Run("all namespaces", func(t *testing.T) {
		rd := reader()
		r := exec(t, rd, "kubectl-appdeps", "retained", "-A")

		if !strings.HasPrefix(r.stdout, "Context: prod-eks   Namespace: all\n") || !strings.Contains(r.stdout, "orders") || !strings.Contains(r.stdout, "audit") {
			t.Errorf("stdout:\n%s", r.stdout)
		}
		if len(rd.Calls) != 1 || rd.Calls[0] != "list " || !r.opts.AllNamespaces {
			t.Errorf("calls = %v, options = %+v", rd.Calls, r.opts)
		}
	})
	t.Run("it never claims to see what it cannot", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "retained", "-n", "checkout")

		if !strings.Contains(r.stdout, "Key=cloudctl.io/released-from") {
			t.Errorf("the AWS tag-query hint is missing:\n%s", r.stdout)
		}
	})
	t.Run("a forbidden cluster-wide list is a cluster error with the hint", func(t *testing.T) {
		forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "deps.cloudctl.io", Resource: "appdependencies"}, "", errors.New("no"))
		r := exec(t, &clitest.FakeReader{ListErr: errors.Join(kube.ErrForbidden, forbidden)}, "kubectl-appdeps", "retained", "-A")

		if r.code != cmd.ExitCluster || !strings.Contains(r.stderr, kube.ExplainForbidden(forbidden)) {
			t.Errorf("exit = %d, stderr = %q", r.code, r.stderr)
		}
	})
	t.Run("json", func(t *testing.T) {
		r := exec(t, reader(), "kubectl-appdeps", "retained", "-A", "-o", "json")

		var doc struct {
			Kind      string           `json:"kind"`
			Namespace string           `json:"namespace"`
			Items     []map[string]any `json:"items"`
		}
		if err := json.Unmarshal([]byte(r.stdout), &doc); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, r.stdout)
		}
		if doc.Kind != "Retained" || doc.Namespace != "all" || len(doc.Items) != 2 {
			t.Errorf("document = %+v", doc)
		}
	})
}

func TestVersion(t *testing.T) {
	set := func(t *testing.T, v, commit string) {
		t.Helper()
		oldV, oldC := version.Version, version.Commit
		version.Version, version.Commit = v, commit
		t.Cleanup(func() { version.Version, version.Commit = oldV, oldC })
	}

	t.Run("client and server API", func(t *testing.T) {
		set(t, "v0.8.1", "abc1234")
		reader := &clitest.FakeReader{Server: kube.ServerInfo{APIVersions: []string{"deps.cloudctl.io/v1alpha1"}}}

		r := exec(t, reader, "kubectl-appdeps", "version")

		if r.code != cmd.ExitOK || !strings.Contains(r.stdout, "Client: v0.8.1 (abc1234)") ||
			!strings.Contains(r.stdout, "Server API: deps.cloudctl.io/v1alpha1") || strings.Contains(r.stdout, "Operator:") {
			t.Errorf("exit = %d, stdout:\n%s", r.code, r.stdout)
		}
	})
	t.Run("an unreachable server is not an error", func(t *testing.T) {
		set(t, "v0.8.1", "abc1234")

		r := exec(t, &clitest.FakeReader{ServerErr: errors.New("connection refused")}, "kubectl-appdeps", "version")

		if r.code != cmd.ExitOK || !strings.Contains(r.stdout, "Server API: unknown (connection refused)") {
			t.Errorf("exit = %d, stdout:\n%s", r.code, r.stdout)
		}
	})
	t.Run("--server shows the operator and warns on a minor difference", func(t *testing.T) {
		set(t, "v0.8.1", "abc1234")
		reader := &clitest.FakeReader{Server: kube.ServerInfo{OperatorImage: "ghcr.io/ningendo7/cloudctl-operator:v0.9.0"}}

		r := exec(t, reader, "kubectl-appdeps", "version", "--server")

		if !strings.Contains(r.stdout, "Operator: ghcr.io/ningendo7/cloudctl-operator:v0.9.0") ||
			!strings.Contains(r.stdout, "Warning: the CLI is v0.8.1 and the operator is v0.9.0; output may not match") {
			t.Errorf("stdout:\n%s", r.stdout)
		}
	})
	t.Run("--server stays quiet when only the patch differs", func(t *testing.T) {
		set(t, "v0.8.1", "abc1234")
		reader := &clitest.FakeReader{Server: kube.ServerInfo{OperatorImage: "ghcr.io/ningendo7/cloudctl-operator:v0.8.4"}}

		r := exec(t, reader, "kubectl-appdeps", "version", "--server")

		if strings.Contains(r.stdout, "Warning") {
			t.Errorf("a patch difference should not warn:\n%s", r.stdout)
		}
	})
	t.Run("--server with an unreadable operator", func(t *testing.T) {
		set(t, "v0.8.1", "abc1234")

		r := exec(t, &clitest.FakeReader{}, "kubectl-appdeps", "version", "--server")

		if !strings.Contains(r.stdout, "Operator: unknown") || strings.Contains(r.stdout, "Warning") {
			t.Errorf("stdout:\n%s", r.stdout)
		}
	})
	t.Run("json", func(t *testing.T) {
		set(t, "v0.8.1", "abc1234")
		reader := &clitest.FakeReader{Server: kube.ServerInfo{APIVersions: []string{"deps.cloudctl.io/v1alpha1"}}}

		r := exec(t, reader, "kubectl-appdeps", "version", "-o", "json")

		var doc struct {
			Kind   string `json:"kind"`
			Client struct {
				Version string `json:"version"`
				Commit  string `json:"commit"`
			} `json:"client"`
			Server struct {
				APIVersions []string `json:"apiVersions"`
			} `json:"server"`
		}
		if err := json.Unmarshal([]byte(r.stdout), &doc); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, r.stdout)
		}
		if doc.Kind != "Version" || doc.Client.Version != "v0.8.1" || doc.Client.Commit != "abc1234" || len(doc.Server.APIVersions) != 1 {
			t.Errorf("document = %+v", doc)
		}
	})
}
