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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
)

// These tests run the reader against a real API server. They need the
// envtest binaries (KUBEBUILDER_ASSETS, set by `make test` and CI) and skip
// without them.
var (
	envCfg   *rest.Config
	admin    client.Client
	stopEnv  func()
	ctx      = context.Background()
	repoRoot = filepath.Join("..", "..", "..")
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		env := &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
			ErrorIfCRDPathMissing: true,
		}
		cfg, err := env.Start()
		if err != nil {
			panic(err)
		}
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = depsv1alpha1.AddToScheme(scheme)
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			panic(err)
		}
		envCfg, admin, stopEnv = cfg, c, func() { _ = env.Stop() }
	}
	code := m.Run()
	if stopEnv != nil {
		stopEnv()
	}
	os.Exit(code)
}

func needCluster(t *testing.T) {
	t.Helper()
	if admin == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set; skipping the real API server tests")
	}
}

// kubeconfigAs writes a kubeconfig for the envtest cluster. A non-empty user
// makes every request impersonate that user.
func kubeconfigAs(t *testing.T, user string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["envtest"] = &clientcmdapi.Cluster{Server: envCfg.Host, CertificateAuthorityData: envCfg.CAData}
	cfg.AuthInfos["admin"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: envCfg.CertData, ClientKeyData: envCfg.KeyData, Token: envCfg.BearerToken, Impersonate: user,
	}
	cfg.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "admin", Namespace: "checkout"}
	cfg.CurrentContext = "envtest"
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		t.Fatal(err)
	}
	return path
}

type recorder struct {
	mu       sync.Mutex
	requests []string
}

func (r *recorder) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		r.mu.Lock()
		r.requests = append(r.requests, req.Method+" "+req.URL.Path)
		r.mu.Unlock()
		return next.RoundTrip(req)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

var seeded sync.Once

// seed creates the namespace, a CR with a status and an event, and the
// operator Deployment, once for every test in this package.
func seed(t *testing.T) {
	t.Helper()
	seeded.Do(func() {
		must := func(err error) {
			if err != nil && !strings.Contains(err.Error(), "already exists") {
				panic(err)
			}
		}
		for _, ns := range []string{"checkout", "payments", "cloudctl-operator-system"} {
			must(admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		}
		for _, ns := range []string{"checkout", "payments"} {
			cr := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: ns + "-service"},
				Spec:       depsv1alpha1.AppDependenciesSpec{SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}},
			}
			must(admin.Create(ctx, cr))
			cr.Status.Conditions = []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSectionsReady", Message: "ok",
				LastTransitionTime: metav1.Now(), ObservedGeneration: cr.Generation,
			}}
			must(admin.Status().Update(ctx, cr))
		}
		must(admin.Create(ctx, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{Namespace: "checkout", Name: "ev1"},
			EventTime:  metav1.NewMicroTime(time.Now()), Reason: "AdoptRequired", Note: "S3Ready: bucket exists", Type: "Warning",
			Action: "Reconcile", ReportingController: "cloudctl-operator", ReportingInstance: "cloudctl-operator-0",
			Regarding: corev1.ObjectReference{Kind: "AppDependencies", APIVersion: "deps.cloudctl.io/v1alpha1", Namespace: "checkout", Name: "checkout-service"},
		}))
		labels := map[string]string{"app.kubernetes.io/name": "cloudctl-operator"}
		must(admin.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "cloudctl-operator-system", Name: "cloudctl-operator-controller-manager", Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "manager", Image: "ghcr.io/ningendo7/cloudctl-operator:v0.8.1"}}},
				},
			},
		}))

		// A user who may do only what the shipped viewer role allows.
		raw, err := os.ReadFile(filepath.Clean(filepath.Join(repoRoot, "config", "rbac", "appdependencies_viewer_role.yaml")))
		if err != nil {
			panic(err)
		}
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal(raw, &role); err != nil {
			panic(err)
		}
		must(admin.Create(ctx, &role))
		must(admin.Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "viewer-binding"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role.Name},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "viewer"}},
		}))
	})
}

func newReader(t *testing.T, user string, rec *recorder) kube.Reader {
	t.Helper()
	opts := kube.Options{Kubeconfig: kubeconfigAs(t, user)}
	if rec != nil {
		opts.WrapTransport = rec.wrap
	}
	reader, _, err := kube.New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return reader
}

func TestReader_GetAndList(t *testing.T) {
	needCluster(t)
	seed(t)
	reader := newReader(t, "", nil)

	cr, err := reader.Get(ctx, "checkout", "checkout-service")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if cr.Name != "checkout-service" || len(cr.Status.Conditions) != 1 || cr.Status.Conditions[0].Type != "Ready" {
		t.Errorf("Get() = %+v, want the CR with its status", cr)
	}

	one, err := reader.List(ctx, "checkout")
	if err != nil || len(one) != 1 {
		t.Errorf("List(checkout) = %d items, err = %v, want 1", len(one), err)
	}
	all, err := reader.List(ctx, "")
	if err != nil || len(all) < 2 {
		t.Errorf("List(all) = %d items, err = %v, want at least 2", len(all), err)
	}
}

func TestReader_NotFoundIsErrNotFound(t *testing.T) {
	needCluster(t)
	seed(t)

	_, err := newReader(t, "", nil).Get(ctx, "checkout", "missing")

	if !errors.Is(err, kube.ErrNotFound) {
		t.Errorf("Get() error = %v, want it to match kube.ErrNotFound", err)
	}
}

func TestReader_EventsForOneCR(t *testing.T) {
	needCluster(t)
	seed(t)

	events, err := newReader(t, "", nil).Events(ctx, "checkout", "checkout-service")

	if err != nil || len(events) != 1 || events[0].Reason != "AdoptRequired" || events[0].Type != "Warning" || events[0].Message != "S3Ready: bucket exists" {
		t.Errorf("Events() = %+v, err = %v", events, err)
	}
	other, err := newReader(t, "", nil).Events(ctx, "payments", "payments-service")
	if err != nil || len(other) != 0 {
		t.Errorf("another CR's events = %+v, err = %v, want none", other, err)
	}
}

func TestReader_ServerVersions(t *testing.T) {
	needCluster(t)
	seed(t)

	info, err := newReader(t, "", nil).ServerVersions(ctx)

	if err != nil {
		t.Fatalf("ServerVersions() error = %v", err)
	}
	if len(info.APIVersions) == 0 || !contains(info.APIVersions, "deps.cloudctl.io/v1alpha1") {
		t.Errorf("APIVersions = %v, want deps.cloudctl.io/v1alpha1", info.APIVersions)
	}
	if info.OperatorImage != "ghcr.io/ningendo7/cloudctl-operator:v0.8.1" {
		t.Errorf("OperatorImage = %q", info.OperatorImage)
	}
}

// The CLI is meant to work with nothing beyond the shipped viewer role.
func TestReader_AViewerOnlyUserCanReadWhatTheCommandsNeed(t *testing.T) {
	needCluster(t)
	seed(t)
	reader := newReader(t, "viewer", nil)

	if _, err := reader.Get(ctx, "checkout", "checkout-service"); err != nil {
		t.Errorf("Get() as the viewer error = %v", err)
	}
	if list, err := reader.List(ctx, ""); err != nil || len(list) < 2 {
		t.Errorf("cluster-wide List() as the viewer = %d items, err = %v", len(list), err)
	}
	info, err := reader.ServerVersions(ctx)
	if err != nil {
		t.Fatalf("ServerVersions() as the viewer must not fail when the Deployment is unreadable: %v", err)
	}
	if info.OperatorImage != "" || !contains(info.APIVersions, "deps.cloudctl.io/v1alpha1") {
		t.Errorf("info = %+v, want the API versions and an unknown operator", info)
	}
}

func TestReader_ForbiddenIsErrForbiddenAndExplained(t *testing.T) {
	needCluster(t)
	seed(t)

	_, err := newReader(t, "viewer", nil).Events(ctx, "checkout", "checkout-service")

	if !errors.Is(err, kube.ErrForbidden) {
		t.Fatalf("Events() as the viewer error = %v, want it to match kube.ErrForbidden", err)
	}
	if hint := kube.ExplainForbidden(err); !strings.Contains(hint, "events") {
		t.Errorf("hint = %q, want it to name events", hint)
	}
	nobody := newReader(t, "nobody", nil)
	if _, err := nobody.Get(ctx, "checkout", "checkout-service"); !errors.Is(err, kube.ErrForbidden) {
		t.Errorf("Get() as a user with no rights error = %v, want kube.ErrForbidden", err)
	}
}

// "Never reads a Secret, never writes" as an executable guarantee: whatever the
// reader does, no request names secrets and every request is a read.
func TestReader_NeverTouchesSecretsAndNeverWrites(t *testing.T) {
	needCluster(t)
	seed(t)
	rec := &recorder{}
	reader := newReader(t, "", rec)

	_, _ = reader.Get(ctx, "checkout", "checkout-service")
	_, _ = reader.List(ctx, "")
	_, _ = reader.List(ctx, "checkout")
	_, _ = reader.Events(ctx, "checkout", "checkout-service")
	_, _ = reader.ServerVersions(ctx)

	if len(rec.requests) == 0 {
		t.Fatal("the recording transport saw no requests; WrapTransport is not being applied")
	}
	for _, req := range rec.requests {
		if !strings.HasPrefix(req, "GET ") {
			t.Errorf("request %q is not a read", req)
		}
		if strings.Contains(req, "secrets") {
			t.Errorf("request %q touches secrets", req)
		}
	}
}

func contains(items []string, want string) bool {
	for _, s := range items {
		if s == want {
			return true
		}
	}
	return false
}
