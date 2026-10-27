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

// Package policy_test runs what the repository ships for people to apply
// themselves, the samples and the example admission policy, against a real API
// server, so neither can rot. It needs the envtest binaries (KUBEBUILDER_ASSETS,
// set by `make test` and CI) and skips without them.
package policy_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

const (
	// The operator's own finalizer, added and removed with a metadata-only update.
	finalizer = "deps.cloudctl.io/finalizer"

	namespacePolicy = "appdependencies-enabled-namespaces"
	limitsPolicy    = "appdependencies-limits"
)

var (
	admin    client.Client
	stopEnv  func()
	ctx      = context.Background()
	repoRoot = filepath.Join("..", "..")
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
		admin, stopEnv = c, func() { _ = env.Stop() }
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

// readDocs splits a multi-document YAML file into objects.
func readDocs(t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	var docs []*unstructured.Unstructured
	for _, part := range strings.Split(string(raw), "\n---\n") {
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(part), &obj); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if len(obj) > 0 {
			docs = append(docs, &unstructured.Unstructured{Object: obj})
		}
	}
	return docs
}

// waitFor polls until cond holds. Admission policies are compiled
// asynchronously, so a policy that was just created or deleted takes a moment
// to start or stop judging requests.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func denied(err error) bool {
	return err != nil && strings.Contains(err.Error(), "denied request")
}

// install creates one example policy and its binding, and waits until it is
// judging requests (enforcing reports true). It waits for the policy to be gone
// again before the test ends, so the next test starts clean.
func install(t *testing.T, name string, enforcing func() bool) {
	t.Helper()
	t.Cleanup(func() {
		waitFor(t, name+" to stop judging requests", func() bool { return !enforcing() })
	})
	policyFile := filepath.Join(repoRoot, "config", "samples", "policy", "appdependencies-guardrails.yaml")
	for _, doc := range readDocs(t, policyFile) {
		if doc.GetName() != name {
			continue
		}
		if err := admin.Create(ctx, doc); err != nil {
			t.Fatalf("creating %s %s: %v", doc.GetKind(), name, err)
		}
		t.Cleanup(func() { _ = admin.Delete(ctx, doc) })
	}
	waitFor(t, name+" to start judging requests", enforcing)
}

func namespace(t *testing.T, name string, enabled bool) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if enabled {
		ns.Labels = map[string]string{"cloudctl.io/enabled": "true"}
	}
	if err := admin.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
}

func appDeps(ns, name string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}},
		},
	}
}

func withDatabase(cr *depsv1alpha1.AppDependencies, instanceClass string, storage int32) *depsv1alpha1.AppDependencies {
	cr.Spec.RDS = &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
		Name: "orders-db", DBSubnetGroupName: "prod-private-data-tier", Engine: "postgres", EngineVersion: "16",
		InstanceClass: instanceClass, AllocatedStorage: storage,
	}}}
	return cr
}

func withFinalizer(cr *depsv1alpha1.AppDependencies) *depsv1alpha1.AppDependencies {
	cr.Finalizers = []string{finalizer}
	return cr
}

func dryRun(cr *depsv1alpha1.AppDependencies) error {
	return admin.Create(ctx, cr, client.DryRunAll)
}

// deleteLikeTheOperator deletes the CR and then removes the operator's
// finalizer the way the operator does, with an update that touches no spec.
func deleteLikeTheOperator(t *testing.T, ns, name string) {
	t.Helper()
	cr := &depsv1alpha1.AppDependencies{}
	key := client.ObjectKey{Namespace: ns, Name: name}
	if err := admin.Get(ctx, key, cr); err != nil {
		t.Fatal(err)
	}
	if err := admin.Delete(ctx, cr); err != nil {
		t.Fatal(err)
	}
	if err := admin.Get(ctx, key, cr); err != nil {
		t.Fatal(err)
	}
	if cr.DeletionTimestamp == nil {
		t.Fatal("expected the finalizer to hold the CR in deletion")
	}
	controllerutil.RemoveFinalizer(cr, finalizer)
	if err := admin.Update(ctx, cr); err != nil {
		t.Fatalf("removing the finalizer was refused, so the CR could never be deleted: %v", err)
	}
	if err := admin.Get(ctx, key, cr); err == nil {
		t.Fatal("expected the CR to be gone once its finalizer was removed")
	}
}

func TestSamplesApplyAndAreValid(t *testing.T) {
	needCluster(t)
	var kustomization struct {
		Resources []string `json:"resources"`
	}
	raw, err := os.ReadFile(filepath.Clean(filepath.Join(repoRoot, "config", "samples", "kustomization.yaml")))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &kustomization); err != nil {
		t.Fatal(err)
	}
	if len(kustomization.Resources) == 0 {
		t.Fatal("found no samples in config/samples/kustomization.yaml")
	}
	for _, file := range kustomization.Resources {
		for _, doc := range readDocs(t, filepath.Join(repoRoot, "config", "samples", file)) {
			if doc.GetNamespace() == "" && doc.GetKind() == "AppDependencies" {
				doc.SetNamespace("default")
			}
			if err := admin.Create(ctx, doc); err != nil {
				t.Errorf("%s: applying the %s sample %q failed: %v", file, doc.GetKind(), doc.GetName(), err)
				continue
			}
			t.Cleanup(func() { _ = admin.Delete(ctx, doc) })
		}
	}
}

func TestNamespacePolicy_OnlyEnabledNamespacesMayCreateCRs(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-ns-enabled", true)
	namespace(t, "policy-ns-plain", false)
	install(t, namespacePolicy, func() bool { return denied(dryRun(appDeps("policy-ns-plain", "probe"))) })

	if err := admin.Create(ctx, appDeps("policy-ns-plain", "checkout")); !denied(err) {
		t.Errorf("expected a CR in an unlabelled namespace to be denied, got %v", err)
	}
	if err := admin.Create(ctx, appDeps("policy-ns-enabled", "checkout")); err != nil {
		t.Errorf("expected a CR in a labelled namespace to be allowed, got %v", err)
	}
}

// Removing the label must not strand the CRs already in the namespace: the
// operator's finalizer update has to keep working, or deleting the CR (and the
// namespace) hangs for ever.
func TestNamespacePolicy_RemovingTheLabelDoesNotBlockDeletion(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-ns-unlabelled-later", true)
	namespace(t, "policy-ns-probe", false)
	install(t, namespacePolicy, func() bool { return denied(dryRun(appDeps("policy-ns-probe", "probe"))) })

	if err := admin.Create(ctx, withFinalizer(appDeps("policy-ns-unlabelled-later", "checkout"))); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{}
	if err := admin.Get(ctx, client.ObjectKey{Name: "policy-ns-unlabelled-later"}, ns); err != nil {
		t.Fatal(err)
	}
	delete(ns.Labels, "cloudctl.io/enabled")
	if err := admin.Update(ctx, ns); err != nil {
		t.Fatal(err)
	}

	cr := &depsv1alpha1.AppDependencies{}
	key := client.ObjectKey{Namespace: "policy-ns-unlabelled-later", Name: "checkout"}
	waitFor(t, "the label removal to take effect", func() bool {
		if err := admin.Get(ctx, key, cr); err != nil {
			return false
		}
		edited := cr.DeepCopy()
		edited.Spec.SQS.Resources[0].Name = "orders-edited"
		return denied(admin.Update(ctx, edited))
	})

	// A change that touches no spec is what the operator does to its finalizer.
	if err := admin.Get(ctx, key, cr); err != nil {
		t.Fatal(err)
	}
	cr.Annotations = map[string]string{"example": "metadata-only"}
	if err := admin.Update(ctx, cr); err != nil {
		t.Errorf("expected a metadata-only update in an unlabelled namespace to be allowed, got %v", err)
	}
	deleteLikeTheOperator(t, key.Namespace, key.Name)
}

func TestLimitsPolicy_DeniesOversizedDatabases(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-limits", true)
	install(t, limitsPolicy, func() bool {
		return denied(dryRun(withDatabase(appDeps("policy-limits", "probe"), "db.m5.large", 20)))
	})

	for name, cr := range map[string]*depsv1alpha1.AppDependencies{
		"an instance class outside the list": withDatabase(appDeps("policy-limits", "big-class"), "db.r6g.xlarge", 20),
		"storage above the cap":              withDatabase(appDeps("policy-limits", "big-storage"), "db.t4g.small", 501),
	} {
		if err := admin.Create(ctx, cr); !denied(err) {
			t.Errorf("%s: expected the CR to be denied, got %v", name, err)
		}
	}
	if err := admin.Create(ctx, withDatabase(appDeps("policy-limits", "fine"), "db.t4g.small", 100)); err != nil {
		t.Errorf("expected a CR inside the limits to be allowed, got %v", err)
	}
	if err := admin.Create(ctx, appDeps("policy-limits", "no-database")); err != nil {
		t.Errorf("expected a CR with no database to be allowed, got %v", err)
	}
}

// Tightening the limits below what a CR already declares must not make that CR
// undeletable, because the finalizer update re-validates an unchanged spec.
func TestLimitsPolicy_TightenedLimitsDoNotBlockDeletion(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-tightened", true)
	for _, name := range []string{"stays", "goes"} {
		cr := withFinalizer(withDatabase(appDeps("policy-tightened", name), "db.r6g.xlarge", 800))
		if err := admin.Create(ctx, cr); err != nil {
			t.Fatal(err)
		}
	}
	install(t, limitsPolicy, func() bool {
		return denied(dryRun(withDatabase(appDeps("policy-tightened", "probe"), "db.m5.large", 20)))
	})

	stays := &depsv1alpha1.AppDependencies{}
	if err := admin.Get(ctx, client.ObjectKey{Namespace: "policy-tightened", Name: "stays"}, stays); err != nil {
		t.Fatal(err)
	}
	stays.Spec.RDS.Resources[0].EngineVersion = "17"
	if err := admin.Update(ctx, stays); !denied(err) {
		t.Errorf("expected an edit that leaves the CR over the limits to be denied, got %v", err)
	}

	deleteLikeTheOperator(t, "policy-tightened", "goes")
}
