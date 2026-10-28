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
// themselves, the samples and the admission policies, against a real API
// server, so neither can rot. The policies are the sample rendered from the
// chart, and the chart itself for what the sample does not show. It needs the
// envtest binaries (KUBEBUILDER_ASSETS, set by `make test` and CI) and skips
// without them; the tests that render the chart also need helm.
package policy_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

const (
	// The operator's own finalizer, added and removed with a metadata-only update.
	finalizer = "deps.cloudctl.io/finalizer"

	// The policies take their names from the release the sample is rendered as.
	namespacePolicy  = "cloudctl-operator-enabled-namespaces"
	limitsPolicy     = "cloudctl-operator-limits"
	productionPolicy = "cloudctl-operator-protect-production"
	encryptionPolicy = "cloudctl-operator-require-encryption"
	policyNamespace  = "cloudctl-system"
)

var (
	admin    client.Client
	envCfg   *rest.Config
	scheme   *runtime.Scheme
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
		scheme = runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = depsv1alpha1.AddToScheme(scheme)
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			panic(err)
		}
		admin, envCfg, stopEnv = c, cfg, func() { _ = env.Stop() }
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
	for part := range strings.SplitSeq(string(raw), "\n---\n") {
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

// deniedFor is denied with a reason in the message. A policy whose parameters
// are not there yet also refuses requests, so a probe that waits for a policy
// to be ready has to wait for what the policy itself says.
func deniedFor(err error, reason string) bool {
	return denied(err) && strings.Contains(err.Error(), reason)
}

// sampleDocs are the objects of the shipped sample that carry these names: a
// policy and its binding.
func sampleDocs(t *testing.T, names ...string) []*unstructured.Unstructured {
	t.Helper()
	return pick(t, readDocs(t, filepath.Join(repoRoot, "config", "samples", "policy", "appdependencies-guardrails.yaml")), names...)
}

// chartDocs renders the chart with the policies on and returns the objects
// with these names. It skips the test without helm.
func chartDocs(t *testing.T, templates []string, set map[string]string, names ...string) []*unstructured.Unstructured {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not on the PATH; skipping the tests that render the chart")
	}
	args := []string{"template", "cloudctl-operator", filepath.Join(repoRoot, "charts", "chart"), "--namespace", policyNamespace, "--set", "policies.enabled=true"}
	for k, v := range set {
		args = append(args, "--set", k+"="+v)
	}
	for _, name := range templates {
		args = append(args, "-s", "templates/policy/"+name+".yaml")
	}
	out, err := exec.Command(helm, args...).CombinedOutput() //nolint:gosec // fixed binary, arguments built in this file
	if err != nil {
		t.Fatalf("helm %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	path := filepath.Join(t.TempDir(), "rendered.yaml")
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
	return pick(t, readDocs(t, path), names...)
}

func pick(t *testing.T, docs []*unstructured.Unstructured, names ...string) []*unstructured.Unstructured {
	t.Helper()
	var picked []*unstructured.Unstructured
	for _, doc := range docs {
		for _, name := range names {
			if doc.GetName() == name {
				picked = append(picked, doc)
			}
		}
	}
	if len(picked) == 0 {
		t.Fatalf("found no object named %v", names)
	}
	return picked
}

// install creates policy objects and waits until probe is refused with reason
// in the message. It waits until probe is no longer refused at all after they
// are deleted, so the next test starts clean: a binding whose parameters are
// already gone still refuses requests, with a different message.
func install(t *testing.T, docs []*unstructured.Unstructured, probe func() error, reason string) {
	t.Helper()
	installWith(t, docs, func() bool { return deniedFor(probe(), reason) }, func() bool { return !denied(probe()) })
}

// installWith is install for a policy whose effect is not a refusal.
func installWith(t *testing.T, docs []*unstructured.Unstructured, ready, quiet func() bool) {
	t.Helper()
	t.Cleanup(func() { waitFor(t, "the policy to stop judging requests", quiet) })
	for _, doc := range docs {
		doc = doc.DeepCopy()
		if err := admin.Create(ctx, doc); err != nil {
			t.Fatalf("creating %s %s: %v", doc.GetKind(), doc.GetName(), err)
		}
		t.Cleanup(func() { _ = admin.Delete(ctx, doc) })
	}
	waitFor(t, "the policy to start judging requests", ready)
}

func namespace(t *testing.T, name string, enabled bool) {
	t.Helper()
	labelledNamespace(t, name, enabledLabels(enabled))
}

func enabledLabels(enabled bool) map[string]string {
	if !enabled {
		return nil
	}
	return map[string]string{"cloudctl.io/enabled": "true"}
}

func labelledNamespace(t *testing.T, name string, labels map[string]string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
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
	install(t, sampleDocs(t, namespacePolicy), func() error { return dryRun(appDeps("policy-ns-plain", "probe")) }, "")

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
	install(t, sampleDocs(t, namespacePolicy), func() error { return dryRun(appDeps("policy-ns-probe", "probe")) }, "")

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
	install(t, sampleDocs(t, limitsPolicy), func() error {
		return dryRun(withDatabase(appDeps("policy-limits", "probe"), "db.m5.large", 20))
	}, "instanceClass")

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
	install(t, sampleDocs(t, limitsPolicy), func() error {
		return dryRun(withDatabase(appDeps("policy-tightened", "probe"), "db.m5.large", 20))
	}, "instanceClass")

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

// bare is a CR that declares nothing, for the tests that add one thing.
func bare(ns, name string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
}

func withQueue(cr *depsv1alpha1.AppDependencies, mutate func(*depsv1alpha1.SQSQueueSpec)) *depsv1alpha1.AppDependencies {
	queue := depsv1alpha1.SQSQueueSpec{Name: "orders"}
	mutate(&queue)
	cr.Spec.SQS = &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{queue}}
	return cr
}

func TestProductionPolicy_RefusesForceAndDeleteOnlyInProductionNamespaces(t *testing.T) {
	needCluster(t)
	labelledNamespace(t, "policy-prod", map[string]string{"cloudctl.io/environment": "production"})
	labelledNamespace(t, "policy-dev", map[string]string{"cloudctl.io/environment": "dev"})
	install(t, sampleDocs(t, productionPolicy), func() error {
		return dryRun(withQueue(bare("policy-prod", "probe"), func(q *depsv1alpha1.SQSQueueSpec) { q.Force = true }))
	}, "")

	force := func(q *depsv1alpha1.SQSQueueSpec) { q.Force = true }
	deleteIt := func(q *depsv1alpha1.SQSQueueSpec) { q.DeletionPolicy = depsv1alpha1.DeletionPolicyDelete }
	for name, mutate := range map[string]func(*depsv1alpha1.SQSQueueSpec){"force": force, "deletionPolicy Delete": deleteIt} {
		if err := admin.Create(ctx, withQueue(bare("policy-prod", "refused-"+strings.ToLower(strings.ReplaceAll(name, " ", "-"))), mutate)); !denied(err) {
			t.Errorf("%s in a production namespace: expected the CR to be denied, got %v", name, err)
		}
		if err := admin.Create(ctx, withQueue(bare("policy-dev", "fine-"+strings.ToLower(strings.ReplaceAll(name, " ", "-"))), mutate)); err != nil {
			t.Errorf("%s outside production: expected the CR to be allowed, got %v", name, err)
		}
	}
	if err := admin.Create(ctx, appDeps("policy-prod", "plain")); err != nil {
		t.Errorf("expected a CR with the safe defaults in production to be allowed, got %v", err)
	}
}

// A production CR that already asks for Delete must stay deletable.
func TestProductionPolicy_DoesNotBlockDeletion(t *testing.T) {
	needCluster(t)
	labelledNamespace(t, "policy-prod-existing", map[string]string{"cloudctl.io/environment": "production"})
	labelledNamespace(t, "policy-prod-probe", map[string]string{"cloudctl.io/environment": "production"})
	deleteIt := func(q *depsv1alpha1.SQSQueueSpec) { q.DeletionPolicy = depsv1alpha1.DeletionPolicyDelete }
	if err := admin.Create(ctx, withFinalizer(withQueue(bare("policy-prod-existing", "checkout"), deleteIt))); err != nil {
		t.Fatal(err)
	}
	install(t, sampleDocs(t, productionPolicy), func() error {
		return dryRun(withQueue(bare("policy-prod-probe", "probe"), deleteIt))
	}, "")

	key := client.ObjectKey{Namespace: "policy-prod-existing", Name: "checkout"}
	cr := &depsv1alpha1.AppDependencies{}
	if err := admin.Get(ctx, key, cr); err != nil {
		t.Fatal(err)
	}
	edited := cr.DeepCopy()
	edited.Spec.SQS.Resources[0].Overrides = &depsv1alpha1.SQSOverrides{VisibilityTimeoutSeconds: new(int32(60))}
	if err := admin.Update(ctx, edited); !denied(err) {
		t.Errorf("expected a spec change that keeps Delete to be denied, got %v", err)
	}
	deleteLikeTheOperator(t, key.Namespace, key.Name)
}

func TestEncryptionPolicy_RequiresEncryptionAndRefusesAnRDSOptOut(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-encryption", true)
	install(t, sampleDocs(t, encryptionPolicy), func() error {
		return dryRun(withQueue(bare("policy-encryption", "probe"), func(*depsv1alpha1.SQSQueueSpec) {}))
	}, "")

	encrypted := func(q *depsv1alpha1.SQSQueueSpec) { q.Encryption = &depsv1alpha1.EncryptionSpec{Enabled: true} }
	if err := admin.Create(ctx, withQueue(bare("policy-encryption", "plain"), func(*depsv1alpha1.SQSQueueSpec) {})); !denied(err) {
		t.Errorf("expected a queue without encryption to be denied, got %v", err)
	}
	if err := admin.Create(ctx, withQueue(bare("policy-encryption", "sealed"), encrypted)); err != nil {
		t.Errorf("expected an encrypted queue to be allowed, got %v", err)
	}

	optOut := withDatabase(bare("policy-encryption", "opt-out"), "db.t4g.small", 100)
	optOut.Spec.RDS.Resources[0].Encryption = &depsv1alpha1.EncryptionSpec{Enabled: false}
	if err := admin.Create(ctx, optOut); !denied(err) {
		t.Errorf("expected an RDS instance that opts out of encryption to be denied, got %v", err)
	}
	if err := admin.Create(ctx, withDatabase(bare("policy-encryption", "default"), "db.t4g.small", 100)); err != nil {
		t.Errorf("expected an RDS instance with the default encryption to be allowed, got %v", err)
	}
}

// A namespace labelled with a profile gets that profile's caps instead of the
// defaults, and a cap the profile leaves out stays the default's.
func TestLimitsPolicy_AProfileReplacesTheCapsOfItsNamespaces(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-profile-default", true)
	labelledNamespace(t, "policy-profile-large", map[string]string{"cloudctl.io/policy-profile": "large"})
	docs := chartDocs(t, []string{"limits"}, map[string]string{
		"policies.action": "Deny",
		"policies.limits.profiles.large.rds.maxStorage": "2000",
	}, limitsPolicy, limitsPolicy+"-large")
	install(t, docs, func() error {
		return dryRun(withDatabase(appDeps("policy-profile-default", "probe"), "db.t4g.small", 501))
	}, "allocatedStorage")
	waitFor(t, "the profile binding to start judging requests", func() bool {
		return deniedFor(dryRun(withDatabase(appDeps("policy-profile-large", "probe"), "db.t4g.small", 2001)), "allocatedStorage")
	})

	for name, tc := range map[string]struct {
		ns            string
		class         string
		storage       int32
		ceiling       *int32
		wantDenied    bool
		wantDescribed string
	}{
		"default namespace, over the default cap":    {"policy-profile-default", "db.t4g.small", 1000, nil, true, "allocatedStorage"},
		"profile namespace, inside its own cap":      {"policy-profile-large", "db.t4g.small", 1000, nil, false, ""},
		"profile namespace, over its own cap":        {"policy-profile-large", "db.t4g.small", 2500, nil, true, "allocatedStorage"},
		"profile namespace, a class not in the list": {"policy-profile-large", "db.m5.large", 100, nil, true, "instanceClass"},
		"autoscaling ceiling over the cap":           {"policy-profile-default", "db.t4g.small", 100, new(int32(600)), true, "maxAllocatedStorage"},
		"autoscaling ceiling inside the cap":         {"policy-profile-default", "db.t4g.small", 100, new(int32(400)), false, ""},
	} {
		cr := withDatabase(bare(tc.ns, ""), tc.class, tc.storage)
		cr.GenerateName = "profile-"
		cr.Spec.RDS.Resources[0].MaxAllocatedStorage = tc.ceiling
		err := admin.Create(ctx, cr)
		if got := denied(err); got != tc.wantDenied {
			t.Errorf("%s: denied = %v, want %v (%v)", name, got, tc.wantDenied, err)
		}
		if tc.wantDenied && err != nil && !strings.Contains(err.Error(), tc.wantDescribed) {
			t.Errorf("%s: expected the message to mention %q, got %v", name, tc.wantDescribed, err)
		}
	}
}

type warnings struct {
	mu  sync.Mutex
	got []string
}

func (w *warnings) HandleWarningHeader(_ int, _ string, text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.got = append(w.got, text)
}

func (w *warnings) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.got)
}

// In Warn mode the request goes through and the user is told why it would
// have been refused.
func TestWarnMode_LetsTheRequestThroughWithAWarning(t *testing.T) {
	needCluster(t)
	namespace(t, "policy-warn-plain", false)
	seen := &warnings{}
	cfg := rest.CopyConfig(envCfg)
	cfg.WarningHandler = seen
	warned, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	docs := chartDocs(t, []string{"enabled-namespaces"}, map[string]string{"policies.action": "Warn"}, namespacePolicy)
	warns := func() bool {
		before := seen.count()
		_ = warned.Create(ctx, appDeps("policy-warn-plain", "probe"), client.DryRunAll)
		return seen.count() > before
	}
	installWith(t, docs, warns, func() bool { return !warns() })

	before := seen.count()
	if err := admin.Create(ctx, appDeps("policy-warn-plain", "checkout")); err != nil {
		t.Errorf("expected Warn mode to allow the CR, got %v", err)
	}
	if err := warned.Create(ctx, appDeps("policy-warn-plain", "checkout-two")); err != nil {
		t.Errorf("expected Warn mode to allow the CR, got %v", err)
	}
	if seen.count() == before {
		t.Error("expected a warning telling the user the namespace is not labelled")
	}
}
