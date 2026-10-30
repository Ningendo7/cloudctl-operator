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

// Package cli_test holds the guarantees about the whole CLI that no single
// package owns. They read the source of internal/cli, so they fail the build
// when a rule is broken, not only when a test happens to exercise it.
package cli_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/clitest"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/view"
)

// cliFiles returns the Go files of the CLI: everything under internal/cli and
// cmd/kubectl-appdeps, tests excluded unless withTests.
func cliFiles(t *testing.T, withTests bool) []string {
	t.Helper()
	var files []string
	for _, root := range []string{".", filepath.Join("..", "..", "cmd", "kubectl-appdeps")} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			if !withTests && strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(files) == 0 {
		t.Fatal("found no CLI source files; the walk is looking in the wrong place")
	}
	return files
}

// The CLI has no AWS credentials and makes no AWS calls, now or by accident.
func TestTheCLIDoesNotImportAnythingAWS(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range cliFiles(t, true) {
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(p, "github.com/aws/") ||
				strings.HasSuffix(p, "/internal/aws") ||
				strings.Contains(p, "/internal/resources/") ||
				strings.HasSuffix(p, "/internal/controller") {
				t.Errorf("%s imports %s; the CLI must not depend on AWS or on the operator's internals", path, p)
			}
		}
	}
}

// The CLI never reads a Secret, and never writes to the cluster.
func TestTheCLISourceNeverTouchesSecretsOrWrites(t *testing.T) {
	forbidden := []string{
		"corev1.Secret", "SecretList", `"secrets"`, "Secrets(",
		".Create(", ".Update(", ".Patch(", ".Delete(", ".DeleteAllOf(", ".Apply(",
	}
	for _, path := range cliFiles(t, false) {
		raw, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range forbidden {
			if strings.Contains(string(raw), bad) {
				t.Errorf("%s contains %q; the CLI is read-only and never reads Secrets", path, bad)
			}
		}
	}
}

// Only internal/cli/kube may talk to an API server.
func TestOnlyTheKubePackageImportsAClient(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range cliFiles(t, false) {
		if strings.Contains(filepath.ToSlash(path), "/kube/") || strings.HasPrefix(filepath.ToSlash(path), "kube/") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(p, "k8s.io/client-go") || strings.HasPrefix(p, "sigs.k8s.io/controller-runtime/pkg/client") {
				t.Errorf("%s imports %s; only internal/cli/kube may reach the cluster", path, p)
			}
		}
	}
}

// stringConsts returns the string constants of the package in dir whose names
// start with prefix (case-insensitively), by name, tests excluded.
func stringConsts(t *testing.T, dir, prefix string) (map[string]string, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	consts := map[string]string{}
	var files []*ast.File
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, name := range spec.Names {
				if i >= len(spec.Values) || !strings.HasPrefix(strings.ToLower(name.Name), strings.ToLower(prefix)) {
					continue
				}
				if lit, ok := spec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					value, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					consts[name.Name] = value
				}
			}
			return true
		})
	}
	if len(consts) == 0 {
		t.Fatalf("found no %s constants in %s; the guard is reading the wrong place", prefix, dir)
	}
	return consts, files
}

// The controller decides which conditions are sections (they make up Ready)
// and which are not; the CLI must file each one the same way. Both lists are
// read from the controller's source, so a condition added there and not to the
// CLI fails here instead of being misfiled silently.
func TestTheCLIFilesEveryConditionTheWayTheControllerDoes(t *testing.T) {
	controllerConsts, controllerFiles := stringConsts(t, filepath.Join("..", "controller"), "conditionType")
	statusConsts, _ := stringConsts(t, filepath.Join("..", "status"), "ConditionType")

	var sectionNames []string
	for _, file := range controllerFiles {
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.ValueSpec)
			if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "sectionTypes" || len(spec.Values) != 1 {
				return true
			}
			if list, ok := spec.Values[0].(*ast.CompositeLit); ok {
				for _, elt := range list.Elts {
					if ident, ok := elt.(*ast.Ident); ok {
						sectionNames = append(sectionNames, ident.Name)
					}
				}
			}
			return false
		})
	}
	if len(sectionNames) == 0 {
		t.Fatal("found no sectionTypes in the controller; the guard is reading the wrong place")
	}

	sections := map[string]bool{}
	for _, name := range sectionNames {
		value, ok := controllerConsts[name]
		if !ok {
			t.Fatalf("sectionTypes names %s, which is not a string constant this guard found", name)
		}
		sections[value] = true
	}
	all := map[string]bool{}
	for _, consts := range []map[string]string{controllerConsts, statusConsts} {
		for _, value := range consts {
			all[value] = true
		}
	}
	delete(all, "Ready") // the aggregate, shown on its own line

	opts := []clitest.Option{clitest.Condition("Ready", metav1.ConditionFalse, "SomethingFailed", "x", 12, time.Minute)}
	for condType := range all {
		opts = append(opts, clitest.Condition(condType, metav1.ConditionFalse, "SomethingFailed", "x", 12, time.Minute))
	}
	v := view.Status(clitest.CR("checkout", "checkout-service", opts...), kube.Target{}, clitest.Now)

	filed := map[string]string{}
	for _, s := range v.Sections {
		filed[s.Type] = "a section"
	}
	for _, s := range v.Other {
		filed[s.Type] = "another condition"
	}
	for _, condType := range slices.Sorted(maps.Keys(all)) {
		want := "another condition"
		if sections[condType] {
			want = "a section"
		}
		if filed[condType] != want {
			t.Errorf("the controller treats %s as %s, but the CLI files it as %q", condType, want, filed[condType])
		}
	}
}
