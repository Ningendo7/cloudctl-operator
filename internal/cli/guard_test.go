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
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
