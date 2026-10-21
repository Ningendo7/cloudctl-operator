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

package reasons

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite docs/reasons.md from the catalogue")

// conditionReason is what the API server accepts as a condition reason.
var conditionReason = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`)

func TestEveryReasonIsExplained(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range All() {
		if seen[r.Name] {
			t.Errorf("%s is registered twice", r.Name)
		}
		seen[r.Name] = true
		if !conditionReason.MatchString(r.Name) {
			t.Errorf("%q is not a valid condition reason", r.Name)
		}
		if r.Meaning == "" {
			t.Errorf("%s has no meaning", r.Name)
		}
		if r.Kind != Info && r.Action == "" {
			t.Errorf("%s needs a next step, since it is %s", r.Name, r.Kind)
		}
	}
}

func TestLookupFallsBackToAFamily(t *testing.T) {
	if r, ok := Lookup("AdoptRequired"); !ok || r.Prefix {
		t.Errorf("an exact reason should be found as itself, got %+v", r)
	}
	if r, ok := Lookup("InstanceModifying"); !ok || !r.Prefix || r.Kind != Waiting {
		t.Errorf("an RDS status reason should fall back to the Instance family, got %+v", r)
	}
	if r, ok := Lookup("InstanceFailed"); !ok || r.Prefix || r.Kind != Failing {
		t.Errorf("InstanceFailed is its own entry, not the family's, got %+v", r)
	}
	if _, ok := Lookup("NotAReason"); ok {
		t.Error("an unknown reason should not be found")
	}
}

// TestNoReasonLiteralsOutsideTheCatalogue fails when code outside this
// package writes a reason as a string literal, which would put it beyond the
// catalogue and leave it unexplained.
func TestNoReasonLiteralsOutsideTheCatalogue(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "bin", "vendor", ".git", "reasons":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.KeyValueExpr:
				if key, ok := v.Key.(*ast.Ident); ok && key.Name == "Reason" {
					flagLiteral(t, fset, v.Value)
				}
			case *ast.CallExpr:
				if sel, ok := v.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetSectionCondition" && len(v.Args) > 3 {
					flagLiteral(t, fset, v.Args[3])
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func flagLiteral(t *testing.T, fset *token.FileSet, e ast.Expr) {
	t.Helper()
	if lit, ok := e.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		t.Errorf("%s: reason %s is a string literal; add it to internal/reasons and use the constant", fset.Position(lit.Pos()), lit.Value)
	}
}

func TestReasonsDocIsUpToDate(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "reasons.md")
	want := Markdown()
	if *update {
		if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v; generate it with: go test ./internal/reasons -run Doc -update", err)
	}
	if string(got) != want {
		t.Error("docs/reasons.md is out of date; regenerate it with: go test ./internal/reasons -run Doc -update")
	}
}

func TestInstanceStatus(t *testing.T) {
	for in, want := range map[string]string{
		"creating":                        "InstanceCreating",
		"configuring-enhanced-monitoring": "InstanceConfiguringEnhancedMonitoring",
		"failed":                          "InstanceFailed",
	} {
		if got := InstanceStatus(in); got != want {
			t.Errorf("InstanceStatus(%q) = %q, want %q", in, got, want)
		}
	}
}
