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

package controller

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Status is only ever patched. Status().Update sends the whole status and
// fails on any concurrent write to the CR, which would drop a section's
// result or ledger entry on the floor.
func TestStatusIsNeverWrittenWithUpdate(t *testing.T) {
	fset := token.NewFileSet()
	scanned := 0
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			if isStatusUpdate(n) {
				t.Errorf("%s calls Status().Update; use Status().Patch", fset.Position(n.Pos()))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("found no controller source files; the walk is looking in the wrong place")
	}
}

// isStatusUpdate matches <x>.Status().Update(...).
func isStatusUpdate(n ast.Node) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return false
	}
	update, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || update.Sel.Name != "Update" {
		return false
	}
	inner, ok := update.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	status, ok := inner.Fun.(*ast.SelectorExpr)
	return ok && status.Sel.Name == "Status"
}

func TestIsStatusUpdate(t *testing.T) {
	for src, want := range map[string]bool{
		"r.Status().Update(ctx, cr)":                true,
		"r.Client.Status().Update(ctx, cr)":         true,
		"r.Status().Patch(ctx, cr, patch)":          false,
		"r.Update(ctx, cr)":                         false,
		"r.SubResource(\"status\").Get(ctx, cr, o)": false,
	} {
		expr, err := parser.ParseExpr(src)
		if err != nil {
			t.Fatal(err)
		}
		if got := isStatusUpdate(expr); got != want {
			t.Errorf("isStatusUpdate(%s) = %v, want %v", src, got, want)
		}
	}
}
