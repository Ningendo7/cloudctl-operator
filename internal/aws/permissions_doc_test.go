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

package aws

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// clientInterfaces names, per service row of docs/threat-model.md, the
// interfaces whose methods are the AWS operations the operator can call, and
// the directory that holds each.
var clientInterfaces = map[string][]struct{ dir, name string }{
	"SQS":                       {{".", "SQSClient"}},
	"SNS":                       {{".", "SNSClient"}},
	"S3":                        {{"../resources/s3", "s3API"}},
	"DynamoDB":                  {{"../resources/dynamodb", "dynamodbAPI"}},
	"KMS":                       {{".", "KMSClient"}},
	"IAM":                       {{".", "IAMClient"}},
	"CloudWatch":                {{".", "CloudWatchClient"}},
	"RDS":                       {{".", "RDSClient"}},
	"EC2 (RDS security groups)": {{".", "EC2Client"}},
	"Secrets Manager (RDS)":     {{".", "SecretsManagerClient"}},
}

// iamActionFor maps an API operation to the IAM actions that authorize it. Most
// are the same word; the S3 ones are not.
var iamActionFor = map[string][]string{
	"HeadBucket":                      {"ListBucket"},
	"GetPublicAccessBlock":            {"GetBucketPublicAccessBlock"},
	"GetBucketLifecycleConfiguration": {"GetLifecycleConfiguration"},
	"PutBucketLifecycleConfiguration": {"PutLifecycleConfiguration"},
	"DeleteBucketLifecycle":           {"PutLifecycleConfiguration"},
	"GetBucketEncryption":             {"GetEncryptionConfiguration"},
	"PutBucketEncryption":             {"PutEncryptionConfiguration"},
	"ListObjectVersions":              {"ListBucketVersions"},
	"DeleteObjects":                   {"DeleteObject", "DeleteObjectVersion"},
	"ListMultipartUploads":            {"ListBucketMultipartUploads"},
}

// extraActions are listed in the table for a reason no interface method shows:
// AWS authorizes a tag on create, or a service acting on the caller's behalf,
// with a separate action. Each is marked "docs, verify" in the table.
var extraActions = map[string][]string{
	"S3":         {"TagResource"},
	"KMS":        {"CreateGrant"},
	"CloudWatch": {"TagResource"},
}

func interfaceMethods(t *testing.T, dir, name string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || ts.Name.Name != name {
					continue
				}
				iface, ok := ts.Type.(*ast.InterfaceType)
				if !ok {
					continue
				}
				var methods []string
				for _, m := range iface.Methods.List {
					for _, n := range m.Names {
						methods = append(methods, n.Name)
					}
				}
				return methods
			}
		}
	}
	t.Fatalf("found no interface %s in %s", name, dir)
	return nil
}

func documentedActions(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(filepath.Join("..", "..", "docs", "threat-model.md")))
	if err != nil {
		t.Fatal(err)
	}
	section := string(raw)
	if i := strings.Index(section, "### AWS actions the manager's role needs"); i >= 0 {
		section = section[i:]
	} else {
		t.Fatal("docs/threat-model.md has no AWS actions section")
	}
	parenthetical := regexp.MustCompile(`\([^)]*\)`)
	backticked := regexp.MustCompile("`([A-Za-z0-9]+)`")
	rows := map[string][]string{}
	for line := range strings.SplitSeq(section, "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 4 || strings.HasPrefix(strings.TrimSpace(cells[1]), "---") {
			continue
		}
		service := strings.TrimSpace(cells[1])
		// Remarks in brackets may name an action the operator does not need.
		for _, m := range backticked.FindAllStringSubmatch(parenthetical.ReplaceAllString(cells[2], ""), -1) {
			rows[service] = append(rows[service], m[1])
		}
	}
	return rows
}

// The permissions table is what an installer builds a least-privilege policy
// from, so an operation the code can call must be in it, under the name IAM
// expects, and nothing may be listed that the code never calls.
func TestPermissionsTableMatchesTheClientInterfaces(t *testing.T) {
	documented := documentedActions(t)

	for service, interfaces := range clientInterfaces {
		var want []string
		for _, iface := range interfaces {
			for _, op := range interfaceMethods(t, iface.dir, iface.name) {
				if actions, ok := iamActionFor[op]; ok {
					want = append(want, actions...)
				} else {
					want = append(want, op)
				}
			}
		}
		want = append(want, extraActions[service]...)
		slices.Sort(want)
		want = slices.Compact(want)

		got := slices.Clone(documented[service])
		slices.Sort(got)
		got = slices.Compact(got)
		if len(got) == 0 {
			t.Errorf("docs/threat-model.md has no row for %q", service)
			continue
		}
		for _, action := range want {
			if !slices.Contains(got, action) {
				t.Errorf("%s: the code can call something that needs %q, which the permissions table does not list", service, action)
			}
		}
		for _, action := range got {
			if !slices.Contains(want, action) {
				t.Errorf("%s: the permissions table lists %q, which nothing in the code needs", service, action)
			}
		}
	}
}
