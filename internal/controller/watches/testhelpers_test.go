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

package watches

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// Shared fixtures for both producer_consumer_test.go and
// rds_subnetgroupgrant_test.go - kept in their own file rather than
// duplicated in each, the same role fake_test.go plays for the rds
// package's own several _test.go files.

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

func requestedNames(requests []reconcile.Request) []string {
	names := make([]string, 0, len(requests))
	for _, r := range requests {
		names = append(names, r.Namespace+"/"+r.Name)
	}
	return names
}

func containsRequest(requests []reconcile.Request, namespace, name string) bool {
	for _, r := range requests {
		if r.Namespace == namespace && r.Name == name {
			return true
		}
	}
	return false
}
