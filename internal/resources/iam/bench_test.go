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

package iam

import (
	"fmt"
	"net/url"
	"testing"
)

func benchGrants(n int) []grant {
	types := []string{resourceTypeSQS, resourceTypeSNS, resourceTypeDynamoDB, resourceTypeS3}
	grants := make([]grant, 0, n)
	for i := range n {
		grants = append(grants, grant{
			resourceType: types[i%len(types)],
			arn:          fmt.Sprintf("arn:aws:%s:us-east-1:111122223333:resource-%d", types[i%len(types)], i),
			readWrite:    i%2 == 0,
		})
	}
	return grants
}

// A pass that changes nothing builds the desired policy and compares it with
// the stored one.
func BenchmarkBuildPolicyDocument(b *testing.B) {
	grants := benchGrants(40)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := buildPolicyDocument(grants); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPolicyDocumentEquivalent(b *testing.B) {
	desired, err := buildPolicyDocument(benchGrants(40))
	if err != nil {
		b.Fatal(err)
	}
	stored := url.QueryEscape(desired)
	b.ReportAllocs()
	for b.Loop() {
		if !policyDocumentEquivalent(stored, desired) {
			b.Fatal("the documents should be equivalent")
		}
	}
}
