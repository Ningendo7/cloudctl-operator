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
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestResourceName(t *testing.T) {
	got := ResourceName("default", "checkout-service", "sqs", "orders", 80)
	wantPrefix := "default-checkout-service-orders-"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("ResourceName() = %q, want prefix %q", got, wantPrefix)
	}
	hash := strings.TrimPrefix(got, wantPrefix)
	if len(hash) != identityHashLen {
		t.Errorf("hash suffix length = %d, want %d", len(hash), identityHashLen)
	}
	if got2 := ResourceName("default", "checkout-service", "sqs", "orders", 80); got2 != got {
		t.Errorf("ResourceName() is not deterministic: %q vs %q", got, got2)
	}
}

func TestResourceName_TruncatesPrefixToFitMaxLen(t *testing.T) {
	ns, cr, resourceType, key := "a-very-long-namespace-name", "a-very-long-cr-name-too", "sqs", "orders"
	got := ResourceName(ns, cr, resourceType, key, 40)
	if len(got) > 40 {
		t.Errorf("len(ResourceName()) = %d, want <= 40", len(got))
	}
	if !strings.HasSuffix(got, identityHash(ns, cr, resourceType, key)) {
		t.Error("expected the hash suffix to survive truncation intact")
	}
}

func TestDerivedKey(t *testing.T) {
	got := DerivedKey("orders", "dlq")
	want := "orders#dlq"
	if got != want {
		t.Errorf("DerivedKey() = %q, want %q", got, want)
	}
}

func TestDerivedResourceName(t *testing.T) {
	got := DerivedResourceName("default", "checkout-service", "sqs", 80, "orders", "dlq")
	wantPrefix := "default-checkout-service-orders-dlq-"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("DerivedResourceName() = %q, want prefix %q", got, wantPrefix)
	}
	if got2 := DerivedResourceName("default", "checkout-service", "sqs", 80, "orders", "dlq"); got2 != got {
		t.Errorf("DerivedResourceName() is not deterministic: %q vs %q", got, got2)
	}
}

// TestDerivedResourceName_NeverCollidesWithAPlainResourceOfTheSameVisibleName
// guards against a derived name's visible prefix looking identical to a
// plain, user-declared resource's own name (e.g. a queue "orders" with a
// DLQ produces the same prefix text as a user-declared queue literally
// named "orders-dlq") — role must be hashed as its own tuple element, not
// string-joined into resourceKey first, or the two would hash identically
// and collide on the same real AWS name.
func TestDerivedResourceName_NeverCollidesWithAPlainResourceOfTheSameVisibleName(t *testing.T) {
	derived := DerivedResourceName("default", "checkout-service", "sqs", 80, "orders", "dlq")
	plain := ResourceName("default", "checkout-service", "sqs", "orders-dlq", 80)
	if derived == plain {
		t.Errorf("expected a derived name and a plain resource with the same visible text to never collide, both produced %q", derived)
	}
}

// Two different (namespace, crName) identities must never produce the
// same AWS resource name.
func TestResourceName_DoesNotCollideAcrossDifferentIdentities(t *testing.T) {
	cases := []struct {
		ns1, cr1 string
		ns2, cr2 string
		key      string
	}{
		{"team-a", "orders", "team", "a-orders", "q"},
		{"platform-eng", "checkout", "platform", "eng-checkout", "orders"},
		{"a", "b-c", "a-b", "c", "key"},
	}
	for _, c := range cases {
		name1 := ResourceName(c.ns1, c.cr1, "sqs", c.key, 255)
		name2 := ResourceName(c.ns2, c.cr2, "sqs", c.key, 255)
		if name1 == name2 {
			t.Errorf("(%q,%q) and (%q,%q) are different identities but both produced %q", c.ns1, c.cr1, c.ns2, c.cr2, name1)
		}
	}
}

// Two identities that differ only in resourceType must never produce the
// same AWS resource name.
func TestResourceName_DoesNotCollideAcrossDifferentResourceTypes(t *testing.T) {
	sqsName := ResourceName("default", "checkout-service", "sqs", "data", 255)
	s3Name := ResourceName("default", "checkout-service", "s3", "data", 255)
	if sqsName == s3Name {
		t.Errorf("sqs and s3 identities for the same (namespace, crName, key) both produced %q", sqsName)
	}
}

// dns1123Segment generates a lowercase-alphanumeric string with no
// internal hyphens.
func dns1123Segment(t *rapid.T, label string) string {
	return rapid.StringMatching(`^[a-z0-9]{1,6}$`).Draw(t, label)
}

// drawHyphenatedTriple generates a (namespace, crName, key) triple in
// which at least one field is guaranteed to contain an internal hyphen.
func drawHyphenatedTriple(t *rapid.T) (ns, cr, key string) {
	compound := func(label string) string {
		n := rapid.IntRange(1, 3).Draw(t, label+".segments")
		parts := make([]string, n)
		for i := range parts {
			parts[i] = dns1123Segment(t, fmt.Sprintf("%s.%d", label, i))
		}
		return strings.Join(parts, "-")
	}

	ns, cr, key = compound("ns"), compound("cr"), compound("key")
	if !strings.Contains(ns, "-") && !strings.Contains(cr, "-") && !strings.Contains(key, "-") {
		forced := dns1123Segment(t, "forced.a") + "-" + dns1123Segment(t, "forced.b")
		switch rapid.IntRange(0, 2).Draw(t, "forcedField") {
		case 0:
			ns = forced
		case 1:
			cr = forced
		default:
			key = forced
		}
	}
	return ns, cr, key
}

// hyphenIndexes returns the byte offset of every '-' in s.
func hyphenIndexes(s string) []int {
	var idx []int
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			idx = append(idx, i)
		}
	}
	return idx
}

// For any (namespace, crName, key) triple, re-cutting their joined string
// at any other pair of hyphens must never reproduce the same name.
func TestProperty_ResourceName_DifferentIdentitiesNeverCollide(t *testing.T) {
	alternatesChecked := 0
	rapid.Check(t, func(t *rapid.T) {
		ns1, cr1, key1 := drawHyphenatedTriple(t)
		full := ns1 + "-" + cr1 + "-" + key1
		name1 := ResourceName(ns1, cr1, "sqs", key1, 255)

		hyphens := hyphenIndexes(full)
		for i := range hyphens {
			for j := i + 1; j < len(hyphens); j++ {
				ns2 := full[:hyphens[i]]
				cr2 := full[hyphens[i]+1 : hyphens[j]]
				key2 := full[hyphens[j]+1:]
				if ns2 == "" || cr2 == "" || key2 == "" {
					continue
				}
				if ns2 == ns1 && cr2 == cr1 && key2 == key1 {
					continue // reconstructs the same identity, not a distinct case
				}
				alternatesChecked++
				if name2 := ResourceName(ns2, cr2, "sqs", key2, 255); name2 == name1 {
					t.Fatalf("distinct identities (%q,%q,%q) and (%q,%q,%q) both produced %q", ns1, cr1, key1, ns2, cr2, key2, name1)
				}
			}
		}
	})
	if alternatesChecked == 0 {
		t.Fatal("test setup broken: drawHyphenatedTriple should guarantee at least one alternate cut to check on every example, but none were found")
	}
}

func ExampleResourceName_distinctIdentitiesStayDistinct() {
	fmt.Println(ResourceName("team-a", "orders", "sqs", "q", 255) == ResourceName("team", "a-orders", "sqs", "q", 255))
	// Output: false
}

func TestTopicARN(t *testing.T) {
	got := TopicARN("us-east-1", "123456789012", "default-checkout-service-orders")
	want := "arn:aws:sns:us-east-1:123456789012:default-checkout-service-orders"
	if got != want {
		t.Errorf("TopicARN() = %q, want %q", got, want)
	}
}
