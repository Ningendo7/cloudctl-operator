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
	"context"
	"reflect"
	"testing"
	"time"
)

func TestMergeTags(t *testing.T) {
	cases := []struct {
		name     string
		existing map[string]string
		desired  map[string]string
		want     map[string]string
	}{
		{
			name:     "preserves existing tags not touched by desired",
			existing: map[string]string{"team": "checkout", "cost-center": "123"},
			desired:  map[string]string{"cloudctl.io/owner": "default/checkout-service"},
			want: map[string]string{
				"team":              "checkout",
				"cost-center":       "123",
				"cloudctl.io/owner": "default/checkout-service",
			},
		},
		{
			name:     "desired overwrites a conflicting key",
			existing: map[string]string{"cloudctl.io/owner": "stale/value"},
			desired:  map[string]string{"cloudctl.io/owner": "default/checkout-service"},
			want:     map[string]string{"cloudctl.io/owner": "default/checkout-service"},
		},
		{
			name:     "nil existing is treated as empty",
			existing: nil,
			desired:  map[string]string{"a": "b"},
			want:     map[string]string{"a": "b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MergeTags(tc.existing, tc.desired)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("MergeTags() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReleaseTags(t *testing.T) {
	now := time.Date(2026, 10, 21, 23, 59, 0, 0, time.FixedZone("x", -5*3600))

	got := ReleaseTags(context.Background(), "checkout", "checkout-service", now)

	if got[ReleasedFromTagKey] != "checkout/checkout-service" || got[ReleasedAtTagKey] != "2026-10-22" {
		t.Errorf("ReleaseTags() = %v, want the CR and the UTC date", got)
	}
	if ReleaseTags(WithReleaseTags(context.Background(), false), "checkout", "checkout-service", now) != nil {
		t.Error("turned off, ReleaseTags should return nothing")
	}
	if ReleaseTags(WithReleaseTags(context.Background(), true), "checkout", "checkout-service", now) == nil {
		t.Error("turned on explicitly, ReleaseTags should return the tags")
	}
}

func TestReleasedTagsAreNeverOwnership(t *testing.T) {
	tags := ReleaseTags(context.Background(), "ns", "app", time.Now())
	if IsOwnedBy(tags, "ns", "app", "uid") {
		t.Error("the released tags must not make a resource look owned")
	}
	if _, stale := IsStaleUID(tags, "ns", "app", "uid"); stale {
		t.Error("the released tags must not look like a previous incarnation")
	}
}

func TestWithoutReleaseTagsKeepsEverythingElse(t *testing.T) {
	in := map[string]string{"team": "payments", ReleasedFromTagKey: "ns/app", ReleasedAtTagKey: "2026-10-21"}

	out := WithoutReleaseTags(in)

	if len(out) != 1 || out["team"] != "payments" {
		t.Errorf("WithoutReleaseTags() = %v, want only the unrelated tag", out)
	}
	if len(in) != 3 {
		t.Error("the input map must not be modified")
	}
	if HasReleaseTags(out) || !HasReleaseTags(in) {
		t.Error("HasReleaseTags disagrees with WithoutReleaseTags")
	}
}
