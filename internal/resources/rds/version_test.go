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

package rds

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func TestParseVersion(t *testing.T) {
	for in, ok := range map[string]bool{"16": true, "16.4": true, "8.0.35": true, "": false, "16.x": false, "-1": false, "16..4": false} {
		if _, got := parseVersion(in); got != ok {
			t.Errorf("parseVersion(%q) ok = %v, want %v", in, got, ok)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	v := func(s string) []int { out, _ := parseVersion(s); return out }
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"16.4", "16.4", 0},
		{"16", "16.0", 0},
		{"16.10", "16.4", 1},
		{"16.4", "16.10", -1},
		{"9.6.24", "16.4", -1},
		{"10.11.6", "10.6.16", 1},
		{"8.0.35", "8.0", 1},
	} {
		if got := compareVersions(v(tc.a), v(tc.b)); got != tc.want {
			t.Errorf("compareVersions(%s, %s) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func fakeUpgradeTargets(client *fakeRDS, targets ...types.UpgradeTarget) {
	client.upgradeTargets = targets
}

func TestEngineVersionUpgrade(t *testing.T) {
	minor := types.UpgradeTarget{EngineVersion: aws.String("16.6"), IsMajorVersionUpgrade: aws.Bool(false)}
	major := types.UpgradeTarget{EngineVersion: aws.String("17.2"), IsMajorVersionUpgrade: aws.Bool(true)}

	tests := []struct {
		name              string
		current, desired  string
		wantNeeded, wantM bool
		wantErr           bool
		wantDescribeCalls int
	}{
		{name: "same version", current: "16.4", desired: "16.4"},
		{name: "major-only spec satisfied by any minor", current: "16.4", desired: "16"},
		{name: "instance ahead of spec after AWS auto minor upgrade", current: "16.6", desired: "16.4"},
		{name: "minor upgrade", current: "16.4", desired: "16.6", wantNeeded: true, wantDescribeCalls: 1},
		{name: "major upgrade", current: "16.4", desired: "17.2", wantNeeded: true, wantM: true, wantDescribeCalls: 1},
		{name: "major-only upgrade spec", current: "16.4", desired: "17", wantNeeded: true, wantM: true, wantDescribeCalls: 1},
		{name: "not a valid target", current: "16.4", desired: "18.1", wantErr: true, wantDescribeCalls: 1},
		{name: "unparsable spec", current: "16.4", desired: "sixteen", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeRDS()
			fakeUpgradeTargets(client, minor, major)
			needed, isMajor, err := engineVersionUpgrade(context.Background(), client, "postgres", tc.current, tc.desired)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if needed != tc.wantNeeded || isMajor != tc.wantM {
				t.Errorf("needed=%v major=%v, want needed=%v major=%v", needed, isMajor, tc.wantNeeded, tc.wantM)
			}
			if client.describeEngineVersionsCalls != tc.wantDescribeCalls {
				t.Errorf("DescribeDBEngineVersions calls = %d, want %d", client.describeEngineVersionsCalls, tc.wantDescribeCalls)
			}
		})
	}
}
