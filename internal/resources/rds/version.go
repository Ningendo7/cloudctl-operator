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
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

// parseVersion splits a dotted numeric engine version ("16.4", "8.0.35").
func parseVersion(v string) ([]int, bool) {
	if v == "" {
		return nil, false
	}
	parts := strings.Split(v, ".")
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

// compareVersions orders a against b, treating missing components as zero.
func compareVersions(a, b []int) int {
	for i := 0; i < max(len(a), len(b)); i++ {
		var x, y int
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// hasVersionPrefix reports whether prefix is a leading run of v's components.
func hasVersionPrefix(v, prefix []int) bool {
	if len(prefix) > len(v) {
		return false
	}
	for i, p := range prefix {
		if v[i] != p {
			return false
		}
	}
	return true
}

// engineVersionUpgrade decides whether current already meets desired. The
// spec version is a minimum, not an exact pin: AWS applies minor upgrades
// on its own, and a downgrade is never possible, so an instance at or past
// desired needs nothing. Otherwise it asks AWS which upgrade targets are
// valid from current, and whether reaching desired is a major upgrade.
func engineVersionUpgrade(ctx context.Context, awsClient rdsAPI, engine, current, desired string) (needed, major bool, err error) {
	if current == "" {
		return false, false, nil
	}
	cur, ok := parseVersion(current)
	if !ok {
		return false, false, fmt.Errorf("cannot compare current engine version %q", current)
	}
	want, ok := parseVersion(desired)
	if !ok {
		return false, false, fmt.Errorf("engineVersion %q must be dotted numbers such as 16 or 16.4", desired)
	}
	if compareVersions(cur, want) >= 0 {
		return false, false, nil
	}

	out, err := awsClient.DescribeDBEngineVersions(ctx, &rds.DescribeDBEngineVersionsInput{
		Engine:        aws.String(engine),
		EngineVersion: aws.String(current),
	})
	if err != nil {
		return false, false, wrapAWSError(err, "listing valid engine upgrade targets")
	}
	matched := false
	for _, v := range out.DBEngineVersions {
		for _, target := range v.ValidUpgradeTarget {
			tv, ok := parseVersion(aws.ToString(target.EngineVersion))
			if !ok || !hasVersionPrefix(tv, want) {
				continue
			}
			matched = true
			if aws.ToBool(target.IsMajorVersionUpgrade) {
				major = true
			}
		}
	}
	if !matched {
		return false, false, fmt.Errorf("engineVersion %q is not a valid upgrade target from %s %s", desired, engine, current)
	}
	return true, major, nil
}
