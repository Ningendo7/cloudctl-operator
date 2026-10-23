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

package version_test

import (
	"runtime"
	"testing"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/version"
)

func set(t *testing.T, v, commit string) {
	t.Helper()
	oldV, oldC := version.Version, version.Commit
	version.Version, version.Commit = v, commit
	t.Cleanup(func() { version.Version, version.Commit = oldV, oldC })
}

func TestCurrent_UsesTheStampedVersionAndCommit(t *testing.T) {
	set(t, "v0.8.1", "abc1234")

	got := version.Current()

	if got.Version != "v0.8.1" || got.Commit != "abc1234" || got.GoVersion != runtime.Version() {
		t.Errorf("Current() = %+v", got)
	}
}

func TestCurrent_IsDevWhenNothingWasStamped(t *testing.T) {
	set(t, "", "")

	got := version.Current()

	// Under `go test` build info reports "(devel)" or nothing for the main module.
	if got.Version != "dev" {
		t.Errorf("Version = %q, want dev", got.Version)
	}
	if got.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
	}
}
