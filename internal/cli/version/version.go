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

// Package version reports which build of the CLI this is.
package version

import (
	"runtime"
	"runtime/debug"
)

// Version and Commit are set at release time with -ldflags. A build from
// `go install` leaves them empty and Current falls back to the build info.
var (
	Version string
	Commit  string
)

// Info describes the running binary.
type Info struct {
	Version   string
	Commit    string
	GoVersion string
}

// Current returns the stamped version, else the module version from build
// info, else "dev".
func Current() Info {
	info := Info{Version: Version, Commit: Commit, GoVersion: runtime.Version()}
	if build, ok := debug.ReadBuildInfo(); ok {
		if info.Version == "" && build.Main.Version != "(devel)" {
			info.Version = build.Main.Version
		}
		if info.Commit == "" {
			for _, setting := range build.Settings {
				if setting.Key == "vcs.revision" {
					info.Commit = setting.Value
				}
			}
		}
	}
	if info.Version == "" {
		info.Version = "dev"
	}
	return info
}
