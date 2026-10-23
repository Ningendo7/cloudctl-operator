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
