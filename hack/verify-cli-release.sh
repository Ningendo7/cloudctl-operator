#!/usr/bin/env bash
# Copyright 2026 Patrick Ajah.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks what `make cli-release` built before anything is signed or
# published: every archive is there and listed in checksums.txt, holds exactly
# the two binaries and the licence, and the binaries for this machine run,
# report the version they were built as and use the name they were started as.
#
# Usage: hack/verify-cli-release.sh <dist dir> <version>
set -euo pipefail

dir=${1:?usage: verify-cli-release.sh <dist dir> <version>}
version=${2:?usage: verify-cli-release.sh <dist dir> <version>}
platforms=(linux_amd64 linux_arm64 darwin_amd64 darwin_arm64)

fail() {
	echo "verify-cli-release: $*" >&2
	exit 1
}

cd "$dir"
[[ -f checksums.txt ]] || fail "checksums.txt is missing"
[[ $(wc -l <checksums.txt) -eq ${#platforms[@]} ]] || fail "checksums.txt should list exactly the ${#platforms[@]} archives"
for platform in "${platforms[@]}"; do
	archive="appdeps_${version}_${platform}.tar.gz"
	[[ -f $archive ]] || fail "$archive is missing"
	grep -q "  ${archive}\$" checksums.txt || fail "$archive is not in checksums.txt"
done
sha256sum --check --strict --quiet checksums.txt || fail "a checksum does not match its archive"
for platform in "${platforms[@]}"; do
	archive="appdeps_${version}_${platform}.tar.gz"
	contents=$(tar -tzf "$archive" | sort | tr '\n' ' ')
	[[ $contents == "LICENSE appdeps kubectl-appdeps " ]] || fail "$archive should hold LICENSE, appdeps and kubectl-appdeps, holds: $contents"
done

host="$(go env GOOS)_$(go env GOARCH)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
tar -xzf "appdeps_${version}_${host}.tar.gz" -C "$work"
for bin in kubectl-appdeps appdeps; do
	[[ -x $work/$bin ]] || fail "$bin is not executable"
done

# No cluster here on purpose: version must work without one.
out=$(KUBECONFIG=/nonexistent "$work/appdeps" version) || fail "appdeps version failed without a cluster: $out"
[[ $out == *"Client: ${version} ("* ]] || fail "appdeps version does not report ${version}: $out"

"$work/kubectl-appdeps" --help | grep -q "kubectl appdeps status" || fail "kubectl-appdeps --help does not use the plugin name"
standalone=$("$work/appdeps" --help)
grep -q "  appdeps status" <<<"$standalone" || fail "appdeps --help does not use its own name"
grep -q "kubectl appdeps" <<<"$standalone" && fail "appdeps --help shows the plugin name"

info=$(go version -m "$work/appdeps")
grep -q "CGO_ENABLED=0" <<<"$info" || fail "appdeps was built with cgo; it must be a static binary"
grep -q -- "-trimpath=true" <<<"$info" || fail "appdeps was built without -trimpath"

echo "verify-cli-release: ${version} is complete and consistent"
