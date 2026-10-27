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

# Installs a CI or release tool at a pinned version and refuses it unless its
# SHA-256 matches the one written below, so a moved tag or a replaced upload
# cannot change what runs in a job that holds a registry login.
#
# Usage: hack/install-tool.sh <tool> [<dest dir>]   (default /usr/local/bin)
# Tools: helm kind kubebuilder promtool actionlint trivy
#
# To bump a tool, change its version and take the new checksums from the
# publisher's own checksum file (named in the comment on each tool); never
# compute them from a download of the new version.
set -euo pipefail

tool=${1:?usage: install-tool.sh <tool> [<dest dir>]}
dest=${2:-/usr/local/bin}

case "$(uname -s)" in
Linux) os=linux ;;
Darwin) os=darwin ;;
*)
	echo "install-tool: unsupported OS $(uname -s)" >&2
	exit 1
	;;
esac
case "$(uname -m)" in
x86_64 | amd64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
*)
	echo "install-tool: unsupported architecture $(uname -m)" >&2
	exit 1
	;;
esac
platform="${os}_${arch}"

# Sets url, sha256 and member (the file to take out of an archive; empty when
# the download is the binary itself).
select_tool() {
	member=""
	case "$tool" in
	helm)
		# <url>.sha256sum on get.helm.sh
		version=4.3.1
		url="https://get.helm.sh/helm-v${version}-${os}-${arch}.tar.gz"
		member="${os}-${arch}/helm"
		case "$platform" in
		linux_amd64) sha256=0403a88ae20913d11b10a2467b50087721fea7581736da6b9d3992f5357fe54b ;;
		linux_arm64) sha256=8cf8f66ecbc252ac8f7f06cca68aba88508ddfde3a6a34feb1678ad1694b4f88 ;;
		darwin_amd64) sha256=34bec4eba761ba5c16d1965ee6b8f5b0abe5993974a802274e2a4f0418893d89 ;;
		darwin_arm64) sha256=6b4e61c8cd27d99e5a801e63ff25dd5c70b36875c2cd659d3e9ce6b76fc23198 ;;
		esac
		;;
	kind)
		# <url>.sha256sum on kind.sigs.k8s.io
		version=0.33.0
		url="https://kind.sigs.k8s.io/dl/v${version}/kind-${os}-${arch}"
		case "$platform" in
		linux_amd64) sha256=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d ;;
		linux_arm64) sha256=20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2 ;;
		darwin_amd64) sha256=5a99f26f57246dc9319dd294803313197a0f34d33c525b3ea8b655db5916ece0 ;;
		darwin_arm64) sha256=0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f ;;
		esac
		;;
	kubebuilder)
		# checksums.txt, and the digest of each asset, on the GitHub release
		version=4.15.0
		url="https://github.com/kubernetes-sigs/kubebuilder/releases/download/v${version}/kubebuilder_${os}_${arch}"
		case "$platform" in
		linux_amd64) sha256=9632ba818c35e10d9664f19090972ee5d667bdffbf4b83c34e8374c5e846afa2 ;;
		linux_arm64) sha256=bd99723fc106502f64a3541b9c5b7079bc68961a76df4f465893e311b594d295 ;;
		darwin_amd64) sha256=c1e2fd86b7e2e392cd26b77a72bb9665511435ecdeefb16712820ba5da3e666b ;;
		darwin_arm64) sha256=8a8b2806c352f3e0749d315ecea91a352b2ce9321af927fca823a1604bd3530a ;;
		esac
		;;
	promtool)
		# sha256sums.txt on the GitHub release
		version=3.15.0
		url="https://github.com/prometheus/prometheus/releases/download/v${version}/prometheus-${version}.${os}-${arch}.tar.gz"
		member="prometheus-${version}.${os}-${arch}/promtool"
		case "$platform" in
		linux_amd64) sha256=2a542df32eac02ee17b9d844fb2aa1de00dafa5476579ba8a3ba862e9d572ea0 ;;
		linux_arm64) sha256=f1f90ec08e849d494ca66c611470afc50192f0355f1a61c33f2cbde02d067823 ;;
		darwin_amd64) sha256=2d79e744c2d7e505db936fbc898e05abc74fcb6e437c25befd26e9c9f00aa58b ;;
		darwin_arm64) sha256=920df4d17e78b3b0175af144eb318b0c74d1cf7b1d1251b326966f0e81977260 ;;
		esac
		;;
	actionlint)
		# actionlint_<version>_checksums.txt on the GitHub release
		version=1.7.12
		url="https://github.com/rhysd/actionlint/releases/download/v${version}/actionlint_${version}_${os}_${arch}.tar.gz"
		member="actionlint"
		case "$platform" in
		linux_amd64) sha256=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8 ;;
		linux_arm64) sha256=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6 ;;
		darwin_amd64) sha256=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644 ;;
		darwin_arm64) sha256=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f ;;
		esac
		;;
	trivy)
		# trivy_<version>_checksums.txt on the GitHub release
		version=0.74.0
		case "$platform" in
		linux_amd64) asset=Linux-64bit sha256=2ae6fe3ee734b7fdf11335663e18c75ea12dccc76062f09f164a3b0f8be4371a ;;
		linux_arm64) asset=Linux-ARM64 sha256=b94ce1976bbf3c15b514b605ee88be7c6d94a29be2302847ff01cb794d47aad5 ;;
		darwin_amd64) asset=macOS-64bit sha256=472816f6888dda689d075c30254d4210b4d1035acf365aa72332f584c2f60485 ;;
		darwin_arm64) asset=macOS-ARM64 sha256=1caada5e0e2091909357c7525d3aa76f4b660b13821bc143b190c7483e31cc11 ;;
		esac
		url="https://github.com/aquasecurity/trivy/releases/download/v${version}/trivy_${version}_${asset}.tar.gz"
		member="trivy"
		;;
	*)
		echo "install-tool: unknown tool $tool" >&2
		exit 1
		;;
	esac
}

sha256_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d' ' -f1
	else
		shasum -a 256 "$1" | cut -d' ' -f1
	fi
}

select_tool

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

echo "install-tool: $tool $version ($url)"
curl -fsSL --retry 3 --retry-delay 2 -o "$work/download" "$url"
got=$(sha256_of "$work/download")
if [[ $got != "$sha256" ]]; then
	echo "install-tool: checksum mismatch for $url" >&2
	echo "  want $sha256" >&2
	echo "  got  $got" >&2
	exit 1
fi

if [[ -n $member ]]; then
	tar -xzf "$work/download" -C "$work" "$member"
	binary="$work/$member"
else
	binary="$work/download"
fi
chmod +x "$binary"

mkdir -p "$dest"
if [[ -w $dest ]]; then
	install -m 0755 "$binary" "$dest/$tool"
else
	sudo install -m 0755 "$binary" "$dest/$tool"
fi
echo "install-tool: installed $dest/$tool"
