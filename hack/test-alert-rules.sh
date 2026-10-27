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

# Renders the chart's alert rules and runs the promtool unit tests in
# test/alerts against them, so the rules that ship are the rules that are
# tested. Needs helm, python3 with PyYAML, and promtool.
#
# Usage: hack/test-alert-rules.sh
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

helm template "$root/charts/chart" \
	--set prometheus.enabled=true --set prometheus.rules.enabled=true \
	--show-only templates/prometheus/controller-manager-alert-rules.yaml |
	python3 -c 'import sys, yaml; yaml.safe_dump(yaml.safe_load(sys.stdin)["spec"], sys.stdout)' \
		>"$work/rules.yaml"

promtool check rules "$work/rules.yaml"
cp "$root"/test/alerts/*_test.yaml "$work/"
promtool test rules "$work"/*_test.yaml
