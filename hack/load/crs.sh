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

# Prints AppDependencies CRs for the load run, ten to a namespace.
#
# Usage: hack/load/crs.sh <small|typical|heavy> <first> <last>
#   small    one queue
#   typical  a queue with a DLQ, a second queue and a table
#   heavy    ten resources, four of them encrypted with a dedicated key
#
# SQS, DynamoDB and dedicated KMS keys only. The pinned LocalStack can't
# create an SNS topic or a tagged S3 bucket the way the operator does, and
# doesn't emulate alarms, standalone keys or RDS faithfully.
set -euo pipefail

shape=${1:?usage: crs.sh <small|typical|heavy> <first> <last>}
first=${2:?usage: crs.sh <small|typical|heavy> <first> <last>}
last=${3:?usage: crs.sh <small|typical|heavy> <first> <last>}

for ((i = first; i <= last; i++)); do
	ns=$(printf 'load-%03d' $((i / 10)))
	name=$(printf 'app-%04d' "$i")
	if ((i % 10 == 0)) || ((i == first)); then
		printf -- '---\napiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n' "$ns"
	fi
	printf -- '---\napiVersion: deps.cloudctl.io/v1alpha1\nkind: AppDependencies\nmetadata:\n  name: %s\n  namespace: %s\nspec:\n' "$name" "$ns"
	case $shape in
	small)
		cat <<EOF
  sqs:
    resources:
      - name: orders
EOF
		;;
	typical)
		cat <<EOF
  sqs:
    resources:
      - name: orders
        dlq: true
      - name: notifications
  dynamodb:
    resources:
      - name: sessions
        partitionKey: pk
EOF
		;;
	heavy)
		cat <<EOF
  sqs:
    resources:
      - name: orders
        dlq: true
      - name: payments
        encryption: {enabled: true}
      - name: emails
      - name: refunds
        encryption: {enabled: true}
  dynamodb:
    resources:
      - name: sessions
        partitionKey: pk
        encryption: {enabled: true}
      - name: carts
        partitionKey: pk
      - name: profiles
        partitionKey: pk
      - name: ledger
        partitionKey: pk
        encryption: {enabled: true}
      - name: audit
        partitionKey: pk
      - name: events
        partitionKey: pk
EOF
		;;
	*)
		echo "crs.sh: unknown shape $shape" >&2
		exit 1
		;;
	esac
done
