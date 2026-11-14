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

# Measures the operator's memory and CPU under load, in a kind cluster with
# LocalStack. Never point it at a real cluster or real AWS.
#
# Usage: hack/load/run.sh <growth|small|heavy> <out dir>
#   growth  0, 100, 300 and 1,000 typical CRs, then unrelated ServiceAccounts,
#           then unrelated Secrets and ConfigMaps, then idle, then a restart
#           with 5, 20 and 50 workers
#   small   100 one-queue CRs
#   heavy   100 ten-resource CRs
#
# Writes to <out dir>: summary.tsv (one row per step), samples.tsv (every
# 5s), and per step the full metrics, a heap profile and its top entries.
set -euo pipefail

scenario=${1:?usage: run.sh <growth|small|heavy> <out dir>}
out=${2:?usage: run.sh <growth|small|heavy> <out dir>}
mkdir -p "$out"
out=$(cd "$out" && pwd)

cluster=${KIND_CLUSTER:-cloudctl-load}
img=cloudctl-operator:load
ns=cloudctl-operator-system
deploy=cloudctl-operator-controller-manager
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
pf_pid=""
sampler_pid=""
# Its own kubeconfig, holding only the kind cluster it creates, so no kubectl
# or make call here can reach whatever cluster the shell was pointed at.
KUBECONFIG=$(mktemp)
export KUBECONFIG

log() { echo "load-run: $(date -u +%H:%M:%S) $*"; }

cleanup() {
	[[ -n $sampler_pid ]] && kill "$sampler_pid" 2>/dev/null || true
	stop_port_forward
	rm -f "$KUBECONFIG"
}
trap cleanup EXIT

# --- cluster and operator ---------------------------------------------------

setup() {
	log "creating kind cluster $cluster"
	kind create cluster --name "$cluster" --wait 2m
	log "building and loading the manager image"
	make -C "$root" docker-build IMG="$img" >/dev/null
	kind load docker-image "$img" --name "$cluster"

	log "starting LocalStack"
	kubectl apply -f "$root/test/e2e/testdata/localstack.yaml"
	# e2e's 1Gi is sized for a handful of CRs, not a thousand.
	kubectl patch deployment localstack -n localstack-system --type=json -p '[
		{"op":"remove","path":"/spec/template/spec/containers/0/resources/limits"}
	]'
	kubectl rollout status deployment/localstack -n localstack-system --timeout=5m

	log "installing CRDs and the operator"
	make -C "$root" install deploy IMG="$img" >/dev/null
	kubectl set env "deployment/$deploy" -n "$ns" \
		AWS_ENDPOINT_URL=http://localstack.localstack-system.svc.cluster.local:4566 \
		AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
	# One replica without leader election, so every number is the pod doing
	# the work. No limits: an OOM kill would hide the number being measured.
	kubectl patch deployment "$deploy" -n "$ns" --type=json -p '[
		{"op":"replace","path":"/spec/replicas","value":1},
		{"op":"remove","path":"/spec/template/spec/containers/0/resources/limits"}
	]'
	set_workers 5
}

# set_workers replaces the manager's args and waits for the new pod. The
# annotation forces a new pod even when the args are unchanged.
set_workers() {
	local workers=$1
	kubectl patch deployment "$deploy" -n "$ns" --type=strategic -p "{\"spec\":{\"template\":{
		\"metadata\":{\"annotations\":{\"load-run/started\":\"$(date +%s)\"}},
		\"spec\":{\"containers\":[{\"name\":\"manager\",\"args\":[
			\"--leader-elect=false\",
			\"--health-probe-bind-address=:8081\",
			\"--metrics-bind-address=:8080\",
			\"--metrics-secure=false\",
			\"--pprof-bind-address=127.0.0.1:6060\",
			\"--max-concurrent-reconciles=$workers\",
			\"--oidc-provider-arn=arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/LOADRUN\",
			\"--oidc-provider-url=oidc.eks.us-east-1.amazonaws.com/id/LOADRUN\"
		]}]}
	}}}"
	kubectl rollout status "deployment/$deploy" -n "$ns" --timeout=5m
	port_forward
}

# live_pod is the running manager pod that is not shutting down; after a
# restart the old one still reports Running for a while.
live_pod() {
	kubectl get pods -n "$ns" -l control-plane=controller-manager -o json |
		jq -r '[.items[] | select(.metadata.deletionTimestamp == null and .status.phase == "Running")][0].metadata.name // empty'
}

# port_forward reaches metrics and pprof on the live pod, and reconnects when
# kubectl drops the connection, which it does under load. pprof listens on
# the pod's loopback only.
port_forward() {
	stop_port_forward
	(
		set +e
		while true; do
			pod=$(live_pod)
			[[ -n $pod ]] && kubectl port-forward -n "$ns" "pod/$pod" 18080:8080 16060:6060 >/dev/null 2>&1
			sleep 1
		done
	) &
	pf_pid=$!
	for _ in $(seq 1 60); do
		fetch_metrics >/dev/null && return 0
		sleep 1
	done
	echo "load-run: metrics never answered on the port-forward" >&2
	exit 1
}

stop_port_forward() {
	[[ -n $pf_pid ]] || return 0
	kill "$pf_pid" 2>/dev/null || true
	pkill -f "kubectl port-forward -n $ns pod/" 2>/dev/null || true
	pf_pid=""
}

# --- measuring ----------------------------------------------------------------

# fetch_metrics prints the operator's metrics, giving up after 10s rather
# than hanging on a stalled port-forward.
fetch_metrics() {
	curl -sf --max-time 10 localhost:18080/metrics
}

# sum prints the sum of every series of one metric name in the metrics on
# stdin.
sum() {
	awk -v m="$1" '$1 == m || index($1, m "{") == 1 { s += $2; n++ } END { if (n) printf "%.0f", s }'
}

# metric is sum over a fresh fetch.
metric() {
	fetch_metrics | sum "$1"
}

ready_count() {
	kubectl get appdependencies -A -o json |
		jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length'
}

# sampler records the operator's numbers every 5s for the whole run, so
# peaks between steps are not lost.
start_sampler() {
	printf 'time\trss_bytes\theap_inuse_bytes\tgoroutines\tcpu_seconds\tqueue_depth\taws_requests\n' >"$out/samples.tsv"
	(
		set +e
		while true; do
			m=$(fetch_metrics)
			printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -u +%s)" \
				"$(sum process_resident_memory_bytes <<<"$m")" \
				"$(sum go_memstats_heap_inuse_bytes <<<"$m")" \
				"$(sum go_goroutines <<<"$m")" \
				"$(awk '$1 == "process_cpu_seconds_total" { print $2 }' <<<"$m")" \
				"$(sum workqueue_depth <<<"$m")" \
				"$(sum cloudctl_aws_requests_total <<<"$m")" >>"$out/samples.tsv"
			sleep 5
		done
	) &
	sampler_pid=$!
}

# snapshot records one summary row, the full metrics and a heap profile,
# after a forced GC so the heap number is live data, not garbage.
snapshot() {
	local step=$1 crs=$2 converge_seconds=${3:-}
	log "snapshot $step"
	# Profile first: the forced GC it runs makes the metrics that follow
	# report live data.
	curl -sf --max-time 120 "localhost:16060/debug/pprof/heap?gc=1" >"$out/$step.heap.pb.gz" ||
		log "no heap profile for $step"
	go tool pprof -top -inuse_space -nodecount=40 "$out/$step.heap.pb.gz" >"$out/$step.heap-top.txt" 2>/dev/null || true
	local m="$out/$step.metrics.txt"
	fetch_metrics >"$m" || log "no metrics for $step"
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$step" "$crs" "$(ready_count)" \
		"$(sum process_resident_memory_bytes <"$m")" "$(sum go_memstats_heap_inuse_bytes <"$m")" \
		"$(sum go_goroutines <"$m")" "$(awk '$1 == "process_cpu_seconds_total" { print $2 }' "$m")" \
		"$(sum cloudctl_aws_requests_total <"$m")" "$converge_seconds" >>"$out/summary.tsv"
}

# --- load -------------------------------------------------------------------

apply_crs() {
	local shape=$1 first=$2 last=$3 file
	file=$(mktemp)
	# Namespaces for real first: a server-side dry run creates nothing, so the
	# CRs' dry run would find their namespaces missing.
	for ((n = first / 10; n <= last / 10; n++)); do
		printf -- '---\napiVersion: v1\nkind: Namespace\nmetadata:\n  name: load-%03d\n' "$n"
	done | kubectl apply -f - >/dev/null
	"$here/crs.sh" "$shape" "$first" "$last" >"$file"
	kubectl apply --dry-run=server -f "$file" >/dev/null
	kubectl apply -f "$file" >/dev/null
	rm -f "$file"
}

# converge waits until want CRs are Ready and prints how long it took, or
# fails the step at limit seconds instead of hanging the job.
converge() {
	local want=$1 limit=$2 start now ready
	start=$(date +%s)
	while true; do
		ready=$(ready_count || echo 0)
		now=$(date +%s)
		if ((ready >= want)); then
			echo $((now - start))
			return 0
		fi
		if ((now - start > limit)); then
			log "only $ready of $want CRs Ready after ${limit}s" >&2
			echo "timeout:$ready"
			return 0
		fi
		sleep 10
	done
}

# step adds CRs up to total, waits for them, lets in-flight work finish and
# takes a snapshot.
step() {
	local shape=$1 from=$2 total=$3 limit=$4 took
	log "adding $shape CRs $from..$((total - 1))"
	apply_crs "$shape" "$from" "$((total - 1))"
	took=$(converge "$total" "$limit")
	sleep 60
	snapshot "${shape}-${total}" "$total" "$took"
}

# unrelated creates count objects of kind in their own namespaces, without
# the operator's label: the cache must keep Secrets and ConfigMaps out, and
# is known to hold every ServiceAccount.
unrelated() {
	local kind=$1 count=$2 file payload
	file=$(mktemp)
	payload=$(head -c 768 /dev/zero | base64 -w0)
	for ((i = 0; i < count; i++)); do
		if ((i % 500 == 0)); then
			printf -- '---\napiVersion: v1\nkind: Namespace\nmetadata:\n  name: unrelated-%s-%02d\n' "${kind,,}" $((i / 500))
		fi
		case $kind in
		ServiceAccount)
			printf -- '---\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: sa-%05d\n  namespace: unrelated-serviceaccount-%02d\n' "$i" $((i / 500))
			;;
		Secret | ConfigMap)
			printf -- '---\napiVersion: v1\nkind: %s\nmetadata:\n  name: obj-%05d\n  namespace: unrelated-%s-%02d\ndata:\n  payload: %s\n' \
				"$kind" "$i" "${kind,,}" $((i / 500)) "$payload"
			;;
		esac
	done >"$file"
	kubectl apply -f "$file" >/dev/null
	rm -f "$file"
}

# restart_drain restarts the operator with workers and times how long it
# takes to work through every CR again, which it does on every start.
restart_drain() {
	local workers=$1 crs=$2 start took m done_count depth
	log "restarting with $workers workers"
	# Timed from before the restart: startup and the cache sync are part of
	# what a restarted operator costs.
	start=$(date +%s)
	set_workers "$workers"
	while true; do
		m=$(fetch_metrics || true)
		done_count=$(awk '$1 ~ /^controller_runtime_reconcile_total\{controller="appdependencies"/ { s += $2 } END { printf "%.0f", s }' <<<"$m")
		depth=$(sum workqueue_depth <<<"$m")
		# Done once the new pod has reconciled as many times as there are CRs,
		# not when the queue empties: at 1,000 CRs drift requeues kept it from
		# ever emptying.
		if [[ -n $m ]] && ((done_count >= crs)); then
			took=$(($(date +%s) - start))
			log "$done_count reconciles after ${took}s, ${depth:-0} still queued"
			break
		fi
		if (($(date +%s) - start > 3600)); then
			took="timeout:$done_count"
			break
		fi
		sleep 5
	done
	sleep 30
	snapshot "restart-${workers}-workers" "$crs" "$took"
}

# --- scenarios ----------------------------------------------------------------

setup
start_sampler
printf 'step\tcrs\tready\trss_bytes\theap_inuse_bytes\tgoroutines\tcpu_seconds\taws_requests\tconverge_seconds\n' >"$out/summary.tsv"
sleep 60
snapshot baseline 0

case $scenario in
growth)
	step typical 0 100 1800
	step typical 100 300 3600
	step typical 300 1000 7200
	log "adding 5,000 unrelated ServiceAccounts"
	unrelated ServiceAccount 5000
	sleep 120
	snapshot plus-5000-serviceaccounts 1000
	log "adding 5,000 unrelated Secrets and 5,000 ConfigMaps"
	unrelated Secret 5000
	unrelated ConfigMap 5000
	sleep 120
	snapshot plus-10000-secrets-configmaps 1000
	log "idle for 15 minutes"
	sleep 900
	snapshot idle-15m 1000
	for workers in 5 20 50; do
		restart_drain "$workers" 1000
	done
	;;
small)
	step small 0 100 1800
	;;
heavy)
	step heavy 0 100 3600
	;;
*)
	echo "run.sh: unknown scenario $scenario" >&2
	exit 1
	;;
esac

log "done; results in $out"
column -t -s $'\t' "$out/summary.tsv"
