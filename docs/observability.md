# Observability

The operator exposes Prometheus metrics and writes structured logs. It does
not collect or store either: that is the platform's job, the same as for any
other workload (see [Where the data goes](#where-the-data-goes)). Everything
below is opt-in except the `/metrics` endpoint itself.

## Metrics

Served on the manager's `/metrics` endpoint (HTTPS with token authentication
by default, see `metrics.secure`), alongside what controller-runtime already
provides: `controller_runtime_reconcile_*`, `workqueue_*`,
`rest_client_requests_total`, `leader_election_master_status`, and the Go and
process metrics.

### Counters and histograms

Their labels come from small fixed sets, never from CR or resource names, so
the number of series does not grow with the number of CRs.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `cloudctl_aws_requests_total` | counter | `service`, `result` | AWS API operations. `result` is `success`, `throttled`, `permission_denied`, `not_found` or `error`. A call the SDK retried counts once. |
| `cloudctl_aws_request_duration_seconds` | histogram | `service` | Wall time of an operation, including SDK retries and rate-limiter waits. |
| `cloudctl_aws_rate_limit_wait_seconds_total` | counter | `service` | Time spent waiting on the operator's own rate limiter. Its `rate()` is the average number of calls queued behind the limiter; a rising value on one service is the signal to raise that service's `AWS_<SERVICE>_RATE_LIMIT_QPS`. |
| `cloudctl_aws_destructive_requests_total` | counter | `service`, `operation`, `result` | The AWS operations that remove or disable something (`DeleteQueue`, `RevokeSecurityGroupIngress`, `Unsubscribe`, ...), a subset of `cloudctl_aws_requests_total`. Compare it with what you expected to be deleted. |
| `cloudctl_section_reconcile_total` | counter | `section`, `result` | One per section per reconcile. `section` is `sqs`, `sns`, `dynamodb`, `s3`, `rds`, `kms`, `alarms` or `iam`. `result` is `ready`, `transient` (waiting, retried), `permission_denied`, `refused` (the operator declined to act and a human has to change something, for example `AdoptionConflict` or `InstanceMissing`) or `error`. |
| `cloudctl_section_reconcile_duration_seconds` | histogram | `section` | Wall time of one section. |
| `cloudctl_events_total` | counter | `type`, `reason` | Every Kubernetes event the operator emits. Kubernetes keeps events for about an hour; this keeps the count. Reasons such as `QueueMissing`, `InstanceMissing`, `KeyMissing` and `InstanceAdopted` make it both an adoption audit trail and the signal that someone is changing things outside the operator. |
| `cloudctl_aws_clients_ready` | gauge | none | 1 once the AWS clients are initialised, 0 while the manager runs without reaching AWS. |

### State gauges

Computed from the manager's cache on every scrape, so they always reflect
current state and a deleted CR's series disappear with it.

| Metric | Labels | Meaning |
|---|---|---|
| `cloudctl_managed_resources` | `type`, `state` | Ledger entries by resource type and state (`Creating`, `TagPending`, `Verified`, `NeedsVerification`, `Relinquished`). |
| `cloudctl_pending_deletion_resources` | `type`, `stuck` | Resources removed from spec and awaiting deletion. `stuck="true"` once past that type's grace period: one hour for RDS, seven days for the rest. |
| `cloudctl_rds_instances` | `status` | RDS instances by the status AWS last reported (`available`, `modifying`, ...). |
| `cloudctl_app_ready` | `namespace`, `name` | 1 if the CR's aggregate `Ready` condition is True. One series per CR. |
| `cloudctl_app_section_ready` | `namespace`, `name`, `section` | The same per section, so you can see which section a CR is stuck on. |
| `cloudctl_app_generation_lag` | `namespace`, `name` | How many spec generations the CR's `Ready` condition is behind. 0 once the operator has caught up with the latest spec; above 0 for a while means a change has not been applied. |
| `cloudctl_resource_verified_age_seconds` | `type` | Age of the oldest ownership re-verification among a type's ledger entries. It grows when the operator stops re-checking something; it should stay near the drift interval. |

### Querying across replicas

The manager runs two replicas and only the leader reconciles. Both replicas
export the state gauges, so aggregate those with `max`; only the leader makes
AWS calls, so `sum` the counters. Counters reset when a pod restarts, which
`rate()` and `increase()` already handle. Query a raw counter value from a
single pod and you will be wrong after every failover.

## Chart switches

| Value | Default | What it adds |
|---|---|---|
| `prometheus.enabled` | `false` | A `ServiceMonitor`, for a prometheus-operator based Prometheus. |
| `prometheus.rules.enabled` | `false` | A `PrometheusRule` with the alerts below. Separate from `prometheus.enabled`: scraping is harmless, alerts that can page someone are a decision. |
| `prometheus.rules.labels` | `{}` | Extra labels on the `PrometheusRule`. kube-prometheus-stack only loads rules whose labels match its `ruleSelector`, usually `release: <its release name>`. |
| `grafana.dashboard.enabled` | `false` | A dashboard `ConfigMap` labeled `grafana_dashboard: "1"`, which kube-prometheus-stack's Grafana sidecar picks up by default. |

With `metrics.secure: true` (the default) the scraper needs a bearer token
whose ServiceAccount is bound to the chart's `metrics-reader` ClusterRole.

## Alerts

| Alert | Severity | Fires when |
|---|---|---|
| `CloudctlOperatorAbsent` | critical | No operator metrics have been scraped for 10 minutes. |
| `CloudctlAWSClientsNotReady` | critical | No replica has reached AWS for 10 minutes. |
| `CloudctlRDSInstanceNeedsManualDeletion` | critical | An RDS instance removed from spec with `deletionPolicy: Delete` is past its grace period. The operator never deletes a database, and billing continues. |
| `CloudctlDataResourceMissing` | critical | An RDS instance or KMS key the operator manages was deleted outside it. Neither is ever recreated. |
| `CloudctlKMSKeyUnusable` | critical | A KMS key the operator manages is disabled or unavailable, so every resource encrypted with it fails. The operator never re-enables it. |
| `CloudctlAppNotReady` | warning | A CR has not been Ready for 30 minutes. |
| `CloudctlAWSPermissionDenied` | warning | AWS has denied the operator calls to a service for 15 minutes. The actions it needs are listed in [threat-model.md](threat-model.md#aws-actions-the-managers-role-needs). |
| `CloudctlAWSThrottling` | warning | More than 5% of a service's calls have been throttled for 15 minutes. |
| `CloudctlResourceStuckPendingDeletion` | warning | A non-RDS resource removed from spec is still not empty after its grace period. |
| `CloudctlOutOfBandDeletion` | info | A queue, topic or bucket was deleted outside the operator and recreated, empty. |
| `CloudctlWorkqueueBacklog` | warning | More than 50 CRs have been waiting to reconcile for 15 minutes. |

The template, [controller-manager-alert-rules.yaml](../charts/chart/templates/prometheus/controller-manager-alert-rules.yaml), is the authoritative list.

Prometheus evaluates these rules and Alertmanager routes them, so route on the `severity` label: `critical` pages someone, `warning` goes to a channel, `info` is for a dashboard. Nothing is deleted when `CloudctlResourceStuckPendingDeletion` or `CloudctlRDSInstanceNeedsManualDeletion` fires: the resource stays in AWS and keeps costing until a person deals with it. The chart renders the rules as a `PrometheusRule`, which needs prometheus-operator's CRDs; without them, write the same expressions in your own rule files, since they only use the metrics above.

## Dashboard

[charts/chart/dashboards/cloudctl-operator.json](../charts/chart/dashboards/cloudctl-operator.json)
covers the same ground as the alerts: which CRs and sections are not ready,
resources by state and pending deletion, RDS statuses, warning events, AWS
calls by result, latency and rate-limiter waits, section results and
durations, reconcile errors and work-queue depth. It has a data-source
variable, so it works with any Prometheus-compatible source.

## Tracing

Off by default. Set the standard `OTEL_EXPORTER_OTLP_ENDPOINT` on the manager (the chart's `manager.extraEnv`) and it exports spans over OTLP/gRPC. Everything else, such as `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_TRACES_SAMPLER` and `OTEL_RESOURCE_ATTRIBUTES`, is read by the OpenTelemetry SDK from its own standard variables, so pointing it at Tempo, a vendor agent or an OTel Collector is configuration only. With no endpoint set nothing is initialised and every span is a free no-op.

```yaml
manager:
  extraEnv:
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: http://otel-collector.observability.svc.cluster.local:4317
```

One reconcile is one trace:

| Span | What it is |
|---|---|
| `Reconcile` | The root, with `cloudctl.namespace` and `cloudctl.name`. Names belong on spans even though they stay off metrics. |
| `ensure finalizer`, `patch status` | The two writes every reconcile makes to the CR. |
| `reconcile <section>` and `finalize <section>` | One per section (`kms`, `sqs`, `sns`, `dynamodb`, `s3`, `rds`, `alarms`, `iam`, plus `connectioninfo`), with `cloudctl.section`. |
| `SQS.GetQueueUrl`, `S3.PutBucketTagging`, ... | Every AWS call, nested under the section that made it. Retries and rate-limiter waits show up as the span's duration. |
| `kube-apiserver GET/PATCH/...` | Every request to the Kubernetes API server, nested under whatever made it: the finalizer update, the status patch, the ConfigMap and Secret applies. Reads served from the informer cache never reach the API server, so they don't appear, and watches are left out because they stay open for minutes. |

Errors are recorded on the span that failed. When tracing is on, each log line
written during a reconcile carries `trace_id` and `span_id`, so a slow or
failed reconcile's logs are found from its trace, and the other way round.

## Where the data goes

| Signal | The operator | The platform |
|---|---|---|
| Metrics | Serves `/metrics`; optional `ServiceMonitor` | A scraper and storage. Prometheus on its own suits short retention; for durability and HA, `remote_write` to Mimir, Thanos or VictoriaMetrics, or a vendor agent. |
| Logs | Structured JSON on stdout | A node agent such as Grafana Alloy or Fluent Bit shipping to Loki, Elasticsearch or a vendor. Container logs on a node disappear when the pod is replaced or the node is lost, so without one the logs from just before a crash are gone. |
| Traces | Exports OTLP only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set | An OTel Collector, Tempo or a vendor agent to receive it |
| Events | Kubernetes events on each `AppDependencies` | Kept by Kubernetes for about an hour. `cloudctl_events_total` keeps the counts; an event exporter keeps the text. |

The operator keeps no history of its own. CR status and the ownership ledger
hold current state only, so anything you want to look back on has to come
from the platform's storage.

Logs carry the CR's namespace and name, and metrics deliberately do not, so
per-CR detail beyond the two state gauges lives in logs.
