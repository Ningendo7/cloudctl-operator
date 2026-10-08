# cloudctl-operator

[![Tests](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test.yml)
[![Lint](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/lint.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/lint.yml)
[![E2E Tests](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test-e2e.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test-e2e.yml)
[![Helm Chart Test](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/helm-chart-test.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/helm-chart-test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Kubernetes operator for everyday AWS application dependencies. Declare
*what your app needs* — a queue, a topic, a bucket, a table — and get back
a derived least-privilege IAM role, safe deletion semantics, and
connection details wired straight into your pods. No hand-authored IAM
policy, no raw Terraform-in-YAML.

```yaml
apiVersion: deps.cloudctl.io/v1alpha1
kind: AppDependencies
metadata:
  name: checkout-service
spec:
  sqs:
    resources:
      - name: orders
        dlq: true
        encryption:
          enabled: true
  s3:
    resources:
      - name: receipts
        backup:
          enabled: true
```

No ARNs, no IAM JSON, no bucket-naming logic. This reconciles to: the
queue and its dead-letter queue, a dedicated KMS key protecting both, a
versioned bucket with a backup lifecycle policy, an IAM role scoped to
exactly these resources, and a ConfigMap the app consumes via `envFrom`
for the queue URL and bucket name.

## Why

Teams provisioning AWS dependencies by hand, or via raw Terraform, tend to
end up with inconsistent, error-prone hand-authored IAM policy per app, and
operational hygiene — encryption, backups, cross-team access — that's
opt-in and frequently skipped. `AppDependencies` is a single CRD that
captures intent rather than raw provider config, and the controller
reconciles everything that intent implies: the resource itself, ownership
tagging, least-privilege IAM, and the IRSA wiring to use it — while still
reporting the concrete result in `status` rather than hiding it behind a
default.

## What it manages

- **SQS** — standard and FIFO queues, dead-letter queues, dedicated or shared KMS encryption.
- **SNS** — standard and FIFO topics, dedicated or shared KMS encryption.
- **DynamoDB** — tables with on-demand or provisioned billing, point-in-time recovery, dedicated or shared KMS encryption.
- **S3** — buckets with versioning/lifecycle-based backup, dedicated or shared KMS encryption.
- **KMS** — standalone keys for deliberate reuse across resources, with real access-level-aware grants.
- **CloudWatch alarms** — per-resource alarms, including notification to a topic owned by a different team's CR.
- **IAM** — never hand-authored. One role per CR, derived from exactly what it owns and what's been explicitly shared with it, attached via IRSA.

Every resource type shares the same safety model: ownership tagging (so
the operator never touches a resource it doesn't own), `adopt`-gated
claiming of pre-existing resources, `Retain`-by-default deletion with a
non-empty guard, and a persisted ownership ledger that survives a resource
being removed from spec. See [docs/architecture.md](docs/architecture.md)
for the full design and [docs/resources.md](docs/resources.md) for
field-by-field behavior.

## Cross-team resource sharing

A resource is owned by exactly one `AppDependencies` CR. Other apps
reference it by name, but access is never automatic — the owner has to
explicitly allow it, the same opt-in pattern as Gateway API's
`ReferenceGrant`:

```yaml
# owner CR
spec:
  sqs:
    resources:
      - name: orders
        sharedWith:
          - namespace: fulfillment
            name: fulfillment-service

# consumer CR
spec:
  sqs:
    consumes:
      - namespace: checkout
        name: checkout-service
        resourceName: orders
```

IAM, the connection ConfigMap, and status conditions all resolve
consistently from that one grant — nothing gets wired twice, and an
ungranted reference is reported, not silently dropped or silently allowed.

## Installation

### Prerequisites

- A Kubernetes cluster with an **IAM OIDC identity provider** configured
  (standard on EKS; required for any workload, including this operator
  itself, to assume an IAM role via IRSA).
- The operator's own pod needs an IAM role (attached via IRSA, the same
  mechanism it sets up for the workloads it manages) scoped to the AWS
  services and resources you intend to let it manage. See
  [docs/threat-model.md](docs/threat-model.md) for what this operator can
  and can't do with that access.
- **`--oidc-provider-arn` and `--oidc-provider-url` are required flags** —
  every IAM role this operator derives is trust-scoped to your cluster's
  OIDC provider, so reconciliation for any CR needing IAM fails without
  them.

### Helm (recommended)

Published as an OCI chart on every tagged release, alongside the manager
image — both on GitHub's own registry, no separate chart repo to add:

```bash
helm install cloudctl-operator oci://ghcr.io/ningendo7/charts/cloudctl-operator \
  --version <latest-release-version> \
  --namespace cloudctl-operator-system \
  --create-namespace \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=<operator-irsa-role-arn> \
  --set-string manager.args="{--leader-elect,--oidc-provider-arn=<oidc-provider-arn>,--oidc-provider-url=<oidc-provider-url>}"
```

See the chart's [values.yaml](charts/chart/values.yaml) for every
configurable field (replica count, resource limits, pod security context,
and more).

### Plain `kubectl` (kustomize-based bundle)

Every tagged release also publishes a consolidated install manifest as a
release asset — no Helm required:

```bash
kubectl apply -f https://github.com/Ningendo7/cloudctl-operator/releases/latest/download/install.yaml
```

This installs the CRDs, RBAC, and the manager Deployment in one step. You
still need to annotate the manager's ServiceAccount with your IRSA role
ARN and set `--oidc-provider-arn`/`--oidc-provider-url` on the Deployment
afterward (or fork the manifest to set them inline before applying).

Both install paths deploy the same image, built for `linux/amd64`,
`linux/arm64`, `linux/s390x`, and `linux/ppc64le`.

## Testing rigor

Three tiers, each catching a different class of bug: unit (fake AWS
clients, runs in milliseconds, every resource type), integration (real
API shapes against LocalStack, CI-gated on every PR, SQS/SNS/DynamoDB/S3/
IAM), and live (a real AWS account, opt-in, never in CI, same five). The
live tier exists specifically to catch what the other
two structurally can't: real, undocumented, or easy-to-mismodel AWS
behavior — wrong exception types, string-matched error codes with no
typed SDK equivalent, response formats that diverge from what a request
sent, eventual-consistency gaps. A fake only ever encodes this project's
own belief about an API; LocalStack only ever encodes its maintainers'.

Every destructive action re-verifies live ownership tags immediately
before acting, regardless of how recently that verification last ran —
cached trust can skip a read-level recheck, but never authorizes a
delete by itself. Adoption of a pre-existing resource is refused outright
unless it's explicitly requested and the resource carries no conflicting
ownership tag from a different CR.

Every PR is gated on all of: unit tests, integration tests, lint
(`golangci-lint`), a Helm chart end-to-end smoke test (real `kind`
cluster, real chart install), a full E2E lifecycle suite per resource
type, and a manifests/chart drift check. See
[docs/testing.md](docs/testing.md) for the complete breakdown.

## Documentation

- **[docs/architecture.md](docs/architecture.md)** — the design decisions:
  ownership and adoption, the trust window, deletion safety, naming,
  validation strategy.
- **[docs/resources.md](docs/resources.md)** — field-by-field behavior,
  known gaps, and a cross-resource-type lifecycle comparison for every
  resource type currently implemented.
- **[docs/testing.md](docs/testing.md)** — the test tiers: unit (always),
  integration (LocalStack, CI-gated), live (a real AWS account, opt-in,
  never in CI), and what's deliberately deferred.
- **[docs/aws-assumptions.md](docs/aws-assumptions.md)** — which AWS API
  behaviors this code depends on have been verified against a real account
  (and when) versus assumed from documentation.
- **[docs/threat-model.md](docs/threat-model.md)** — what a multi-tenant
  fleet of `AppDependencies` CRs can and can't cause this operator to do,
  given it runs with one set of broad AWS credentials.

Resources default to `deletionPolicy: Retain` — removing one from spec, or
deleting the CR, leaves the AWS resource in place rather than risking data
loss on a typo. See [architecture.md](docs/architecture.md) for the full
deletion-safety model.

## Status

Tagged releases are published from `main` once every check — unit,
integration, lint, Helm chart, E2E, and the manifests/chart drift check —
passes for that exact commit; see
[Releases](https://github.com/Ningendo7/cloudctl-operator/releases) for
the current version. SQS, SNS, DynamoDB, S3, and IAM are implemented and
covered by all three test tiers. KMS's dedicated-key path (the common
case — `encryption.enabled` on any other resource type) rides along
inside those same tiers; the standalone `kms.resources` type and
CloudWatch alarms are unit-tested only, not yet exercised against
LocalStack or real AWS.

Still ahead: RDS support (deferred deliberately — it's the first resource
touching VPC-level infrastructure and a Secret rather than a ConfigMap,
and gets its own design pass rather than being bolted on), SNS→SQS
subscription management (not yet expressible in the schema at all — you
create the subscription yourself today), and Prometheus/OpenTelemetry
instrumentation.

## License

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
