# cloudctl-operator

[![Tests](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test.yml)
[![Lint](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/lint.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/lint.yml)
[![E2E Tests](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test-e2e.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/test-e2e.yml)
[![Helm Chart Test](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/helm-chart-test.yml/badge.svg)](https://github.com/Ningendo7/cloudctl-operator/actions/workflows/helm-chart-test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Kubernetes operator that keeps your app's AWS dependencies in the state you
declared. Declare *what your app needs* — a queue, a topic, a bucket, a table,
a database — and a reconcile loop creates it, derives a least-privilege IAM
role, wires the connection details into your pods, and keeps correcting drift
instead of provisioning once. No hand-authored IAM policy, no raw
Terraform-in-YAML, and the concrete result is reported in `status`.

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

This reconciles to the queue and its dead-letter queue, a dedicated KMS key
protecting both, a versioned bucket with a backup lifecycle policy, an IAM
role scoped to exactly these resources, and a ConfigMap the app consumes via
`envFrom` for the queue URL and bucket name.

## What it manages

- **SQS** — standard and FIFO queues, dead-letter queues, dedicated or shared KMS encryption, and subscribing to a shared SNS topic.
- **SNS** — standard and FIFO topics, dedicated or shared KMS encryption.
- **DynamoDB** — tables with on-demand or provisioned billing, point-in-time recovery, dedicated or shared KMS encryption.
- **S3** — buckets with versioning/lifecycle-based backup, dedicated or shared KMS encryption.
- **RDS** — Postgres, MySQL and MariaDB instances in a subnet group your platform team grants, with a dedicated security group opened only to the CRs you share it with, AWS-managed credentials mirrored into a Secret, encryption and deletion protection on by default. The operator never deletes an instance.
- **KMS** — standalone keys for deliberate reuse across resources, with real access-level-aware grants.
- **CloudWatch alarms** — per-resource alarms, including notification to a topic owned by a different team's CR.
- **IAM** — one role per CR, derived from exactly what it owns and what's been explicitly shared with it, attached via IRSA.

Every resource type shares one safety model: ownership tagging (the operator
never touches a resource it doesn't own), `adopt`-gated claiming of
pre-existing resources, `deletionPolicy: Retain` by default with a non-empty
guard, and a persisted ownership ledger that survives a resource being
removed from spec.

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

IAM, the connection ConfigMap and status all resolve from that one grant. An
ungranted reference is reported, not silently dropped or allowed.

## Installation

Every release is published from `main` only after every check passes for that
exact commit.

### Prerequisites

- A cluster with an **IAM OIDC identity provider** (standard on EKS), and the
  **`--oidc-provider-arn` and `--oidc-provider-url` flags** set on the
  manager. Every IAM role the operator derives is trust-scoped to that
  provider, so any CR needing IAM fails reconciliation without them.
- An IAM role for the operator's own pod, attached via IRSA and scoped to
  the AWS services you intend to let it manage. See
  [docs/threat-model.md](docs/threat-model.md) for what it can and can't do
  with that access.
- **Pod base security groups**, only if CRs use `rds.consumes`: set
  `manager.podBaseSecurityGroupIds` (normally the EKS cluster security
  group). See [docs/resources.md](docs/resources.md#rds).

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
configurable field. The manager keeps Go's memory use under 90% of its
container's memory limit unless `GOMEMLIMIT` is set.

### Plain `kubectl` (kustomize-based bundle)

Every tagged release also publishes a consolidated install manifest as a
release asset — no Helm required:

```bash
kubectl apply -f https://github.com/Ningendo7/cloudctl-operator/releases/latest/download/install.yaml
```

This installs the CRDs, RBAC and the manager Deployment. You still need to
annotate the manager's ServiceAccount with your IRSA role ARN and set
`--oidc-provider-arn`/`--oidc-provider-url` on the Deployment afterward (or
fork the manifest to set them inline before applying).

Both install paths deploy the same image, built for `linux/amd64`,
`linux/arm64`, `linux/s390x`, and `linux/ppc64le`.

### The `kubectl appdeps` CLI

A read-only kubectl plugin, also usable as `appdeps`, that explains a CR
(`status`), lists its ledger (`resources`) and shows what is kept beyond its CR
(`retained`). Each release attaches signed archives for Linux and macOS:

```bash
VERSION=<release> OS=linux ARCH=amd64   # or darwin, arm64
FILE=appdeps_${VERSION}_${OS}_${ARCH}.tar.gz
curl -LO https://github.com/Ningendo7/cloudctl-operator/releases/download/$VERSION/$FILE
curl -LO https://github.com/Ningendo7/cloudctl-operator/releases/download/$VERSION/$FILE.sigstore.json
cosign verify-blob --bundle $FILE.sigstore.json \
  --certificate-identity https://github.com/Ningendo7/cloudctl-operator/.github/workflows/release.yml@refs/tags/$VERSION \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com $FILE
tar -xzf $FILE kubectl-appdeps appdeps && sudo install kubectl-appdeps appdeps /usr/local/bin/
kubectl appdeps version
```

`checksums.txt` and an SPDX SBOM per platform are attached too. Or build it:
`go install github.com/Ningendo7/cloudctl-operator/cmd/kubectl-appdeps@<release>`.

## Documentation

- [Architecture](docs/architecture.md) — ownership and adoption, the trust window, deletion safety, naming, validation.
- [Resources](docs/resources.md) — field-by-field behavior and known gaps for every resource type.
- [Condition reasons](docs/reasons.md) — every status reason, what it means and what to do.
- [Threat model](docs/threat-model.md) — what a multi-tenant fleet of CRs can and can't make the operator do, and how to limit who may create them.
- [Observability](docs/observability.md) — Prometheus metrics, alerts, the Grafana dashboard and opt-in tracing.
- [Testing](docs/testing.md) — the unit, integration (LocalStack) and live (real AWS) tiers, and what each can't catch.
- [AWS assumptions](docs/aws-assumptions.md) — which AWS behaviors are verified against a real account and which are assumed from documentation.

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
