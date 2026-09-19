# cloudctl-operator

A Kubernetes operator for everyday AWS application dependencies. Declare
*what your app needs* — a queue, a topic, a bucket, a table — and get back
a derived least-privilege IAM role, safe deletion semantics, and
connection details wired straight into your pods. No hand-authored IAM
policy, no raw Terraform-in-YAML.

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

## What it manages

SQS, SNS, DynamoDB, S3, KMS, CloudWatch alarms, and auto-derived IAM.

- **[docs/architecture.md](docs/architecture.md)** — the design decisions:
  ownership and adoption, the trust window, deletion safety, naming,
  validation strategy.
- **[docs/resources.md](docs/resources.md)** — field-by-field behavior and
  known gaps for every resource type currently implemented.
- **[docs/testing.md](docs/testing.md)** — the test tiers: unit (always),
  integration (LocalStack, CI-gated), and what's deliberately deferred.

Resources default to `deletionPolicy: Retain` — removing one from spec, or
deleting the CR, leaves the AWS resource in place rather than risking data
loss on a typo. See [architecture.md](docs/architecture.md) for the full
deletion-safety model.

## Status

Pre-1.0, under active development. SQS, SNS, DynamoDB, S3, KMS, CloudWatch
alarms, and IAM are implemented and covered by unit and envtest suites;
RDS support is next.

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
