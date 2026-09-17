# cloudctl-operator

A Kubernetes operator that manages a bundle of everyday AWS dependencies —
SNS, SQS, DynamoDB, S3, and auto-derived IAM (with KMS and CloudWatch alarms
planned) — behind a single opinionated `AppDependencies` CRD, instead of
exposing raw cloud-provider config as YAML.

## Description

App teams provisioning AWS dependencies by hand, or via raw Terraform,
tend to end up with hand-authored IAM policy per app (inconsistent,
error-prone) and operational hygiene — alarms, backups, replication — that's
opt-in and frequently skipped. `AppDependencies` lets a team declare *what
their app needs* (a queue, a topic, a bucket) and has the controller derive
the IAM, naming, and safety semantics that go with it, while still reporting
the concrete result in `status` rather than hiding it behind the defaults.

- **[docs/architecture.md](docs/architecture.md)** — the design decisions:
  ownership and adoption, the trust window, deletion safety, naming,
  validation strategy.
- **[docs/resources.md](docs/resources.md)** — what's actually implemented
  today (SQS, SNS, DynamoDB, S3, IAM), with field-by-field behavior and
  known gaps.

## Getting Started

### Prerequisites
- go version v1.24.6+
- docker version 17.03+
- kubectl version v1.11.3+
- access to a Kubernetes v1.11.3+ cluster with IRSA (IAM Roles for Service
  Accounts) set up, i.e. an OIDC identity provider registered for the
  cluster — required for the auto-derived IAM role's trust policy

### Deploy

```sh
make docker-build docker-push IMG=<some-registry>/cloudctl-operator:tag
make install    # CRDs
make deploy IMG=<some-registry>/cloudctl-operator:tag
```

The manager needs `--oidc-provider-arn` and `--oidc-provider-url` set to
your cluster's IAM OIDC identity provider (see `cmd/main.go`) — every IAM
role it derives is scoped to that provider via IRSA. Without these, the IAM
section refuses to run rather than emitting a role nobody can assume.

Apply a sample CR:

```sh
kubectl apply -k config/samples/
```

### Uninstall

```sh
kubectl delete -k config/samples/
make uninstall   # CRDs
make undeploy
```

Resources with `deletionPolicy: Retain` (the default) are left in AWS on CR
deletion — see [architecture.md](docs/architecture.md) for why.

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
