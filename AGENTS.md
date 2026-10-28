# cloudctl-operator - agent guide

A Kubernetes operator that provisions an app's AWS dependencies (SQS, SNS, S3,
DynamoDB, RDS, KMS, CloudWatch alarms) from one `AppDependencies` CR, derives the
IAM role for it, and keeps correcting drift. Start with [README.md](README.md);
the design reasoning is in [docs/architecture.md](docs/architecture.md) and the
threat model in [docs/threat-model.md](docs/threat-model.md). Read those before
changing ownership, deletion, naming or sharing behaviour.

## Layout

```
api/v1alpha1/                 CRD types and their kubebuilder markers (validation lives here, as CEL)
cmd/main.go                   Manager entry point and flags
cmd/kubectl-appdeps/          The read-only CLI
internal/controller/          Reconciler: one section per resource type, run in dependency order
internal/resources/<type>/    One package per AWS resource type: the resource, cleanup, policy, tests
internal/aws/                 Client interfaces, rate limiting, error classification, naming, tags
internal/awspolicy/           Adding and removing the operator's own statements in resource policies
internal/status/              Conditions, the ownership ledger, events
internal/reasons/             The catalogue of condition reasons
internal/livetest/, policycheck/   Helpers for the live tests
charts/chart/                 Helm chart (generated, see below)
hack/helm/                    Chart additions that survive regeneration
config/                       Kustomize base and generated CRD/RBAC
test/e2e, test/policy, test/alerts   Suites that run outside the package tests
docs/                         User documentation
```

## Generated files: do not edit by hand

`config/crd/bases/`, `config/rbac/role.yaml`, `**/zz_generated.*`, `PROJECT`, and everything
under `charts/chart/` are produced by `make manifests generate helm-generate`. A hand edit is
overwritten, and the Manifests Drift check fails when the committed files differ from a fresh
regeneration.

The chart is regenerated wholesale, so a chart-only change goes in one of two places:

- a `sed` in the `helm-generate` recipe in the `Makefile` (a value or template line inside an
  existing block, as `extraEnv` and `podBaseSecurityGroupIds` are), or
- a top-level block in `hack/helm/values-*.yaml`, appended to `values.yaml`.

Then run `make helm-generate` and commit the result. Never remove `+kubebuilder:scaffold:*`
markers. A new API kind is added with `kubebuilder create api`, not by hand.

## What must stay true

- **Never act on a resource the operator does not own.** Ownership is the `cloudctl.io/owner` and
  `cloudctl.io/owner-uid` tags, re-checked before any destructive action. A name match is never
  ownership; taking over needs `adopt: true`.
- **`deletionPolicy` defaults to `Retain`.** `Delete` is opt-in, and a resource with data in it is
  not deleted without `force`. The operator never calls `DeleteDBInstance`.
- **Backup protections only go up.** Drift correction never turns off or shortens something
  another system may rely on.
- **A statement the operator writes into a resource policy has its own Sid, and everything else in
  the policy is left alone.** Write only when the policy differs.

## When you change X, also change Y

| Change | Also |
|---|---|
| A field or marker in `api/` | `make manifests generate helm-generate`; add a row to the table in `internal/controller/appdependencies_validation_test.go` if it adds validation |
| A new AWS call | Add it to the client interface and the fake; add the IAM action to the permissions table in `docs/threat-model.md` (`internal/aws/permissions_doc_test.go` fails otherwise) |
| A new condition reason | Add it to `internal/reasons`, then `go test ./internal/reasons -run Doc -update` to regenerate `docs/reasons.md` |
| A metric or alert | `docs/observability.md`; alert rules are tested by `test/alerts` with promtool |
| An admission policy template | `make policy-sample`, which regenerates `config/samples/policy/` |
| A policy or API behaviour only AWS can judge | A live test (below) |

## Tests

| Tier | Run | Needs |
|---|---|---|
| Unit | `make test` | Nothing external; envtest for the controller |
| Integration | `make test-integration` | LocalStack in a container |
| E2E | `make test-e2e` | A kind cluster; use a dedicated one, never a real cluster |
| Live | `make test-live` | Real AWS credentials; creates and deletes real resources and costs money |

The integration, live and e2e files carry build tags. `make lint` lints each tag separately, so
code behind a tag is linted too. See [docs/testing.md](docs/testing.md).

The fakes encode our own belief about AWS, so they cannot catch a wrong belief. Several defects
were found only by the live tier: SNS rejects a wildcard action in a topic policy, DynamoDB accepts
only letters and digits in a statement ID, SQS now refuses plain HTTP on every queue, and IAM has no
`dynamodb:TransactGetItems`. When a change depends on how AWS treats a policy, an action name or an
error code, add a live test that shows it.

## Code conventions

- `gocyclo` fails above 20; aim for 15 or less. Do not add `nolint` without a reason on the line.
- `lll` limits lines to 120 characters, except under `api/`, `internal/` and `test/e2e/`.
- Use the `slices` package, not `sort`; `for range n`; preallocate slices of known length.
- Comments say why, never what or when. They do not refer to tracking IDs, audits, reviews or other
  projects, and do not promise behaviour that no test checks.
- Log messages start with a capital letter, end without a period, and are in the past tense
  (`log.Info("Created Deployment", "name", name)`).
- Docs cover configuration, prerequisites and reason names, not every behaviour.
