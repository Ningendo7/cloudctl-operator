# Testing

Three tiers, each catching a different class of bug.

## Unit (`go test ./...`, always runs)

Every `internal/resources/*` package tests against its own hand-written
fake AWS client, and `internal/controller` runs the same reconciler code
against a real Kubernetes API server via envtest (real CRDs, finalizers,
watches) with those same fakes standing in for AWS.

**What this can't catch:** a fake only ever encodes our own belief about
how an AWS API behaves. If that belief is wrong — a misread error code, a
field we assumed was optional but isn't — a fully green unit suite will
never notice, because the fake just does whatever we told it to.

## Integration (`make test-integration`, CI-gated on every push)

Runs the real AWS SDK against a [LocalStack](https://www.localstack.io/)
container instead of the fakes, for the create/adopt/drift-correct/delete
lifecycle of each resource type. This is what actually catches the gap
above: it validates real request/response shapes and real error codes,
not our mental model of them.

Gated behind the `integration` build tag, so a bare `go test ./...` never
touches Docker or a network dependency — local iteration stays exactly as
fast and dependency-free as it is today. Run it locally with:

```sh
make test-integration
```

which starts LocalStack, runs the suite, and tears the container down
afterward. CI runs the same suite unconditionally on every push, via a
LocalStack service container — see `.github/workflows/integration.yml`.

**Current coverage:** SQS, S3, SNS, and DynamoDB — each covering the same
baseline lifecycle (create+tags, idempotent reconcile, adopt an untagged
resource, correct attribute drift, forced delete). S3 in particular also
has code guessing at string-matched error codes with no typed SDK exception
to verify against (`isServerSideEncryptionConfigurationNotFoundError`,
`isNoSuchTagSet`; see [resources.md](resources.md)). KMS and IAM are
deferred for now — LocalStack's community edition has historically been
the least faithful for those two, so testing against it there risks false
confidence more than real coverage.

New integration-tested packages get a `<package>_integration_test.go`
file with a `//go:build integration` tag, added to
`INTEGRATION_TEST_PACKAGES` in the `Makefile` and to the `go test` command
in `.github/workflows/integration.yml`.

## Live (`make test-live`, never runs in CI)

The same idea as the integration tier, one level up: real AWS instead of
LocalStack, the final check that LocalStack's own emulation hasn't itself
diverged from real AWS behavior. Deliberately **never** runs in GitHub
Actions CI — it would need real AWS credentials in a public repo's CI
secrets, costs money per run, and can't be bounded to "only ever talks to
LocalStack" the way the integration tier can. Gated behind the `live` build
tag, so it's never picked up by `go test ./...`, `make test-integration`, or
CI by accident. Every test skips cleanly (via a harmless
`sts:GetCallerIdentity` check) unless real credentials resolve through the
standard AWS credential chain, creates its own uniquely-named real
resource(s), and cleans up via `t.Cleanup` even on failure.

**Current coverage:** SQS, SNS, S3, DynamoDB. Each tier is deliberately
lean — calibrated against forge-operator's own restrained live-tier
practice (a handful of tests per service, not exhaustive scenario
coverage) — and targets specifically the kind of thing a fake or
LocalStack can't be trusted to catch: real, undocumented, or
easy-to-mismodel API behavior, not plain CRUD. This is exactly how it's
earned its keep so far — real bugs found only once these tests ran against
actual AWS, never caught by the unit or integration tiers:

- **sqs:** the real character-set restrictions on AWS resource names, once
  found to collide with the internal ledger-key separator character
  (`internal/aws/naming.go`'s `DerivedResourceName`); a stale-UID
  adoption gap present in all five resource packages
  (`cloudctlaws.IsStaleUID`); `sqs:SendMessageBatch` not being a real,
  recognized SQS action.
- **sns:** `ListTagsForResource`'s "not found" case raising
  `ResourceNotFoundException`, not the `NotFoundException` every other
  topic operation uses — the exact mismatch silently broke creating any
  brand-new topic, invisible to every unit/controller test because both
  in-memory fakes simulated the same wrong exception type the buggy code
  checked for.
- **dynamodb:** `ContinuousBackupsUnavailableException` (a normal,
  transient state right after a table reaches `ACTIVE`) not being
  classified as retryable by the shared `internal/aws.IsRetryable`.

KMS and IAM have no dedicated live tier of their own — KMS's
dedicated-key path is already exercised end-to-end through sqs/sns/s3/
dynamodb's own live tests (same code, only the `resourceType` string
differs), and a standalone IAM live tier is still open work.

Run it locally with whatever already authenticates your AWS CLI:

```sh
make test-live
```

## E2E

Kubebuilder's own scaffolded tier (`make test-e2e`, `test/e2e/`, tagged
`e2e`): spins up a kind cluster and deploys the built manager image,
currently checking only that the manager comes up healthy and serves
metrics. Distinct from the integration tier above — it exercises
deployment plumbing (RBAC, the manager binary, the metrics endpoint), not
AWS reconciliation behavior, and doesn't touch LocalStack or AWS at all
today.
