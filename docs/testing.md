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

**Current coverage:** SQS and S3 — deliberately chosen first, since S3 in
particular has code guessing at string-matched error codes with no typed
SDK exception to verify against
(`isServerSideEncryptionConfigurationNotFoundError`, `isNoSuchTagSet`; see
[resources.md](resources.md)). KMS and IAM are deferred for now —
LocalStack's community edition has historically been the least faithful
for those two, so testing against it there risks false confidence more
than real coverage.

New integration-tested packages get a `<package>_integration_test.go`
file with a `//go:build integration` tag, added to
`INTEGRATION_TEST_PACKAGES` in the `Makefile` and to the `go test` command
in `.github/workflows/integration.yml`.

## Live (not yet built)

A future third tier: the same lifecycle tests, but against a real AWS
account instead of LocalStack — the final check that LocalStack's own
emulation hasn't itself diverged from real AWS behavior. Deliberately
**never** runs in GitHub Actions CI: it would need real AWS credentials in
a public repo's CI secrets, costs money per run, and can't be bounded to
"only ever talks to LocalStack" the way the integration tier can. When
built, this runs locally against the maintainer's own AWS account, gated
behind a separate build tag (e.g. `live`) so it's never picked up by
`go test ./...`, `make test-integration`, or CI by accident.

## E2E

Kubebuilder's own scaffolded tier (`make test-e2e`, `test/e2e/`, tagged
`e2e`): spins up a kind cluster and deploys the built manager image,
currently checking only that the manager comes up healthy and serves
metrics. Distinct from the integration tier above — it exercises
deployment plumbing (RBAC, the manager binary, the metrics endpoint), not
AWS reconciliation behavior, and doesn't touch LocalStack or AWS at all
today.
