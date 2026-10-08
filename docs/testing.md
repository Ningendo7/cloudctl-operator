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
LocalStack service container — see `.github/workflows/test.yml`.

**Current coverage:** SQS, S3, SNS, DynamoDB, and IAM — each covering the
same baseline lifecycle (create+tags, idempotent reconcile, adopt/refuse
an unowned resource, forced delete), plus SQS's subscription management
(real SNS `Subscribe`/`Unsubscribe` and the queue policy grant together).
S3 in particular also has code guessing at string-matched error codes
with no typed SDK exception to verify against
(`isServerSideEncryptionConfigurationNotFoundError`, `isNoSuchTagSet`; see
[resources.md](resources.md)). Two things are deliberately deferred here
— LocalStack's community edition has historically been the least
faithful for both, so testing against it risks false confidence more
than real coverage:

- Standalone KMS (the `kms.resources` type, as opposed to the
  dedicated-key path every other resource type already exercises).
- CloudWatch alarms — confirmed directly, not just assumed: this
  package's integration test originally lived here, but `DescribeAlarms`
  against the pinned LocalStack image returned a persistent
  `500 InternalError` ("An unknown error occurred when trying to
  serialize the response"), retried three times, failing every time, on
  every test that reached it. Not our bug — LocalStack's own CloudWatch
  alarm emulation. Removed rather than worked around; alarms still has a
  live tier (below), which passes against real AWS.

New integration-tested packages get a `<package>_integration_test.go`
file with a `//go:build integration` tag, added to
`INTEGRATION_TEST_PACKAGES` in the `Makefile` and to the `go test` command
in `.github/workflows/test.yml`.

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

**Current coverage:** SQS (including subscriptions), SNS, S3, DynamoDB,
IAM, KMS, and alarms (CloudWatch). Each tier is deliberately
lean — a handful of tests per service, not exhaustive scenario coverage —
and targets specifically the kind of thing a fake or LocalStack can't be
trusted to catch: real, undocumented, or easy-to-mismodel API behavior
(wrong exception types, string-matched error codes with no typed SDK
equivalent, illegal characters in a generated name, a transient state
missing from the shared retryable-error classification), not plain CRUD.
Findings from this tier that turned out to be real bugs get fixed directly
in the relevant package, not logged here — `git log` and
[aws-assumptions.md](aws-assumptions.md) are the record of what's actually
been confirmed against real AWS and when.

The dedicated-key path (every other resource type's `encryption.enabled`)
is exercised end-to-end through sqs/sns/s3/dynamodb's own live tests
(same code, only the `resourceType` string differs) — including its
delete path, scheduled directly via a real `ScheduleKeyDeletion` call at
AWS's 7-day minimum rather than through `kms.Cleanup()`, since AWS has no
faster or instant way to actually delete a key. The standalone
`kms.resources` type has its own live tier covering creation, tagging,
rotation, and adoption, with cleanup following that same direct-schedule
pattern — but deliberately does **not** re-verify `kms.Cleanup()`'s own
`ScheduleKeyDeletion` call against real AWS: that call's shape is already
proven by the dedicated-key tests above, `Cleanup()`'s surrounding logic
(the quiet window, backdating, ledger removal) is pure Go already covered
by the unit tier's fakes, and the only difference would be the literal
window value (30 days here vs. 7 there) — not a class of bug this tier
exists to catch. Still no integration tier for standalone KMS (see the
note above on why LocalStack is deferred for it).

Run it locally with whatever already authenticates your AWS CLI:

```sh
make test-live
```

## E2E

`make test-e2e` (`test/e2e/`, tagged `e2e`): spins up a kind cluster,
deploys the built manager image, and deploys LocalStack inside the
cluster itself (`localstack-system` namespace, kept separate from the
manager's own namespace since LocalStack's image doesn't run under the
manager's restricted Pod Security Standard). Beyond the baseline check
that the manager comes up healthy and serves metrics, a dedicated
lifecycle suite per resource type (SQS, SNS, S3, DynamoDB, KMS) applies a
real `AppDependencies` CR and verifies the full lifecycle end-to-end
against that in-cluster LocalStack: create, adopt an untagged resource,
retain-and-relinquish on removal from spec, the non-empty delete guard,
and pending-deletion cancellation when a resource reappears in spec.
Distinct from the integration tier above specifically in scope, not
mechanism — both go through LocalStack, but this tier drives everything
through a real Kubernetes reconcile loop (CRDs, finalizers, the actual
controller binary) rather than calling package functions directly.
