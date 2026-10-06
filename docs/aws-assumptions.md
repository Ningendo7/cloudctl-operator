# AWS API assumptions

This code depends on specific AWS API behavior that isn't always obvious
from the SDK's type signatures alone. This doc distinguishes what's been
**verified against a real AWS account** (with a date, since AWS behavior
can shift under a floating SDK version) from what's **assumed from
documentation** and never independently checked. Add to this file whenever
either kind of assumption gets made or verified — see
[testing.md](testing.md) for how the disposable-test verification pattern
works.

## Verified against real AWS

### S3 `CreateBucket` + tagging is atomic via `CreateBucketConfiguration.Tags`

**Verified 2026-09-30.** `s3.CreateBucketInput.CreateBucketConfiguration.Tags`
lets bucket creation and ownership tagging happen in one call, including in
`us-east-1` with no `LocationConstraint` set — an older assumption in this
codebase held that the whole `CreateBucketConfiguration` struct had to be
omitted for `us-east-1`, which was wrong and cost this resource type an
entire `TagPending`/checkpoint/trust-window carve-out that every other
resource type (which all support atomic create+tag natively) never needed.
Adoption of a pre-existing foreign bucket still needs the older separate
`PutBucketTagging` call, since there's no "tag on adopt" API.

### DynamoDB's `CREATING` → `ACTIVE` table lifecycle

**Verified 2026-09-30.** `TestLive_Ensure_CreatesRealTableAsyncAndBecomesActiveWithTags`
(`internal/resources/dynamodb/live_test.go`) creates a real table, confirms
`Ensure` records it in the ledger as `Creating` immediately after
`CreateTable` returns (before `DescribeTable` would ever report `ACTIVE`),
waits for a real transition to `ACTIVE` (up to 3 minutes), then re-runs
`Ensure` and confirms it moves the ledger entry to `Verified`. Watched
end-to-end against a real account, not just LocalStack's simulation of it.

### SQS `SetQueueAttributes` accepts an empty-string `RedrivePolicy` to clear it

**Verified 2026-09-30.** Setting `RedrivePolicy` to `""` via
`SetQueueAttributes` genuinely clears the attribute rather than being
rejected or ignored — confirmed against a real queue. This is what
`reconcileAttributes` (`internal/resources/sqs/sqs.go`) relies on to detach
a DLQ that's been removed from spec. Note: `CreateQueue` also accepts an
explicit empty-string value for the same attribute, though that path isn't
currently used anywhere in this codebase.

### SNS's tagging-family calls raise a different "not found" exception than its attribute calls

**Verified 2026-09-30.** `ListTagsForResource`/`TagResource`/
`UntagResource` raise `types.ResourceNotFoundException` for a nonexistent
topic; `GetTopicAttributes`/`SetTopicAttributes` raise the differently-named
`types.NotFoundException` for the exact same condition. `ensureTopic`
(`internal/resources/sns/sns.go`) originally checked only for
`NotFoundException` on the `ListTagsForResource` call made right after
`CreateTopic` — since that call actually returns the other type, the
"topic doesn't exist yet, go create it" branch never matched on a
genuinely new topic, silently breaking creation of every new SNS topic
until this was caught against a real account.

### IAM role propagation lag is invisible to `DescribeRole`

A freshly created IAM role can take several seconds to become universally
assumable (e.g. via IRSA) after `CreateRole` returns success, but no
`DescribeRole` field exposes that lag — there's nothing for this operator's
reconcile loop to poll or wait on. Not addressed here by design; the
realistic fix lives in the *consuming* workload's own SDK retry behavior,
not in this controller.

**Measured 2026-10-02.** `TestLive_Ensure_MeasuresRolePropagationLag`
(`internal/resources/iam/live_test.go`) creates a real role and repeatedly
attempts `sts:AssumeRole` against it until it succeeds, against a generous
60-second ceiling rather than a hard timing assertion (propagation time
isn't guaranteed to be stable run to run). One real measurement: assumable
after ~9.1 seconds, 9 attempts.

## Assumed from documentation, not independently verified

These are used throughout the code but have only ever been checked against
AWS's published docs/SDK error type definitions, not exercised against a
real account. Listed here so a future bug report pointing at one of these
has somewhere to start.

### Error classification (`internal/aws/errors.go`)

`IsRetryable` treats `smithy.ErrorFault() == FaultServer`, throttling error
codes, DynamoDB's `ResourceInUseException`, and IAM's
`ConcurrentModification` as transient/retry-worthy; `IsPermissionDenied`
treats `AccessDenied`/`AccessDeniedException`/`UnauthorizedException`/
`UnauthorizedOperation`/`NotAuthorized`/`AuthorizationError` (plus a bare
403 for calls that don't implement `smithy.APIError`, e.g. S3's
`HeadBucket`) as permission gaps. Both lists are built from AWS's published
exception docs per service, not from having actually triggered each one for
real. `ResourceInUseException` and `ConcurrentModification` in particular
are worth a real verification pass at some point, since they're the two
that most directly gate whether a reconcile gets retried automatically
versus surfaced as a hard failure.
