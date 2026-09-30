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

### SQS `SetQueueAttributes` accepts an empty-string `RedrivePolicy` to clear it

**Verified 2026-09-30.** Setting `RedrivePolicy` to `""` via
`SetQueueAttributes` genuinely clears the attribute rather than being
rejected or ignored — confirmed against a real queue. This is what
`reconcileAttributes` (`internal/resources/sqs/sqs.go`) relies on to detach
a DLQ that's been removed from spec. Note: `CreateQueue` also accepts an
explicit empty-string value for the same attribute, though that path isn't
currently used anywhere in this codebase.

## Assumed from documentation, not independently verified

These are used throughout the code but have only ever been checked against
AWS's published docs/SDK error type definitions, not exercised against a
real account. Listed here so a future bug report pointing at one of these
has somewhere to start.

### DynamoDB's `CREATING` → `ACTIVE` table lifecycle

`DescribeTable().TableStatus` is the only observable signal for a table's
async creation completing (`internal/resources/dynamodb/dynamodb.go`). This
is a stable, long-documented AWS behavior, and the integration test tier
(LocalStack) does exercise it, but no test in this repo has watched a real
AWS table transition end-to-end.

### IAM role propagation lag is invisible to `DescribeRole`

A freshly created IAM role can take several seconds to become universally
assumable (e.g. via IRSA) after `CreateRole` returns success, but no
`DescribeRole` field exposes that lag — there's nothing for this operator's
reconcile loop to poll or wait on. Documented as a known limitation, not
something addressed here; the realistic fix lives in the *consuming*
workload's own SDK retry behavior, not in this controller. See
`project_hardening_audit` memory for the fuller writeup, including how this
generalizes (or doesn't) to a future RDS resource type.

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
