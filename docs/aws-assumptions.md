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

### What each service accepts in a policy, and what the operator's permissions need

**Verified 2026-10-28**, by the live tests and by calling the services directly.

- SNS refuses a wildcard in the action of a topic policy statement
  (`InvalidParameter: Policy statement action out of service scope!`, for
  `sns:*` and `SNS:*` alike), so the transport statement names the topic
  actions. Explicit actions in either case are accepted.
- DynamoDB accepts only letters and digits in the ID of a resource policy
  statement. A deny on writes took effect within about ten seconds. A second
  change within fifteen seconds of the last is refused with
  `ThrottlingException`, and `DeleteTable` right after a policy change fails
  with `ResourceInUseException` ("has a pending resource-based policy update");
  both are retried.
- DynamoDB answers a change to a table's tags made while another is in flight
  with `LimitExceededException: Subscriber limit exceeded: Table tags are being
  updated`. It clears on its own and is retried. Other services use the same
  code for quotas that never clear, so only this message and the one for too many
  simultaneous table operations count as retryable.
- DynamoDB refuses `DeleteTable` on a protected table with a
  `ValidationException`, which is not retried. `UpdateTable` clears the
  protection, after which the delete goes through.
- SQS accepts 20 statements in a queue policy and refuses 21 (`OverLimit`). Ten
  subscriptions plus the operator's two statements come to about 5 KB of the
  8,192 bytes allowed. SQS also refuses plain HTTP on every queue itself
  (`InvalidSecurity: All requests to this queue must use HTTPS and SigV4`), so
  the transport statement cannot be seen doing anything there.
- `CreateBucket` with tags needs `s3:TagResource`, and `CreateTable` with a
  customer key needs `kms:CreateGrant`; both are refused without them.
  `PutMetricAlarm` with tags worked, and applied the tags, without
  `cloudwatch:TagResource`, so that entry is kept only because AWS documents it.
- Every action in the permissions table is one Access Analyzer recognises.
  `dynamodb:TransactGetItems` and `dynamodb:TransactWriteItems` are not IAM
  actions: transactions are authorised by the item actions they contain.
- SNS delivery into a queue encrypted with a customer key, and a CloudWatch
  alarm into an encrypted topic, both work under key policy statements scoped
  by `aws:SourceArn`.
- `CleanupSubscriptions` finishes without error for a subscription SNS no
  longer has, and for a queue that is gone.
- `CreateDBInstance` without a `StorageType` produces gp2 storage (measured on
  a 20 GiB PostgreSQL instance), so the operator asks for gp3, which a real
  instance then reports.
- S3 and DynamoDB judge a request on an open connection by the policy that
  connection last saw. A client that opens a new connection per request saw a
  policy added or deleted within seconds (S3) or about ten (DynamoDB), while a
  client reusing one kept being served under the old policy for more than two
  minutes. The deny statements are therefore a safety net, not an instant
  block, for an application with connections already open.
- An RDS instance's `CopyTagsToSnapshot` and `MaxAllocatedStorage` apply
  without the instance leaving `available`.

## Assumed from documentation, not independently verified

These are used throughout the code but have only ever been checked against
AWS's published docs/SDK error type definitions, not exercised against a
real account. Listed here so a future bug report pointing at one of these
has somewhere to start.

### DynamoDB table statuses and KMS key states

`tableStatusError` (`internal/resources/dynamodb/dynamodb.go`) reports a table
in `INACCESSIBLE_ENCRYPTION_CREDENTIALS`, `ARCHIVING` or `ARCHIVED` as
`KeyUnavailable`, and treats `REPLICATION_NOT_AUTHORIZED` (a global-table state
this operator never sets up) as active. `keyStateError`
(`internal/resources/kms/kms.go`) reports a key in `PendingImport`,
`PendingReplicaDeletion` or `Unavailable` the same way. Both follow the SDK's
enum values and AWS's documentation; none of these states has been triggered
against a real account.

### Error classification (`internal/aws/errors.go`)

`IsRetryable` treats `smithy.ErrorFault() == FaultServer`, throttling error
codes, DynamoDB's `ResourceInUseException`, and IAM's
`ConcurrentModification` as transient/retry-worthy; `IsPermissionDenied`
treats `AccessDenied`/`AccessDeniedException`/`UnauthorizedException`/
`UnauthorizedOperation`/`NotAuthorized`/`AuthorizationError` (plus a bare
403 for calls that don't implement `smithy.APIError`, e.g. S3's
`HeadBucket`) as permission gaps. Both lists are built from AWS's published
exception docs per service, not from having actually triggered each one for
real. `ResourceInUseException` has since been seen and is retried (above).
`ConcurrentModification` has not: forty-eight concurrent role-policy writes all
succeeded, so it still gates retry-versus-failure without ever having been
observed.
