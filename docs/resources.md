# Resources

What `AppDependencies` actually manages today. See
[architecture.md](architecture.md) for the design decisions behind the
behavior described here (ownership, deletion safety, naming).

Planned but not yet implemented: S3, KMS, auto-derived IAM, CloudWatch
alarms, backup policy. Their CRD shape exists in
`api/v1alpha1/appdependencies_types.go` as a preview of the intended
surface, but nothing reconciles them yet — declaring them in a CR today has
no effect.

## SQS

```yaml
spec:
  sqs:
    resources:
      - name: orders
        dlq: true
        fifo: false
        deletionPolicy: Retain
        overrides:
          visibilityTimeoutSeconds: 60
          maxReceiveCount: 5
          contentBasedDeduplication: false
```

| Field | Notes |
|---|---|
| `name` | Required. Combined with the CR's namespace/name to form the actual AWS queue name (see [architecture.md](architecture.md#naming)). |
| `dlq` | Creates a companion dead-letter queue and wires up a redrive policy. The DLQ inherits the main queue's `fifo`-ness — AWS requires a FIFO source queue to pair with a FIFO DLQ. |
| `fifo` | Ordered, deduplicated delivery. **Immutable once set** — AWS doesn't support converting between FIFO and standard queues. |
| `deletionPolicy` | `Retain` (default) or `Delete`. See [architecture.md](architecture.md#deletion-is-a-lifecycle-not-a-single-api-call). |
| `force` | Allows deleting a queue that isn't empty (visible, in-flight, *and* delayed message counts are all checked independently). |
| `adopt` | Required to bring an existing, differently-owned or untagged queue under this CR's management. |
| `sharedWith` | Grants other `AppDependencies` CRs permission to consume this queue (up to 20 entries). Not yet enforced in IAM — auto-derived IAM isn't built yet. |
| `overrides.visibilityTimeoutSeconds` | 0–43200. Drift-corrected on every reconcile if it diverges from spec. |
| `overrides.maxReceiveCount` | 1–1000. Only meaningful with `dlq: true`. |
| `overrides.contentBasedDeduplication` | Only meaningful with `fifo: true`. |

**Known gap:** if `dlq` is toggled from `true` to `false`, the DLQ itself is
deleted correctly, but the main queue's `RedrivePolicy` attribute pointing
at it isn't cleared — AWS's `SetQueueAttributes` docs don't confirm that an
empty value unsets it, so this is left unaddressed pending verification
rather than guessed at.

## SNS

```yaml
spec:
  sns:
    resources:
      - name: events
        fifo: false
        deletionPolicy: Retain
        overrides:
          contentBasedDeduplication: false
```

| Field | Notes |
|---|---|
| `name` | Required. Same naming scheme as SQS, with a 256-character AWS limit. |
| `fifo` | Ordered, deduplicated delivery, deliverable only to FIFO SQS subscriptions. **Immutable once set**. |
| `deletionPolicy` | Same semantics as SQS. A topic's "non-empty" equivalent is having active subscriptions (confirmed or pending) rather than a message backlog — SNS has no backlog concept of its own. |
| `force` | Allows deleting a topic that still has subscriptions attached. |
| `adopt` | Same semantics as SQS. |
| `sharedWith` | Same semantics as SQS; not yet IAM-enforced. |
| `overrides.contentBasedDeduplication` | Only meaningful with `fifo: true`. Drift-corrected on every reconcile. |

**Not yet built:** subscription management (`Subscribe`/`Unsubscribe` from
this operator's side) and the same trust-window read-skip optimization SQS
has — both deliberately deferred past core lifecycle management.

## DynamoDB

```yaml
spec:
  dynamodb:
    resources:
      - name: sessions
        partitionKey: id
        sortKey: createdAt
        deletionPolicy: Retain
        backup:
          enabled: true
          retentionDays: 14
        overrides:
          billingMode: PayPerRequest
```

| Field | Notes |
|---|---|
| `name` | Required. Same naming scheme as SQS/SNS, with a 255-character limit. |
| `partitionKey` / `sortKey` | Required / optional table key schema, as attribute names (always typed as string — see the naming section below). **Immutable once set** — AWS doesn't support changing a table's key schema after creation. |
| `deletionPolicy` | Same semantics as SQS/SNS. A table's "non-empty" equivalent is a real item count, checked via a strongly-consistent count-only `Scan` rather than `DescribeTable`'s `ItemCount` — AWS documents that field as updated only approximately every six hours, which would let a table populated minutes ago sail through as if it were still empty. |
| `force` | Allows deleting a table that isn't empty. Unlike SQS/SNS, `DeleteTable` itself has **no non-empty guard of its own** — this check is the only thing standing between "removed from spec" and irreversible data loss. |
| `adopt` | Same semantics as SQS/SNS. |
| `sharedWith` | Same semantics as SQS/SNS; not yet IAM-enforced. |
| `backup.enabled` | Toggles point-in-time recovery (continuous backups). |
| `backup.retentionDays` | 1–35. Only meaningful with `enabled: true`; AWS's own default (35 days) applies when unset. |
| `overrides.billingMode` | `PayPerRequest` (default) or `Provisioned`. A decision, not raw throughput config — `Provisioned` tables get AWS's own long-standing default capacity (5/5 RCU/WCU) rather than exposing tunable numbers. |

Table creation is **asynchronous** (`CreateTable` returns while the table is
still `CREATING`), unlike SQS/SNS. A newly created table sits in a
`Creating` ledger state until a later reconcile finds it `ACTIVE` — ownership
verification, tagging drift, billing mode, and PITR are all deferred until
then.

**Not yet built:** the same trust-window read-skip optimization SQS has,
and a deny-policy blocking new writes while a table sits in
`PendingDeletion` (SQS/SNS both have this; DynamoDB resource-based policies
are newer and less battle-tested, and the quiet window plus the 7-day grace
period already cover the realistic risk without it).

## Shared behavior

- **Ownership tagging, adoption, and the trust window** — see
  [architecture.md](architecture.md#ownership-a-resource-is-owned-by-exactly-one-cr).
- **Deletion safety, the quiet window, and `PendingDeletion`/`StuckPendingDeletion`** —
  see [architecture.md](architecture.md#deletion-is-a-lifecycle-not-a-single-api-call).
- **Status conditions** — each section reports its own condition
  (`SQSReady`, `SNSReady`, ...) plus an aggregate `Ready` condition on the CR.
