# Resources

What `AppDependencies` actually manages today. See
[architecture.md](architecture.md) for the design decisions behind the
behavior described here (ownership, deletion safety, naming).

Planned but not yet implemented: S3, DynamoDB, KMS, auto-derived IAM,
CloudWatch alarms, backup policy. Their CRD shape exists in
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

## Shared behavior

- **Ownership tagging, adoption, and the trust window** — see
  [architecture.md](architecture.md#ownership-a-resource-is-owned-by-exactly-one-cr).
- **Deletion safety, the quiet window, and `PendingDeletion`/`StuckPendingDeletion`** —
  see [architecture.md](architecture.md#deletion-is-a-lifecycle-not-a-single-api-call).
- **Status conditions** — each section reports its own condition
  (`SQSReady`, `SNSReady`, ...) plus an aggregate `Ready` condition on the CR.
