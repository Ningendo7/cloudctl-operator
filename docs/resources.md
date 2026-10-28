# Resources

What `AppDependencies` actually manages today. See
[architecture.md](architecture.md) for the design decisions behind the
behavior described here (ownership, deletion safety, naming).

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
| `adopt` | Required to bring an existing, differently-owned or untagged queue under this CR's management. A resource encrypted with a customer-managed KMS key the spec doesn't name is refused with `AdoptionConflict`: declare that key under `kms.resources` with `adopt: true` and point `encryption.kmsKeyRef` at it. This applies to every resource type. |
| `sharedWith` | Grants other `AppDependencies` CRs permission to consume this queue (up to 20 entries), and via `access`, how much (see [IAM](#iam) below). |
| `subscribesTo` | Subscribes this queue to SNS topics (up to 10; each takes one of the 20 statements SQS allows in a queue policy), each referenced by owner namespace, CR name and topic name. The operator creates the real SNS subscription and the queue policy that lets the topic deliver, and removes both when the entry goes. When the queue itself is gone, the subscription is still removed, since SNS keeps it and every publish to the topic would fail delivery. A subscription removed behind the operator's back is restored. The topic's owner must list this CR in its own `sharedWith`, the same as any cross-CR reference. |
| `overrides.visibilityTimeoutSeconds` | 0–43200. Drift-corrected on every reconcile if it diverges from spec. |
| `overrides.maxReceiveCount` | 1–1000. Only meaningful with `dlq: true`. |
| `overrides.contentBasedDeduplication` | Only meaningful with `fifo: true`. |
| `encryption.enabled` / `encryption.kmsKeyRef` | See [KMS](#kms) below. The DLQ shares the main queue's key rather than getting its own. |

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
| `sharedWith` | Same semantics as SQS (see [IAM](#iam)). |
| `overrides.contentBasedDeduplication` | Only meaningful with `fifo: true`. Drift-corrected on every reconcile. |
| `encryption.enabled` / `encryption.kmsKeyRef` | See [KMS](#kms) below. |

Subscriptions are managed from the subscribing queue's side, with the SQS
`subscribesTo` field. A topic with active subscriptions counts as non-empty
for deletion.

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
| `deletionPolicy` | Same semantics as SQS/SNS. New tables are created with deletion protection on, since the operator never recreates one that disappears; its own delete switches the protection off first, and a table that already exists keeps its setting. A table's "non-empty" equivalent is a real item count, checked via a strongly-consistent count-only `Scan` rather than `DescribeTable`'s `ItemCount` — AWS documents that field as updated only approximately every six hours, which would let a table populated minutes ago sail through as if it were still empty. |
| `force` | Allows deleting a table that isn't empty. Unlike SQS/SNS, `DeleteTable` itself has **no non-empty guard of its own** — this check is the only thing standing between "removed from spec" and irreversible data loss. |
| `adopt` | Same semantics as SQS/SNS, plus one DynamoDB-specific check: adoption is refused outright if the existing table's actual key schema doesn't match `partitionKey`/`sortKey` — AWS never allows changing a table's key schema, so adopting a mismatched table would otherwise succeed and only fail later, opaquely, at the workload's first `PutItem`/`Query`. The refusal is `KeySchemaMismatch`. |
| `deletionProtection` | Makes AWS refuse to delete the table until it is turned off. Unset: on for a table the operator creates, and left as it is afterwards, adopted tables included. Set: kept in line while the table is declared, so `false` makes a table deletable by spec and `true` puts protection back if someone removes it. The operator still turns it off itself just before deleting a table it has decided to delete. |
| `sharedWith` | Same semantics as SQS/SNS (see [IAM](#iam)). |
| `backup.enabled` | Turns on point-in-time recovery (continuous backups). It is never turned off: `enabled: false` leaves it on and emits a `BackupsLeftEnabled` warning, since turning it off discards the restore window and something else (AWS Backup, an organisation rule) may rely on it. |
| `backup.retentionDays` | 1–35, a minimum. Only meaningful with `enabled: true`; AWS's own default (35 days) applies when unset. The window is extended to this value and never shortened. |
| `overrides.billingMode` | `PayPerRequest` (default) or `Provisioned`. A decision, not raw throughput config — a new `Provisioned` table gets AWS's own long-standing default capacity (5/5 RCU/WCU) rather than exposing tunable numbers. An existing on-demand table is not switched to `Provisioned` (`ProvisionedCapacityNeeded`), since that placeholder capacity would throttle it: switch it in AWS with the capacity it needs. |
| `encryption.enabled` / `encryption.kmsKeyRef` | See [KMS](#kms) below. |

**A table deleted out-of-band** is not recreated: an empty replacement would be reported Ready. The CR gets a `TableMissing` warning and error. Restore it from a backup under the same name with `adopt: true` (adopted within one trust window, 10 minutes), or remove the entry from spec and add it again to start fresh.

Table creation is **asynchronous** (`CreateTable` returns while the table is
still `CREATING`), unlike SQS/SNS. A newly created table sits in a
`Creating` ledger state until a later reconcile finds it `ACTIVE` — ownership
verification, tagging drift, billing mode, and PITR are all deferred until
then.

For DynamoDB the existence read on every reconcile doubles as the table status
check, which has to be fresh before anything else can proceed. A table in
`INACCESSIBLE_ENCRYPTION_CREDENTIALS`, `ARCHIVING` or `ARCHIVED` fails
`DynamoDBReady` with `KeyUnavailable`, since its KMS key can't be used.

**Writes are blocked while a table waits to be deleted.** From the pass a table
is first noticed as removed from spec, a resource policy statement denies the
writes that add data (`PutItem`, `UpdateItem`, `BatchWriteItem`,
`PartiQLInsert`, `PartiQLUpdate`) on the table and its indexes, so it can be
emptied but not refilled. `DeleteItem` and reads stay open so it can be drained
(or left to TTL), and `force` skips the wait. The statement is removed if the
table is declared again or retained instead, and any other statement in the
table's policy is left alone. DynamoDB refuses a second change to a table's
policy for a few seconds after the first, which the operator retries.

## S3

```yaml
spec:
  s3:
    resources:
      - name: receipts
        deletionPolicy: Retain
        backup:
          enabled: true
        overrides:
          versioningEnabled: true
          lifecycleRules:
            - id: archive-old-invoices
              transitionAfterDays: 90
              transitionStorageClass: GLACIER
```

| Field | Notes |
|---|---|
| `name` | Required, up to 63 characters. The actual bucket name also has an 8-character account-ID hash suffix appended — S3 bucket names are unique across *every* AWS account globally, not just this one, so the plain namespace-crName-key scheme alone isn't safe here. |
| `deletionPolicy` | Same semantics as the other resources. A bucket's "non-empty" equivalent is having *any* object version or delete marker at all, not just current objects — a bucket with backup enabled can have zero current objects but old versions still retained for point-in-time recovery, and those count. |
| `force` | Allows deleting a non-empty bucket. Deletion itself empties every object version and delete marker (paginated, batched 1000 at a time) and aborts in-progress multipart uploads before calling `DeleteBucket` — unlike the other resources, S3 has no single "delete everything" call. |
| `adopt` | Same semantics as the other resources. |
| `sharedWith` | Same semantics as the other resources (see [IAM](#iam)). |
| `backup.enabled` | Enables versioning and a lifecycle policy that cleans up superseded (noncurrent) object versions after 30 days by default. Deliberately never expires or transitions the *current* object — backup protects live data, it doesn't put a deletion timer on it. |
| `overrides.versioningEnabled` | Controls versioning independently of `backup` — useful for a bucket that wants versioning without backup's lifecycle policy, or vice versa. Versioning is turned on by `backup` or by this set to `true`, and suspended only by this set to `false`: a bucket whose spec says nothing keeps whatever it has, since something else (AWS Backup for S3 needs it) may have turned it on. |
| `overrides.lifecycleRules` | Replaces the default backup lifecycle policy entirely. An explicitly empty list (`[]`) opts out of any lifecycle policy, even with `backup.enabled: true` — it does not fall back to the default. |
| `encryption.enabled` / `encryption.kmsKeyRef` | See [KMS](#kms) below. |

Deletion safety matches SQS/SNS in full: a bucket policy Deny on
`s3:PutObject` (which alone covers every write path, including all three
multipart upload calls) blocks new writes the moment a bucket enters
`PendingDeletion`. Within the trust window `HeadBucket` and `GetBucketTagging`
are skipped, but versioning, encryption and lifecycle are still reconciled on
every pass, so a bucket deleted out-of-band is noticed and recreated on that
same pass.

**Not yet built:** `replication` (the CRD field exists but `enabled: true` is
rejected at admission) — cross-region replication needs a bucket and
IAM role in a different region, which needs real multi-region client
support this operator doesn't have yet. Same limitation as DynamoDB's
Global Tables.

## RDS

```yaml
spec:
  rds:
    resources:
      - name: orders-db
        dbSubnetGroupName: prod-private-data-tier
        engine: postgres
        engineVersion: "16"
        instanceClass: db.t4g.micro
        allocatedStorage: 20
        highAvailability: {enabled: true}
        backup: {enabled: true}
        encryption: {enabled: true}
        sharedWith:
          - namespace: fulfillment
            name: fulfillment-service
    consumes:                       # on the consuming CR
      - namespace: checkout
        name: checkout-service
        resourceName: orders-db
```

| Field | Notes |
|---|---|
| `dbSubnetGroupName` | An existing DB subnet group, never created by the operator. The CR's namespace must be covered by an `RDSSubnetGroupGrant` (cluster-scoped, created by a platform admin), otherwise the section reports a retryable authorization error. If the grant is withdrawn later, a new instance is not placed and an existing one is left unchanged, but a consumer removed from `sharedWith` still loses its network access. **Immutable.** |
| `engine` | `postgres`, `mysql` or `mariadb`. **Immutable.** |
| `engineVersion` | A *minimum*, as dotted numbers such as `16` or `16.4`. A higher version upgrades the instance; an instance already at or past it, including one AWS minor-upgraded itself, is left alone. A major upgrade is refused unless `allowMajorVersionUpgrade: true`. |
| `instanceClass`, `highAvailability` | Instance class and Multi-AZ. Changes are drift-corrected (see below). |
| `allocatedStorage` | GiB, 20–65536. Can only grow; AWS rounds a growth up to at least 10% more. |
| `backup` | Automated backups with AWS's default 7-day retention. Only ever turned on or extended: omitting it or disabling it on an instance that has backups leaves them on and emits a `BackupsLeftEnabled` warning, since turning them off deletes the existing backups. |
| `encryption` | `enabled: true` provisions a dedicated key, or `kmsKeyRef` names a key under this CR's `kms.resources` (see [KMS](#kms)). **Immutable once set.** |
| `applyImmediately` | See "Changing a running instance". |
| `deletionPolicy`, `adopt`, `sharedWith` | See below. |
| `replication` | Not implemented; the CRD rejects `replication.enabled: true`. |

**Credentials.** The operator never sets or generates a password. RDS creates and rotates the master user's password in Secrets Manager, and the operator mirrors it into a Secret named `<cr>-rds-credentials` in the *owning* CR's namespace only. Rotation reaches the Secret within one drift-detection interval, but never a running pod's environment. For credentials that must follow rotation, restart the pods when the Secret changes (for example with Stakater Reloader's `reloader.stakater.com/auto: "true"` annotation on the Deployment), or read the RDS-managed secret directly with External Secrets or the Secrets Store CSI driver. The master user can do anything in the database, so consumers never receive it: the owner creates per-app database users and hands the credentials to each team.

**Connection info.** The owner's and each consumer's connection ConfigMap carries `RDS_<NAME>_HOST`, `_PORT`, `_ENGINE` and `_DB_INSTANCE_IDENTIFIER` (consumers' keys also include the producer's namespace and CR name).

**Encrypted and protected by default.** Without `encryption`, a new instance is encrypted with AWS's own RDS key; `encryption: {enabled: false}` opts out. Instances the operator creates have deletion protection on and kept on while declared (`deletionProtection: false` turns it off), so removing one by hand means turning protection off first. An adopted instance keeps its own setting unless the field is set, with a `DeletionProtectionOff` warning when it's off.

**Private by default.** Instances are created with `PubliclyAccessible` explicitly off, since AWS otherwise defaults to public in a default VPC. If an instance later becomes public, the operator emits an `InstancePubliclyAccessible` warning each reconcile but does not change it: that may be a deliberate decision, and the security group is the only access it controls. An adopted instance keeps the security groups it already has, so check them too.

**Network access.** Each instance gets its own security group, created and owned by the operator and never shared. `sharedWith` grants ingress from a consumer CR's pods, which therefore need EKS Security Groups for Pods: the consumer CR gets its own pod security group and a `SecurityGroupPolicy`. A security group found at the expected name is only reused if it carries this CR's ownership tags; `adopt: true` takes over an untagged one or one left by an earlier incarnation of the same CR.

**Pod security groups.** Security Groups for Pods replaces a consumer pod's groups with the database group, which admits nothing, so list the groups the pods still need, normally the EKS cluster security group: `manager.podBaseSecurityGroupIds` in the chart (the manager's `--pod-base-security-group-ids` flag). Without it the pods can stop receiving Service, load balancer and probe traffic, and the operator warns with `PodBaseSecurityGroupsUnset`. It is one list for the whole cluster, and the VPC CNI must run in its default enforcing mode.

Removing `rds.consumes` or deleting the consumer releases its group in order; `RDSReady` shows `WaitingForRuleRelease` until AWS lets it go. An instance's security group is deleted once the instance itself is gone.

**Adopting an existing instance.** With `adopt: true`, an untagged instance at the expected name is adopted only if the spec describes it. Adoption never changes an instance, so it is refused with an `AdoptionConflict` reason, and before anything is created, tagged or modified, when the instance differs on any of:

- `engine`, `dbSubnetGroupName`, or storage encryption (including the key, for a `kmsKeyRef`), none of which can be changed afterwards, or an instance that is publicly accessible;
- `instanceClass` or `highAvailability`, which the first reconcile would otherwise change, possibly downsizing a production instance;
- an `engineVersion` or `allocatedStorage` higher than the instance has, which would upgrade or grow it.

An instance with more storage or a newer version than the spec's minimums is adopted. All conflicts are listed in one message. Make the spec match what exists, adopt, then change the spec: from then on changes go through the normal flow below. An instance already owned by another CR is refused for that reason first.

**Changing a running instance.** Class, Multi-AZ, backup, engine-version and storage changes wait for the instance's weekly maintenance window by default, with a `ChangeDeferred` event, because some of them restart the instance. Set `applyImmediately: true` to apply them at once. Storage growth is the exception: it applies at once when nothing else is pending, since an immediate change also applies every pending one. An edit made while a change is pending replaces the pending value. A security group change always applies at once. While an instance is `backing-up` or `storage-optimization` it is reported ready but not modified; any other status that isn't `available` is retried.

**Observing an instance.** The status AWS reports for each instance is recorded on its `status.managedResources` entry as `observedStatus`, with `observedStatusSince` set when the value last changed (not on every poll). While the instance isn't usable, `RDSReady` is `False` with a reason derived from that status: `InstanceCreating`, `InstanceModifying`, `InstanceConfiguringEnhancedMonitoring` and so on, or `InstanceFailed`, which is not retried and needs manual investigation. The field is informational and never gates an action.

**An instance deleted out-of-band.** If the ledger knows an instance but AWS no longer has it, the operator does not create a replacement: an empty database reported Ready would hide the loss. `RDSReady` goes `False` with reason `InstanceMissing` and an `InstanceMissing` warning event, naming two ways out. Restore it under the same identifier from a snapshot or point-in-time backup and set `adopt: true` (the usual adoption checks then confirm it matches the spec), or remove it from spec and add it again to start fresh. An instance still in its first minutes after creation is retried quietly instead, in case AWS hasn't listed it yet.

**Deletion.** The operator never calls `DeleteDBInstance`. With `deletionPolicy: Delete`, an instance removed from spec goes through the quiet window and a one-hour grace period, then escalates to `InstanceStuckPendingDeletion` on the `RDSReady` condition while the operator takes a final snapshot so the eventual manual delete has a restore point; billing continues until someone deletes the instance. Each pass first re-reads the instance's ownership tags and refuses to act (no snapshot, no stuck report) if they no longer match this CR; an instance already gone from AWS is dropped from the ledger. Final snapshots carry the same tags and are only deleted while they do. `Retain` (the default) just removes the ownership tag.

## KMS

Every resource type above (SQS, SNS, DynamoDB, S3, RDS) exposes an
`encryption` field, offered as two mutually exclusive modes rather than a
spectrum:

```yaml
spec:
  sqs:
    resources:
      - name: orders
        encryption:
          enabled: true          # dedicated key, owned and named after this resource

  sns:
    resources:
      - name: events
        encryption:
          kmsKeyRef: general-purpose   # reuse a key declared below instead

  kms:
    resources:
      - name: general-purpose
```

| Field | Notes |
|---|---|
| `encryption.enabled` | Provisions a dedicated key just for this resource (ledger name `<resource>-key`), created and tagged the same as every other owned resource. No `spec.kms` section needs to be touched — this is the common case. |
| `encryption.kmsKeyRef` | The name of an entry in **this CR's own** `kms.resources`, for deliberate reuse of one key across several of this CR's resources. A name that isn't declared is reported on the `KMSReady` condition as `KMSKeyRefNotDeclared`; a declared key that hasn't been created yet is a retryable, self-resolving condition. |

Keys are never shared across CRs. A resource encrypted with another
team's key would depend on that team for its own data's availability:
revoking access would take it down, and deleting the key would make its
data permanently unreadable. A centrally managed key belongs outside this
operator, with a platform or security team.

A freshly created key is `TagPending` in the ledger until its alias and
rotation are set up, even though tagging is atomic with `CreateKey`. A crash
in between leaves a record findable by ARN, so the next reconcile never
creates a second key.

A standalone `kms.resources` entry (as used by `kmsKeyRef` above) supports
`deletionPolicy` and `adopt` like every other resource type, but
deliberately **no `force`**: unlike a queue or a bucket, a key isn't
"non-empty" in a way force could legitimately override, since deleting one
while anything still uses it to encrypt data makes that data permanently
unrecoverable. `ScheduleKeyDeletion` uses AWS's maximum 30-day window for
the same reason.

A key is never scheduled for deletion while data encrypted with it still
exists, whatever its own `deletionPolicy`. Each resource's ledger entry
records the ARN of its key (`kmsKeyARN`), so this holds after the resource
leaves spec too: a non-empty queue, bucket or table still pending deletion,
a retained resource, or an RDS final snapshot all keep their key. On CR
deletion, keys go last: a key whose data is still draining holds the
finalizer until that data is gone, and a key protecting retained data is
kept and relinquished with it (a `KeyRetainedForData` warning says so when
the key's own policy was `Delete`).

The CR's IAM role gets `kms:Decrypt` and `kms:GenerateDataKey` on every key
the CR declares or provisions.

A consumer of an encrypted queue, topic or bucket gets the key permissions it needs too. A key that is disabled or unavailable fails `KMSReady` (`KeyDisabled`, `KeyUnavailable`) and is never re-enabled by the operator. Keys also let SNS and CloudWatch use them, so SNS can deliver into an encrypted queue and alarms can publish to an encrypted topic. Encrypted S3 buckets use an S3 Bucket Key, which cuts KMS requests; KMS then sees the bucket ARN as the encryption context instead of each object's.

**Upgrading from v0.2.0.** v0.3.0 removed the cross-CR `encryption.kmsKeyRef` object and `kms.resources[].sharedWith` / `kms.consumes`. Go straight to v0.3.1 or later, since v0.3.0 cannot decode a CR that still has the object form; from v0.3.1 such a CR fails `KMSReady` with `LegacyKMSKeyRef` until it is edited.

**Upgrading from before v0.4.0.** Releases from v0.4.0 record which key each resource is encrypted with. A resource that used `kmsKeyRef` and was already removed from spec before then has no recorded key, so removing its key could schedule the key for deletion while data encrypted with it remains. Upgrade to any release from v0.4.0 on and let each CR reconcile once before removing encrypted resources and their keys.

**Removing `encryption` is not applied.** Drift correction only ever moves
in the "enable, or point at a different key" direction: dropping `encryption`
from an entry leaves the resource encrypted with its key. The CR reports it
with `EncryptionMatchesSpec: False` (`EncryptionRemovalIgnored`) and a Warning
event on each pass; `Ready` is unchanged. To get an unencrypted resource, remove
the entry, wait for it to be deleted, and add it again.

## Alarms

A single CR-wide switch — not per-resource — that provisions opinionated
CloudWatch alarms for whatever SQS/SNS/DynamoDB resources this CR
declares:

```yaml
spec:
  alarms:
    enabled: true
    snsTopicRef:            # optional
      namespace: platform
      name: platform-alerts
      resourceName: pagerduty-bridge
```

| Field | Notes |
|---|---|
| `enabled` | One switch for the whole CR — a team declaring resources together almost always wants uniform alerting across them. |
| `snsTopicRef` | Points at an SNS topic to notify when an alarm fires or clears, resolved through the same `sharedWith`/`consumes` authorization as any other cross-CR reference — the topic's owner must list this CR in its `sharedWith`. Left unset, alarms are still created (visible in CloudWatch, e.g. for a dashboard) but have no actions, so nothing pages anyone. |

What gets created, per resource type:

| Resource | Alarm | Threshold |
|---|---|---|
| SQS queue | `ApproximateAgeOfOldestMessage` | > 15 minutes for 3 consecutive 5-minute periods |
| SQS DLQ (`dlq: true`) | `ApproximateNumberOfMessagesVisible` | > 0 — a poison message existing at all is the signal, not its age |
| SNS topic | `NumberOfNotificationsFailed` | > 0 |
| DynamoDB table | `ReadThrottleEvents` and `WriteThrottleEvents` | > 0, one alarm each |

S3 and KMS deliberately have no alarms: S3 has no free per-request
CloudWatch metrics (they require opting into paid request metrics), and a
KMS key has no operationally meaningful metric of its own.

Unlike every other resource type in this operator, alarms carry **no
`status.managedResources` ledger entry, no `adopt`/`force`, and no quiet
window before deletion**. Two things make that safe here in a way it isn't
elsewhere: `PutMetricAlarm` is a genuine create-or-update by alarm name
(no "already exists" race the way `CreateQueue`/`CreateTopic` have), and
an alarm holds no data — deleting one is trivially reversible, since it's
recreated on the next reconcile if still desired. Every reconcile is a
plain declarative diff: compute the desired set from spec, list what
currently exists under this CR's deterministic name prefix, create or
update what's desired, and delete whatever this CR owns that no longer
is — all still gated on the same ownership-tag-before-mutate check every
other resource type uses, so this never silently overwrites or deletes an
alarm a human (or another tool) created at the same name. There's no
`adopt` escape hatch for that conflict specifically, unlike every other
resource type — given alarms are pure derived config with next to no
blast radius, an unresolvable naming collision isn't worth a whole
adoption workflow.

## IAM

Auto-derived — there's no `spec.iam` section to write. The controller
computes least-privilege policy from whatever this CR owns plus whatever it
`consumes` (and has actually been granted via the producer's `sharedWith`),
and reconciles one IAM role per CR:

```yaml
spec:
  serviceAccountName: checkout-worker   # optional; see below
  sqs:
    resources:
      - name: orders
  dynamodb:
    resources:
      - name: sessions
    consumes:
      - namespace: platform
        name: shared-cache
        resourceName: sessions
```

- **Owned resources always get full access**, regardless of `sharedWith` —
  sharing controls what *other* CRs can do with a resource, never what its
  own owner can do.
- **Consumed resources are gated on both sides**: a `consumes` entry alone
  grants nothing — the target resource's owner must list this CR (by
  namespace+name) in that resource's `sharedWith`, and the level of access
  (`ReadOnly` or `ReadWrite`) is the *owner's* choice via `sharedWith[].access`,
  never the consumer's. `ReadOnly` still includes whatever's mechanically
  required to consume at all (`sqs:DeleteMessage`, `sns:GetTopicAttributes`, ...);
  `ReadWrite` adds the ability to write new data (`sqs:SendMessage`,
  `sns:Publish`, `s3:PutObject`, `dynamodb:PutItem`, ...). A `consumes` entry
  with no matching `sharedWith` grant is reported as unauthorized rather
  than silently dropped or silently honored. A topic's consumers can't
  subscribe endpoints themselves: they receive its messages through
  `subscribesTo`, which the operator removes when access is revoked.
- **Role naming and trust policy**: one role per CR, trust-scoped via IRSA
  (`sts:AssumeRoleWithWebIdentity`) to `system:serviceaccount:<namespace>:<serviceAccountName>`
  using the cluster's OIDC provider. Both
  `--oidc-provider-arn` and `--oidc-provider-url` must be set on the manager
  (see the README prerequisites); without them a CR that needs a role fails
  with `IAMReady: False`. Same ownership tagging, adoption (`adopt: true`), and
  drift-correction rules as every other resource type — including for the
  trust policy document itself, which IAM returns URL-encoded and must be
  decoded before comparing against the freshly-computed one, or every
  reconcile would look like drift.
- **`status.iamRoleARN`** reports the role's ARN directly (also present as
  an `iam`/`role` entry in `status.managedResources`).
- **Cleanup**: the role (and its inline policy) is deleted when no longer
  needed — CR deletion, or the CR no longer owning or consuming anything —
  following the same deletion-safety ledger as other resources. No quiet
  window, unlike SQS/SNS/DynamoDB/S3: a role holds no data, so there's
  nothing to lose by deleting it promptly. If something else is attached to
  the role (a managed policy added by hand, an instance profile), AWS
  refuses the delete: the CR shows `DeletionBlocked` with
  `RoleHasExternalAttachments` naming them, the operator leaves them alone,
  and the deletion completes once they are detached.
- **Reconcile propagation**: a producer's `sharedWith` change (or its
  deletion) triggers an immediate reconcile of every consumer CR that
  references it, so access is granted or revoked without waiting on that
  consumer's own next periodic reconcile.
- **ServiceAccount and connection ConfigMap**: the role ARN goes on
  `spec.serviceAccountName`, or on an operator-owned ServiceAccount named
  after the CR (`status.serviceAccountName` says which), and
  `<cr-name>-connection` carries env-var-shaped keys for every owned and
  authorized consumed resource (it exists for as long as the CR does, empty when
  there is nothing to report, so a pod that reads it never fails to start for lack
  of it; a CR name is limited to 237 characters so the generated names fit); if two resources would map to the same key
  (`Orders` and `orders`, `a-b` and `a_b`), `ConnectionInfoReady` fails with
  `ConnectionKeyCollision` naming both, and nothing is written. A named
  ServiceAccount must already exist
  (`ServiceAccountNotFound` otherwise). One this operator did not create is
  only taken over once it's annotated `cloudctl.io/allow-claim: <cr-name>`
  (several CRs: comma separated), whether or not it has a role already, since
  every pod running as it gets the CR's AWS permissions. Without that,
  `ServiceAccountNotClaimable`. The namespace's `default` ServiceAccount is
  never taken, consent or not: every pod that names no ServiceAccount runs as
  it. See
  [architecture.md](architecture.md#resource-identity-delivery-to-workloads).

## Moving to a new cluster, or restoring a CR

A recreated CR gets a new UID, and its resources still carry the old one, so every resource is refused ("tagged with this CR's name but a different UID"), even with `adopt: true`. Two ways through:

- **A planned move, with the old cluster healthy.** Set every resource to `deletionPolicy: Retain` and delete the CRs: retained resources are handed back with their ownership tags removed, while the IAM role, alarms, ConfigMap and Secret are deleted. Apply the CRs in the new cluster with `adopt: true` on each resource, and remove it once they are Ready.
- **A restore, a lost cluster, or an old cluster you can't tear down.** Stop the old cluster's operator, find the old UID (in a backup of the CR, or the `cloudctl.io/owner-uid` tag on any of its resources), and annotate the new CR with `cloudctl.io/adopt-from-uid: <old-uid>`. Every resource tagged with that UID is re-tagged to the new one and an `OwnershipTransferred` event is recorded for each; remove the annotation afterwards. A resource carrying any other earlier UID is still refused.

Never run two operators against the same resources: after the transfer, the old operator fails its own ownership check and stops touching them, but until then they would fight.

A move to another **region** is not supported: an operator refuses a CR whose resources are in a different region (`RegionChanged`) and touches nothing, so run an operator in the CR's own region. A CR in deletion that cannot be reached that way is released, with its resources left in AWS as they are and listed in a `ResourcesAbandoned` event, by annotating it `cloudctl.io/abandon-other-region: "true"`.

## Shared behavior

- **Plain HTTP is refused.** Every queue, topic and bucket the operator manages
  gets a policy statement, `cloudctl-deny-insecure-transport`, that denies any
  request made without TLS. SDKs use TLS by default, so this only affects a
  client configured for plain HTTP. It is added on the next pass to resources
  that already exist, adopted ones included (an event, `TransportSecurityEnforced`,
  says so), and it stays on a resource the operator lets go of. A bucket that
  hosts a website is exempt, because the website endpoint is HTTP only: it gets a
  `TransportSecurityNotEnforced` warning instead.
- **Buckets are private, and the operator does not make one public.** Content
  for the internet belongs behind CloudFront with origin access control, which
  the operator leaves alone: it never touches a policy statement it did not
  write. A bucket that is open to the public through its policy or ACL, after
  Block Public Access is taken into account, gets a `BucketPubliclyAccessible`
  warning on each verification. Nothing is changed.
- **`kubectl get appdeps`** shows `Ready`, and the aggregate condition's
  `Reason`, which is the first section that isn't ready, in its own words
  (`-o wide` adds the message). Every section's condition is under
  `status.conditions`; [reasons.md](reasons.md) lists every reason.
- **What happens on delete** — each ledger entry carries `onDelete`:
  `Retain` (left in AWS, ownership tags removed), `Delete` (deleted once
  empty, after the quiet window), `DeleteEvenIfNotEmpty` (`force: true`) or
  `ManualDeletion` (an RDS instance, which the operator never deletes). A KMS
  key entry lists in `heldBy` the entries that still need it, up to 20.
- **Released tags** — when the operator lets go of a retained resource it
  leaves `cloudctl.io/released-from=<namespace>/<cr-name>` and
  `cloudctl.io/released-at=<date>` on it, so a resource nobody remembers can be
  traced: `aws resourcegroupstaggingapi get-resources --tag-filters Key=cloudctl.io/released-from`.
  They are never used as ownership, and adopting the resource again removes
  just those two. A failure to write them (an S3 bucket allows ten tags) is
  reported as `ReleaseTagFailed` and does not stop the release. Turn them off
  with `--release-tags=false` (chart: `manager.releaseTags: false`).
- **Cost reporting** — every resource carries `cloudctl.io/owner=<namespace>/<cr>`, and `cloudctl.io/released-from` once the operator has let go of it. Activate both as cost allocation tags in AWS Billing to group spend by CR and to see what abandoned resources cost. The operator computes no prices.
- **Ownership tagging, adoption, and the trust window** — see
  [architecture.md](architecture.md#ownership-a-resource-is-owned-by-exactly-one-cr).
- **Deletion safety, the quiet window, and `PendingDeletion`/`StuckPendingDeletion`** —
  see [architecture.md](architecture.md#deletion-is-a-lifecycle-not-a-single-api-call).
- **Emptiness-check backoff** — once a resource has sat `PendingDeletion`
  for more than a day, the real "is it empty yet" AWS call (message count,
  subscriptions, bucket objects, table rows) backs off exponentially from
  10 minutes up to a 20-hour ceiling, rather than firing on every
  reconcile forever. Protects the shared per-service AWS rate-limit budget
  from a fleet of long-neglected, never-emptied resources; a resource
  actively being drained within that first day is never affected. Doesn't
  apply to KMS, which has no emptiness concept at all (see below).
- **Status conditions** — each section reports its own condition
  (`SQSReady`, `SNSReady`, ...) plus an aggregate `Ready` condition on the CR.
  Every section's error/refusal path (adoption refused, deletion refused for
  an ownership mismatch, a naming collision, ...) also fires a matching
  `Warning` Kubernetes Event via the same recorder success paths already
  use, so `kubectl describe appdependencies <name>` surfaces it directly
  instead of requiring a `status.conditions` read.
- **Forcing a reconcile** — metadata-only changes (labels, arbitrary
  annotations) never trigger one; only a real `spec` change or the periodic
  drift-detection timer do. The one documented exception is the exact
  annotation key `cloudctl.io/force-reconcile` — changing its value
  triggers an immediate reconcile. Any other annotation key (including a
  plausible guess like a bare `force-reconcile`) is silently ignored, with
  no error or event.
- **`SharedWithReferencesValid`** — flags any `sharedWith` entry whose
  target consumer CR no longer exists (typically because it was deleted
  after being granted access). A consumer that could not be read leaves it
  `Unknown`. Deliberately excluded from the `Ready`
  aggregate — a stale reference grants nothing on its own, so it's a
  cleanliness signal, not a reconcile failure — and deliberately
  status-only: `sharedWith` is user-authored spec, and silently pruning it
  would fight a GitOps-managed manifest on its next sync rather than
  actually resolving anything.
- **`ConsumeReferencesValid`** — the consumer-side mirror. It checks every
  reference this CR makes to another CR's resource: each section's
  `consumes`, `rds.consumes`, a queue's `subscribesTo` and
  `alarms.snsTopicRef`. A reference resolves when the CR exists, declares the
  resource, and lists this CR in that resource's `sharedWith`; the reason says
  which of the three is missing (`ProducerNotFoundYet`,
  `ConsumedResourceNotDeclared`, `ConsumeNotAuthorized`) and the message names
  the field. A CR that could not be read leaves the condition `Unknown`
  (`ReferencesUnknown`), never `True`. Excluded from `Ready`: a reference to
  something not yet applied resolves on its own, and an `sqs`, `sns`,
  `dynamodb` or `s3` entry already keeps `IAMReady` from going `True`, so this
  condition exists for visibility. A producer still missing after 30 minutes
  escalates to `ProducerLikelyMisconfigured`, since at that point it is more
  likely a typo than a startup-ordering race.

### Lifecycle comparison across resource types

| | SQS | SNS | DynamoDB | S3 | KMS | RDS |
|---|---|---|---|---|---|---|
| Create + tag atomic? | Yes | Yes | Yes | Yes | Yes | Yes |
| "Non-empty" signal | message count (3 counters) | active subscriptions | item count (count-only `Scan`) | any object version/delete marker | *(no concept — see below)* | *(none — the operator never deletes an instance)* |
| Async create lifecycle | No | No | Yes (`Creating`→`Active`) | No | No | Yes (minutes; status-driven) |
| `force` bypasses non-empty guard | Yes | Yes | Yes | Yes | No — deletion is irreversible for anyone else still using the key, so there's nothing to force | No — there is no delete to force |
| Deny-policy on `PendingDeletion` | Yes | Yes | Yes (writes that add data; `DeleteItem` stays open to drain it) | Yes | N/A — uses `ScheduleKeyDeletion`'s own AWS-side wait instead | N/A — the operator never deletes an instance |
| `StuckPendingDeletion` escalation (7-day grace) | Yes | Yes | Yes | Yes | No — no self-imposed grace period; AWS's own 30-day `ScheduleKeyDeletion` window is the wait | Yes, after 1 hour, and it is permanent: the operator never deletes |
| Emptiness-check backoff applies | Yes | Yes | Yes | Yes | No (nothing to back off) | No |
