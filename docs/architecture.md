# Architecture

This document explains the design decisions behind cloudctl-operator — why it
looks the way it does, not just what it does. See the [README](../README.md)
for a quickstart, and [resources.md](resources.md) for what's actually
implemented today.

## Why one CRD instead of exposing each AWS resource directly

Provisioning SNS, SQS, S3, DynamoDB, and the IAM to go with them by hand (or
via raw Terraform) means IAM policy gets hand-authored per app — error-prone
and inconsistent — and operational hygiene (alarms, backups, replication) is
opt-in and frequently skipped.

`AppDependencies` gives app teams a single, opinionated CRD to declare *what
their app needs*, with the controller deriving IAM, monitoring, and
resiliency configuration from that declaration. The schema encodes
*decisions* (`backup: {enabled: true}`), not raw *configuration* (lifecycle
rule JSON, replication role ARNs) — deliberately avoiding the
"Terraform-in-YAML" trap where a CRD just becomes a worse YAML dialect for
the same underlying provider config. `status` reports the concrete result
(resource names/ARNs, computed IAM policy, ...) so the opinionated defaults
never become a black box, and each section has an `overrides` escape hatch
for tuning specifics beyond the default.

## Ownership: a resource is owned by exactly one CR

Every resource this operator creates gets an ownership tag
(`cloudctl.io/owner`, plus the owning CR's UID) at creation. Two rules follow
from that:

- **Existence by deterministic name is never treated as ownership.** If a
  resource already exists under the name this CR would have used, but isn't
  tagged as owned by this CR (or is tagged as owned by a *different* CR), the
  controller refuses to touch it and surfaces a status condition instead of
  silently adopting or failing destructively. Bringing an existing resource
  under management requires an explicit `adopt: true` on that entry — a
  normal spec field, so it's a visible, deliberate, auditable change, not an
  implicit side effect of a name collision.
- **A trust window bounds how long ownership is trusted without
  re-checking.** A resource verified as owned within the last 10 minutes
  skips the AWS round trip to re-verify on every single reconcile — but only
  for read-level trust. Any destructive action, and always a delete, still
  requires the freshest tag check available before acting; cached trust
  alone never authorizes destroying something. The window exempts the
  ownership check only: every reconcile still confirms an SQS queue, SNS
  topic, S3 bucket, DynamoDB table, RDS instance or KMS key exists. A queue,
  topic or bucket deleted out-of-band is reported (a `*Missing` warning
  event) and recreated on the next pass, not after the window lapses. RDS
  instances and KMS keys are the exception by design: a missing one is an
  error, never silently replaced, since a new instance would be empty and
  everything encrypted under a lost key is already unrecoverable. A KMS key
  deletion scheduled by someone else is cancelled on the same pass while
  the key is still declared.

## Deletion is a lifecycle, not a single API call

Every resource's `deletionPolicy` defaults to `Retain`, mirroring
Kubernetes' own PersistentVolume reclaim policy: removing a resource from
spec (or deleting the CR) leaves the AWS-side resource alone by default,
surfaced as orphaned-but-retained rather than silently destroyed.
`deletionPolicy: Delete` is opt-in per resource.

Even under `Delete`, deleting something with real state in it (unread SQS
messages, active SNS subscriptions, non-empty S3 buckets) is blocked unless
`force: true` is set. A resource that's blocked this way is marked
`PendingDeletion` and denied new writes (a resource-policy `Deny` statement,
which beats any `Allow` from any source — the operator's own derived IAM
included) so it can drain safely rather than accumulating more state while
stuck. If it's still not empty after 7 days, it escalates to
`StuckPendingDeletion` — surfaced for a human, never auto-force-deleted, since
silently destroying data because a clock ran out would be worse than the
problem this is meant to catch.

Blocked deletion also isn't just about the current read: `GetQueueAttributes`
(SQS) and `ListSubscriptionsByTopic` (SNS) carry no documented consistency
guarantee, so a resource that looks empty *right now* could have had activity
land moments before this reconcile that hasn't propagated into that read
yet. The first time any resource comes up for deletion, it's unconditionally
held — denied and marked pending — for a short quiet window before its
emptiness is trusted at all, independent of what that first check shows.

A CR stuck mid-deletion this way is never silent about it: a dedicated
`DeletionBlocked` status condition names exactly which resources are still
holding things up, so this is visible from `kubectl get`/`describe`
without reading `metadata.deletionTimestamp` by hand. Once a resource that
blocked deletion has already left spec (so there's no `force` field left
to flip to `true` on it), the `cloudctl.io/force-delete-all` annotation on
the CR itself is the recovery path — checked at finalize time independent
of spec, so a resource with no remaining spec entry to carry `force` isn't
a permanent dead end.

## Naming

Every resource name is deterministic, so it can be recomputed at any time
without stored state: a readable prefix,
`<namespace>-<cr-name>-<resource-key>` (cleaned of characters the service
rejects), then `-` and a 12-character hash of the full `(namespace, cr-name,
resource type, key)` tuple. Hashing the tuple, rather than joining its parts,
means two identities can't land on one name even when a `-` appears inside
one of them. Only the prefix is ever truncated to fit a service's length
limit, never the hash, so the CRD's `MaxLength` on the key doesn't have to
bound the final name. FIFO queues and topics get AWS's required `.fifo`
suffix. S3 bucket names, which are unique across every AWS account, also get
an 8-character hash of the account ID.

## Resource identity delivery to workloads

Two distinct delivery problems, deliberately not solved by the same
mechanism.

**The IAM role → a ServiceAccount annotation.** With `spec.serviceAccountName`
set, the operator merges `eks.amazonaws.com/role-arn` onto that existing
object through server-side apply, claiming only that field and leaving the
object and its other annotations alone. It does so only for a ServiceAccount
whose owner consented with `cloudctl.io/allow-claim: <cr-name>`, never for
`default`, because every pod running as the account gets the role. Unset, it creates and owns a
ServiceAccount named after the CR, garbage-collected with it. The AWS SDK
in the pod assumes the role through its projected token, so no credential
is ever shipped in an env var or ConfigMap, and the role ARN is deliberately
not in the ConfigMap either.

**Resource identifiers → a generated ConfigMap**, `<cr-name>-connection`,
regenerated by server-side apply on every reconcile and deleted once there
is nothing to report. Keys are flat and env-var shaped (`SQS_ORDERS_URL`,
`S3_RECEIPTS_BUCKET`, `DYNAMODB_SESSIONS_TABLE_NAME`, `SNS_EVENTS_ARN`) for
`envFrom: configMapRef`.

A `consumes` entry is mirrored into the consumer's own ConfigMap, with the
producer's namespace and CR name in the key
(`SQS_<PRODUCER_NAMESPACE>_<PRODUCER_CR_NAME>_ORDERS_URL`), since bare CR
names can collide across namespaces. Mirroring is the only option, because
a pod cannot mount a ConfigMap from another namespace. It uses the same
authorization check as IAM policy derivation (`iam.ResolveConsumeARN`), so
an entry the producer hasn't granted is left out of both, and a workload is
never handed an identifier its role can't use.

**RDS credentials (master username/password) → a generated Secret**,
named `<cr-name>-rds-credentials`, with the same ownership, regeneration and
deletion rules as the ConfigMap above. It is re-derived from the secret RDS
keeps in Secrets Manager on every reconcile; this operator never generates or
caches a password, so Secrets Manager stays the sole source of truth. It is
consumed through `envFrom: secretRef`, which is where the guarantee runs out:
a container's environment is fixed at start, so after a rotation the Secret is
correct within one drift-detection interval but a running pod keeps its old
password until something restarts it. The operator deliberately doesn't do that
restart, since it would need write access to workload resources it touches
nowhere else; [resources.md](resources.md#rds) lists the options. IAM database
authentication would remove the problem at the root (a signed, short-lived
token per connection, nothing to rotate) but needs app-side changes this
operator can't make, so it remains a candidate for a future `authMethod`
option.

## Validation: CEL schema rules, not an admission webhook

Self-contained constraints (immutability of fields AWS doesn't support
changing post-creation, like `fifo`; a field required only if another is
set) are enforced via native CRD CEL validation (`x-kubernetes-validations`)
directly in the schema — no webhook server, no TLS certificate lifecycle, no
"webhook is down and blocks every apply of this CRD" failure mode.

Cross-object constraints (does a referenced producer resource exist, is a
consumer on the producer's `sharedWith` list) are deliberately left to
reconcile-time status conditions instead. A webhook can't safely gate these:
GitOps can apply a consumer and its producer CR in the same batch with no
guaranteed order, so rejecting a consumer because its producer "doesn't
exist yet" (but is about to) would be flaky, order-dependent behavior. This
mirrors how Kubernetes already treats other forward references — an Ingress
pointing at a not-yet-created Service isn't rejected at admission, it just
doesn't route until the Service shows up.

## AWS client handling and rate limiting

One shared client per AWS service, built once at manager startup
(`internal/aws.NewClients`) and reused for the process lifetime — not
per-reconcile, since client construction resolves credentials (an STS call
under IRSA) and builds the retry/middleware stack, and doing that on every
tick would add latency to every reconcile and could itself hammer STS
under load.

**Startup credential failure retries rather than crash-loops.**
`NewClients` fails if the STS identity check fails (bad/revoked/rotated-out
credentials) — but `cmd/main.go`'s `waitForAWSClients` retries this on a
fixed interval instead of exiting the process. The manager (and its health
probe port) isn't started until this succeeds, so a pod stuck in this state
isn't crash-looping — it just has nothing listening yet, the same posture
already confirmed for a real AWS network partition at startup. This matters
specifically for a credentials outage that affects every replica at once
(e.g. mid-rotation): without this, every *new* pod started during that
window would crash-loop and the whole deployment could end up at zero
`Ready` replicas; an already-`Running` pod on already-valid cached
credentials is unaffected either way. This doesn't add periodic
re-validation for an already-started manager — only the startup path.

Each service also gets its own independent outgoing rate limiter
(`internal/aws/ratelimit.go`), attached as SDK middleware on that service's
client alone rather than a budget shared across services. Sharing one
budget would mean the tightest service (IAM, whose real limits are
considerably stricter than SQS/SNS/S3/DynamoDB/CloudWatch's) throttles the
others down to its own ceiling for no reason tied to their actual
capacity. This paces requests *before* they're sent, independent of and in
addition to each client's own SDK-level retry behavior, which only reacts
after AWS has already started throttling. Defaults are deliberately
conservative starting points, not verified figures for any specific
account's real limits — tunable per service via
`AWS_<SERVICE>_RATE_LIMIT_QPS`/`_BURST` env vars.

A freshly-elected leader can have a large backlog of CRs to reconcile all
at once (informer cache sync, then the workqueue populating) — exactly
when a real burst past a service's limits is most likely. Right after
`mgr.Elected()`, every limiter ramps from 10% of its configured target up
to that target over 30 seconds, rather than starting at full burst into a
cold backlog.
