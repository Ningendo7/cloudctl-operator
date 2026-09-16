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
  alone never authorizes destroying something.

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

## Naming

Every resource name is deterministic:
`<namespace>-<cr-name>-<resource-key>`, since Kubernetes already guarantees
namespace+name uniqueness and this operator targets one AWS account/region
per cluster. FIFO resources get AWS's required `.fifo` suffix appended. The
full computed name is validated against each service's real length limit
(80 characters for SQS, 256 for SNS) before any AWS call is made — the
CRD's own field-level `MaxLength` only bounds the user-supplied key, not the
final composed name.

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
