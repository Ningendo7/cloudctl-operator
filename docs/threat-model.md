# Multi-tenant threat model

This operator runs as a single controller with one set of broad AWS
credentials, reconciling `AppDependencies` CRs that may come from many
different teams/namespaces. This doc walks through what a CR (or whoever
can create one) can and can't cause the controller to do on their behalf,
and what's been checked versus what's still an open, accepted gap.

## Defended

**Cross-namespace resource sharing (`sharedWith`/`consumes`).**
`resolveConsume` (`internal/resources/iam/derive.go`) requires the
producer's `sharedWith` list to name the exact consumer CR by
`(namespace, name)` before granting anything, and the access level
(`ReadOnly`/`ReadWrite`) comes only from the producer's own grant — a
consumer can never request more than what's been explicitly shared with
it. `internal/resources/configmap` reuses this exact same check
(`iam.ResolveConsumeARN`) rather than duplicating it, so the generated
ConfigMap can never expose a resource identifier the derived IAM policy
wouldn't also grant access to.

**`serviceAccountName` cross-namespace targeting.** Structurally
impossible, not just conventionally avoided: the field is a bare string,
always resolved against `cr.Namespace` — there's no way to express a
different namespace in the schema at all.

**Resource naming collisions.** `ResourceName` hashes the full
`(namespace, crName, resourceType, key)` tuple (12 hex characters); two
different CRs landing on the same AWS resource name would require an
actual hash collision, not just similar names.

**Adoption safety.** Every AWS resource type (SQS, SNS, S3, DynamoDB, KMS,
IAM) refuses to touch a same-named resource unless its ownership tag
(`cloudctl.io/owner` + `cloudctl.io/owner-uid`) matches this CR exactly.
A name match alone is never treated as ownership — see `IsOwnedBy` in
`internal/aws/ownership.go`.

**End-user RBAC exposure.** The shipped `manager-role` ClusterRole
(`config/rbac/role.yaml`) only grants permissions to the controller's own
ServiceAccount. The scaffolded `appdependencies-{admin,editor,viewer}-role`
ClusterRoles ship with no ClusterRoleBinding, and the CRD carries no
`rbac.authorization.k8s.io/aggregate-to-{admin,edit}` label — so installing
this operator grants nobody CR-create access by default; a cluster admin
has to deliberately bind one of those roles (or their own) to someone.

**ConfigMap naming.** `ConfigMapName(crName)` has no user-settable
override (unlike `serviceAccountName`), so it's 1:1 tied to the CR's own
`(namespace, name)` identity, which Kubernetes already guarantees is
unique — two different CRs can't collide on it structurally.

## Fixed (found during this pass)

**ServiceAccount role-arn collision.** `internal/resources/serviceaccount`
used a fixed server-side-apply field-manager name and unconditional
`client.ForceOwnership`, with no check on who currently owned the target
before overwriting `eks.amazonaws.com/role-arn`. Two CRs in the same
namespace (this field has no cross-namespace form) naming the same
`serviceAccountName` would silently take turns attaching their own IAM
role to it on every reconcile — no error, no conflict, whichever CR
reconciled last won. A pod running as that ServiceAccount would then
silently start assuming a different role than whoever configured it
expected.

Fixed by stamping the same `cloudctl.io/owner`/`cloudctl.io/owner-uid`
markers AWS resources already carry as tags, as plain annotations on the
ServiceAccount, and refusing to overwrite `RoleARNAnnotation` when an
existing marker doesn't match the calling CR's `(namespace, name, UID)`.
A ServiceAccount with no marker yet (human-created, or newly seen) is
still claimable on first use — see `internal/resources/serviceaccount/serviceaccount_test.go`
for the regression coverage, including the "deleted-and-recreated CR reuses
the same name" case (name matches, UID doesn't → still refused).

**ConfigMap overwrite/delete of a foreign object.** `configmap.Ensure`
used `client.ForceOwnership` unconditionally with no check on the existing
object's owner reference — a human-created ConfigMap coincidentally named
`<cr-name>-connection` would get silently overwritten, or deleted outright
if this CR's own connection data was empty. Fixed by checking
`metav1.GetControllerOf` against `cr.UID` before either writing or
deleting, refusing both when it doesn't match. See
`internal/resources/configmap/configmap_test.go`'s
`TestEnsure_PreExistingForeignConfigMap_*` tests.

## Accepted, documented, not fixed

**Resource-name-squatting DoS.** Since resource names are fully
deterministic, an attacker who can predict another CR's exact
`(namespace, crName, resourceType, key)` — and who already has *some*
CR-create rights somewhere in the cluster — could pre-create and
self-tag a same-named AWS resource first, blocking the legitimate CR from
provisioning. This already surfaces as a refused-collision error rather
than a silent takeover (see Adoption safety above), so the blast radius is
availability, not confidentiality or integrity. Same severity class as the
IAM propagation-lag gap already tracked in project memory — worth
revisiting if it ever causes real pain, not worth ledger-state machinery
today.

**IAM role propagation lag** — see [aws-assumptions.md](aws-assumptions.md);
unfixable from this operator's own reconcile loop, tracked separately.

**The controller's own AWS-side IAM boundary.** What the manager's own
execution role is allowed to do in AWS is deployment configuration, not
something this repo can enforce in code. Worth documenting a minimum
required policy for installers as a follow-up, but out of scope for a
code fix.
