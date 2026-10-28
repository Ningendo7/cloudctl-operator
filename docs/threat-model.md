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

**Adoption safety.** Every AWS resource type (SQS, SNS, S3, DynamoDB, RDS,
KMS, IAM, alarms) refuses to touch a same-named resource unless its ownership tag
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

## Who may create CRs

Nobody can until an admin binds a role, so bind it per team with a
`RoleBinding` (not a `ClusterRoleBinding`) to `appdependencies-editor-role`
in that team's namespace. Whoever can create a CR there can, with the
operator's AWS credentials:

- **Spend:** any `instanceClass` matching the CRD pattern, storage up to
  64 TiB, provisioned DynamoDB, and any number of CRs.
- **Delete their own data:** `deletionPolicy: Delete` with `force: true`
  empties and deletes their buckets.
- **Squat names**, as described under "Accepted" below.

Bound these with what Kubernetes already provides, since the limits depend on
your organisation:

- [config/samples/policy/](../config/samples/policy/appdependencies-guardrails.yaml)
  is a `ValidatingAdmissionPolicy` (Kubernetes 1.30 or later, tried on 1.36)
  that allows CRs only in namespaces labelled `cloudctl.io/enabled=true`,
  and caps RDS instance classes and storage. Edit its lists to your own. It
  judges creates and spec changes only, so a CR can always be deleted: the
  operator's finalizer update changes no spec and is never denied.
- `ResourceQuota` caps the CRs per namespace:
  `count/appdependencies.deps.cloudctl.io: "10"`.
- The operator's IAM role can carry conditions, such as
  `rds:DatabaseClass` and a region, so a mistake above still has a ceiling.

## Fixed

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

**RDS delete path acted on unverified ownership.** Cleanup of a removed `Delete`-policy instance took a final snapshot of whatever sat at the ledger's ARN, and later deleted that snapshot, without re-reading ownership tags (the snapshots weren't tagged at all). Instance names are deterministic, so a replaced or re-tagged instance would have been snapshotted as if it were ours. Each pass now re-reads the instance's tags before acting, final snapshots carry the CR's ownership tags, and one is deleted only while it still does.

**Releasing a ServiceAccount stripped its owner reference.** `release()` re-applied an empty object under the operator's field manager, which removed the owner reference along with the role annotation, so a ServiceAccount the operator created was never garbage-collected once its CR was deleted. It now keeps an owner reference it set. Only a real cluster could show this, since envtest has no garbage collector.

**A v0.2.0 `kmsKeyRef` object blocked the whole cache.** See "Upgrading from v0.2.0" in [resources.md](resources.md#kms). Found by running the real CRD upgrade against an API server.

## Accepted, documented, not fixed

**Resource-name-squatting DoS.** Since resource names are fully
deterministic, an attacker who can predict another CR's exact
`(namespace, crName, resourceType, key)` — and who already has *some*
CR-create rights somewhere in the cluster — could pre-create and
self-tag a same-named AWS resource first, blocking the legitimate CR from
provisioning. This already surfaces as a refused-collision error rather
than a silent takeover (see Adoption safety above), so the blast radius is
availability, not confidentiality or integrity. Same severity class as the
IAM propagation-lag gap below — worth revisiting if it ever causes real
pain, not worth ledger-state machinery today.

**Grants are by name, not UID.** A `sharedWith` entry names a consumer by namespace and CR name, and is never pruned when that CR is deleted. A CR created later with the same namespace and name is authorized at once. Creating it needs create rights in that namespace, which the producer already trusted by naming it, but remove an entry you no longer want.

**IAM role propagation lag** — see [aws-assumptions.md](aws-assumptions.md);
unfixable from this operator's own reconcile loop, tracked separately.

**The controller's own AWS-side IAM boundary.** What the manager's own
execution role may do in AWS is deployment configuration, not something
this repo can enforce in code. The API calls it makes are listed below, so
an installer can build the narrowest policy for the services they use.

### AWS actions the manager's role needs

Grant only the services you declare in CRs. Every resource name is derived
from the CR, so each action can be scoped by resource ARN prefix; tag
conditions (`aws:ResourceTag/cloudctl.io/owner`) are worth adding for the
destructive ones, so leaked credentials can't touch resources this operator
didn't create.

These are IAM action names without the service prefix, so `CreateQueue` under
SQS is `sqs:CreateQueue`. Several S3 actions differ from the API operation
that needs them (`HeadBucket` needs `s3:ListBucket`, and the lifecycle and
encryption calls use `...Configuration` action names). A test keeps this table
in step with the operations the code can call.

| Service | Actions |
|---|---|
| STS | `GetCallerIdentity` needs no permission |
| SQS | `CreateQueue`, `GetQueueUrl`, `GetQueueAttributes`, `SetQueueAttributes`, `ListQueueTags`, `TagQueue`, `UntagQueue`, `DeleteQueue` |
| SNS | `CreateTopic`, `GetTopicAttributes`, `SetTopicAttributes`, `ListTagsForResource`, `TagResource`, `UntagResource`, `DeleteTopic`, `ListSubscriptionsByTopic`, `Subscribe`, `Unsubscribe` |
| S3 | `CreateBucket`, `ListBucket`, `DeleteBucket`, `GetBucketTagging`, `PutBucketTagging`, `GetBucketVersioning`, `PutBucketVersioning`, `GetLifecycleConfiguration`, `PutLifecycleConfiguration`, `GetEncryptionConfiguration`, `PutEncryptionConfiguration`, `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy`, `GetBucketWebsite`, `GetBucketPolicyStatus`, `GetBucketPublicAccessBlock`, `GetBucketAcl`, `ListBucketVersions`, `DeleteObject`, `DeleteObjectVersion`, `ListBucketMultipartUploads`, `AbortMultipartUpload`, `TagResource` (docs, verify: creating a bucket with tags) |
| DynamoDB | `DescribeTable`, `CreateTable`, `DeleteTable`, `UpdateTable`, `ListTagsOfResource`, `TagResource`, `UntagResource`, `DescribeContinuousBackups`, `UpdateContinuousBackups`, `Scan` (emptiness check), `GetResourcePolicy`, `PutResourcePolicy`, `DeleteResourcePolicy` (the write-blocking policy on a table waiting to be deleted) |
| KMS | `CreateKey`, `DescribeKey`, `CreateAlias`, `EnableKey`, `EnableKeyRotation`, `GetKeyRotationStatus`, `GetKeyPolicy`, `PutKeyPolicy`, `ListResourceTags`, `TagResource`, `UntagResource`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `CreateGrant` (docs, verify: RDS and DynamoDB create a grant when encrypting with a customer key) |
| IAM | `GetRole`, `CreateRole`, `UpdateAssumeRolePolicy`, `ListRoleTags`, `GetRolePolicy`, `PutRolePolicy`, `DeleteRolePolicy`, `DeleteRole`, `ListAttachedRolePolicies`, `ListInstanceProfilesForRole`, `TagRole` |
| CloudWatch | `DescribeAlarms`, `PutMetricAlarm`, `DeleteAlarms`, `ListTagsForResource`, `TagResource` (docs, verify: alarms are created with tags) |
| RDS | `CreateDBInstance`, `ModifyDBInstance`, `DescribeDBInstances`, `DescribeDBSubnetGroups`, `DescribeDBEngineVersions`, `ListTagsForResource`, `AddTagsToResource`, `RemoveTagsFromResource`, `CreateDBSnapshot`, `DescribeDBSnapshots`, `DeleteDBSnapshot` (never `DeleteDBInstance`) |
| EC2 (RDS security groups) | `CreateSecurityGroup`, `DescribeSecurityGroups`, `AuthorizeSecurityGroupIngress`, `RevokeSecurityGroupIngress`, `DeleteSecurityGroup`, `CreateTags` |
| Secrets Manager (RDS) | `GetSecretValue`, scoped to the `rds!db-*` secrets AWS creates |

The RDS rows were exercised against real AWS with a broadly-privileged
user, so whether `ManageMasterUserPassword` needs further actions on the
caller (for example KMS permissions for the secret's key) was not verified
under a least-privilege policy; test yours before relying on it.
