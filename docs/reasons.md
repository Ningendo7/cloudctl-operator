# Condition reasons

Generated from `internal/reasons`; do not edit by hand. Regenerate with
`go test ./internal/reasons -run Doc -update`.

A condition's `reason` says what is wrong. *Waiting* clears by itself, *Failing* needs a
person, *Info* is healthy. `Ready` carries the first section that is not ready,
with that section's own reason. A failing section also records a Warning event under
the same reason, with the section at the start of its text.

| Reason | Kind | Meaning | What to do |
|---|---|---|---|
| `Reconciled` | Info | The section reconciled successfully. | — |
| `AllSectionsReady` | Info | Every declared section is ready. | — |
| `SectionsNotReady` | Waiting | A declared section has no condition yet, or has not run for the current generation. | Wait for the next reconcile. |
| `TransientError` | Waiting | A retryable failure, such as throttling or AWS not having caught up. It is retried with backoff, and about every 5 minutes after 10 failures in a row. | None, unless it lasts; then read the message. |
| `Error` | Failing | The section failed with an error the operator does not classify. | Read the message, which carries the cause, and the operator's logs. |
| `PermissionDenied` | Failing | AWS refused a call made with the operator's credentials. | Grant the operator's IAM role the action named in the message. |
| `OwnedByAnotherCR` | Failing | A resource at the needed name is tagged as owned by a different CR. | Rename one of the two; the operator never takes a resource from another CR. |
| `PreviousIncarnation` | Failing | A resource carries this CR's name but the UID of an earlier CR with the same name, as after a restore or a move. | If this CR replaces it, annotate the CR with cloudctl.io/adopt-from-uid=<uid in the message>, and remove the annotation afterwards. |
| `AdoptRequired` | Failing | A resource at the needed name exists and is not tagged as owned by anyone. | Set adopt: true on the entry to bring it under management. |
| `NameTaken` | Failing | A ConfigMap, Secret or alarm that this operator did not create sits at the name it needs. It is never overwritten. | Rename or delete the existing object. |
| `OwnershipLost` | Failing | The ownership tags were re-read before acting and no longer match this CR, so the operator did nothing. | Restore the cloudctl.io/owner and cloudctl.io/owner-uid tags, or deal with the resource yourself. |
| `AdoptionConflict` | Failing | Adoption was refused because the existing resource differs from the spec in a way that cannot be changed, such as an RDS engine or an encryption key. | Make the spec match what exists, adopt it, then change the spec. |
| `ImmutableSettingChanged` | Failing | The spec changed a setting AWS cannot change on an existing RDS instance: the engine, the subnet group, or storage encryption. | Set it back, or move the data to a new entry. |
| `FIFOChanged` | Failing | The spec asks for a different queue or topic type than the one that exists; fifo cannot change. | Set fifo back, or remove the entry, wait for it to be deleted and add it again. |
| `InstanceMissing` | Failing | The ledger knows an RDS instance that AWS no longer has. The operator does not create an empty replacement. | Restore it under the same identifier and set adopt: true, or remove the entry and add it again. |
| `TableMissing` | Failing | The ledger knows a DynamoDB table that AWS no longer has. The operator does not create an empty replacement. | Restore it from a backup under the same name and set adopt: true, or remove the entry and add it again. |
| `InstanceFailed` | Failing | AWS reports the RDS instance as failed. It is not retried. | Investigate the instance in AWS. |
| `InstanceStuckPendingDeletion` | Failing | An RDS instance was removed from spec with deletionPolicy Delete. The operator never deletes an instance. | After the final snapshot is available, turn off deletion protection and delete it yourself, or add it back to spec. |
| `RoleHasExternalAttachments` | Failing | AWS refused to delete the IAM role because something other than this operator is attached to it. | Detach it, or delete the role yourself. |
| `LegacyKMSKeyRef` | Failing | The CR uses the object form of kmsKeyRef from v0.2.0. | Follow Upgrading from v0.2.0 in docs/resources.md. |
| `KMSKeyRefNotDeclared` | Failing | encryption.kmsKeyRef names a key that is not declared under kms.resources. | Declare the key, or correct the name. |
| `KeyDisabled` | Failing | The KMS key is disabled, so nothing encrypted with it can be read or written. The operator does not re-enable it. | Re-enable the key if that is intended, or point the resource at another key. |
| `KeyUnavailable` | Failing | The KMS key is in a state that cannot encrypt or decrypt. | Check the key in AWS. |
| `AllProducersExist` | Info | Every consumes entry names a CR that currently exists. | — |
| `AllReferencesValid` | Info | Every sharedWith entry names a CR that currently exists. | — |
| `StaleReferencesFound` | Waiting | A sharedWith entry names a CR that does not exist, either not created yet or deleted. Nothing is granted to it. | Create the consumer, or remove the entry from sharedWith. |
| `ProducerNotFoundYet` | Waiting | A consumes entry names a CR that does not exist yet. This is normal while a producer and consumer are applied together. | Apply the producer. After 30 minutes it is reported as ProducerLikelyMisconfigured. |
| `ProducerLikelyMisconfigured` | Failing | A consumes entry has named a CR that does not exist for over 30 minutes. | Check the namespace and name in the consumes entry. |
| `ServiceAccountNotFound` | Waiting | spec.serviceAccountName names a ServiceAccount that does not exist. | Create it, or leave the name unset to have one created. |
| `ServiceAccountNotClaimable` | Failing | The ServiceAccount already has an IAM role set outside this operator. | Annotate it with cloudctl.io/allow-claim=<cr-name> to hand it over. |
| `ServiceAccountInUse` | Failing | The ServiceAccount is already claimed by a different CR. | Use another ServiceAccount. |
| `ConnectionKeyCollision` | Failing | Two sources map to one ConfigMap or Secret key once upper-cased with other characters turned into underscores. | Rename one of the resources. |
| `RegionChanged` | Failing | The CR has resources in a different region than this operator's. Nothing was touched. | Run an operator in that region for it. |
| `WaitingForRuleRelease` | Waiting | A security group cannot be deleted yet because another group's rule or a running pod still uses it. | None; it is retried. |
| `ResourcesNotDrained` | Waiting | Deletion of the CR has not finished: a resource is waiting on its non-empty check or its quiet window. | Empty the resource, or set force: true on it. |
| `DeletionBlocked` | Waiting | Ready is false because deletion of the CR has not finished. | See the DeletionBlocked condition for what is waiting. |
| `Instance…` | Waiting | AWS reports the RDS instance is not available yet, for example InstanceCreating or InstanceModifying. | None; it is polled until it is available. |
