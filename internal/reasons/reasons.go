/*
Copyright 2026 Patrick Ajah.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package reasons is the one list of every reason a status condition can
// carry: what it means, whether it clears by itself, and what to do. The
// strings are visible to users and to anything that watches conditions, so
// an existing one is never renamed, only added to.
package reasons

import "strings"

// Kind says whether a reason needs a person.
type Kind string

const (
	// Info reports a healthy state.
	Info Kind = "Info"
	// Waiting clears by itself; the operator keeps retrying.
	Waiting Kind = "Waiting"
	// Failing needs a person to change something.
	Failing Kind = "Failing"
)

// Reason is one entry of the catalogue.
type Reason struct {
	Name    string
	Kind    Kind
	Meaning string
	// Action is what to do about it; empty only for Info.
	Action string
	// Prefix marks a family: every reason starting with Name is covered.
	Prefix bool
	// OwnEvent marks a reason whose place of origin already records an event
	// of the same name, so the controller must not record a second one.
	OwnEvent bool
}

var catalogue []Reason

func register(name string, kind Kind, meaning, action string) string {
	catalogue = append(catalogue, Reason{Name: name, Kind: kind, Meaning: meaning, Action: action})
	return name
}

// ownEvent marks the reason named as already having an event of its own.
func ownEvent(name string) string {
	for i := range catalogue {
		if catalogue[i].Name == name {
			catalogue[i].OwnEvent = true
		}
	}
	return name
}

// EmitsOwnEvent reports whether the code raising the reason also records an
// event under the same name.
func EmitsOwnEvent(name string) bool {
	r, ok := Lookup(name)
	return ok && r.OwnEvent
}

func registerFamily(prefix string, kind Kind, meaning, action string) {
	catalogue = append(catalogue, Reason{Name: prefix, Kind: kind, Meaning: meaning, Action: action, Prefix: true})
}

// All returns the catalogue in the order it is declared.
func All() []Reason {
	return append([]Reason(nil), catalogue...)
}

// Lookup finds the entry for a reason, falling back to a family it belongs to.
func Lookup(name string) (Reason, bool) {
	for _, r := range catalogue {
		if !r.Prefix && r.Name == name {
			return r, true
		}
	}
	for _, r := range catalogue {
		if r.Prefix && strings.HasPrefix(name, r.Name) {
			return r, true
		}
	}
	return Reason{}, false
}

// InstanceStatus is the reason for an RDS instance in the given AWS status,
// such as "InstanceCreating" for "creating".
func InstanceStatus(awsStatus string) string {
	var b strings.Builder
	b.WriteString("Instance")
	for _, word := range strings.FieldsFunc(awsStatus, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
	}) {
		b.WriteString(strings.ToUpper(word[:1]) + word[1:])
	}
	return b.String()
}

// Healthy states.
var (
	Reconciled = register("Reconciled", Info,
		"The section reconciled successfully.", "")
	AllSectionsReady = register("AllSectionsReady", Info,
		"Every declared section is ready.", "")
)

// Unclassified failures.
var (
	SectionsNotReady = register("SectionsNotReady", Waiting,
		"A declared section has no condition yet, or has not run for the current generation.",
		"Wait for the next reconcile.")
	TransientError = register("TransientError", Waiting,
		"A retryable failure, such as throttling or AWS not having caught up. It is retried with backoff, and about every 5 minutes after 10 failures in a row.",
		"None, unless it lasts; then read the message.")
	Error = register("Error", Failing,
		"The section failed with an error the operator does not classify.",
		"Read the message, which carries the cause, and the operator's logs.")
	PermissionDenied = register("PermissionDenied", Failing,
		"AWS refused a call made with the operator's credentials.",
		"Grant the operator's IAM role the action named in the message.")
	EndpointUnreachable = register("EndpointUnreachable", Failing,
		"The operator cannot reach AWS and waiting will not help: the endpoint's name does not resolve, or its certificate is not trusted.",
		"Check AWS_ENDPOINT_URL, any proxy in the way, and the CA bundle the manager trusts.")
)

// Ownership: a resource exists at the name this CR needs.
var (
	OwnedByAnotherCR = register("OwnedByAnotherCR", Failing,
		"A resource at the needed name is tagged as owned by a different CR.",
		"Rename one of the two; the operator never takes a resource from another CR.")
	PreviousIncarnation = register("PreviousIncarnation", Failing,
		"A resource carries this CR's name but the UID of an earlier CR with the same name, as after a restore or a move.",
		"If this CR replaces it, annotate the CR with cloudctl.io/adopt-from-uid=<uid in the message>, and remove the annotation afterwards.")
	AdoptRequired = register("AdoptRequired", Failing,
		"A resource at the needed name exists and is not tagged as owned by anyone.",
		"Set adopt: true on the entry to bring it under management.")
	NameTaken = register("NameTaken", Failing,
		"A ConfigMap, Secret, alarm or KMS alias that this operator did not create sits at the name it needs. It is never overwritten.",
		"Rename or delete the existing object.")
	OwnershipLost = register("OwnershipLost", Failing,
		"The ownership tags were re-read before acting and no longer match this CR, so the operator did nothing.",
		"Restore the cloudctl.io/owner and cloudctl.io/owner-uid tags, or deal with the resource yourself.")
	AdoptionConflict = register("AdoptionConflict", Failing,
		"Adoption was refused because the existing resource differs from the spec in a way that cannot be changed, such as an RDS engine or an encryption key.",
		"Make the spec match what exists, adopt it, then change the spec.")
)

// Resources that changed under the operator.
var (
	ImmutableSettingChanged = register("ImmutableSettingChanged", Failing,
		"The spec changed a setting AWS cannot change on an existing RDS instance: the engine, the subnet group, or storage encryption.",
		"Set it back, or move the data to a new entry.")
	FIFOChanged = register("FIFOChanged", Failing,
		"The spec asks for a different queue or topic type than the one that exists; fifo cannot change.",
		"Set fifo back, or remove the entry, wait for it to be deleted and add it again.")
	KeySchemaMismatch = register("KeySchemaMismatch", Failing,
		"The existing DynamoDB table's partition or sort key differs from the spec. AWS cannot change a table's keys.",
		"Set partitionKey and sortKey to match the table, or remove the entry, wait for the table to be deleted and add it again.")
	ProvisionedCapacityNeeded = register("ProvisionedCapacityNeeded", Failing,
		"The spec switches an existing DynamoDB table from on-demand to provisioned capacity. The operator can only set a placeholder of 5 read and 5 write units, which would throttle a busy table.",
		"Switch the table to provisioned capacity in AWS with the capacity it needs; billingMode: Provisioned then matches.")
	InstanceMissing = ownEvent(register("InstanceMissing", Failing,
		"The ledger knows an RDS instance that AWS no longer has. The operator does not create an empty replacement.",
		"Restore it under the same identifier and set adopt: true, or remove the entry and add it again."))
	TableMissing = ownEvent(register("TableMissing", Failing,
		"The ledger knows a DynamoDB table that AWS no longer has. The operator does not create an empty replacement.",
		"Restore it from a backup under the same name and set adopt: true, or remove the entry and add it again."))
	InstanceFailed = register("InstanceFailed", Failing,
		"AWS reports the RDS instance as failed. It is not retried.",
		"Investigate the instance in AWS.")
	InstanceStuckPendingDeletion = register("InstanceStuckPendingDeletion", Failing,
		"An RDS instance was removed from spec with deletionPolicy Delete. The operator never deletes an instance.",
		"After the final snapshot is available, turn off deletion protection and delete it yourself, or add it back to spec.")
	RoleHasExternalAttachments = ownEvent(register("RoleHasExternalAttachments", Failing,
		"AWS refused to delete the IAM role because something other than this operator is attached to it.",
		"Detach it, or delete the role yourself."))
)

// Encryption keys.
var (
	LegacyKMSKeyRef = register("LegacyKMSKeyRef", Failing,
		"The CR uses the object form of kmsKeyRef from v0.2.0.",
		"Follow Upgrading from v0.2.0 in docs/resources.md.")
	KMSKeyRefNotDeclared = register("KMSKeyRefNotDeclared", Failing,
		"encryption.kmsKeyRef names a key that is not declared under kms.resources.",
		"Declare the key, or correct the name.")
	KeyDisabled = ownEvent(register("KeyDisabled", Failing,
		"The KMS key is disabled, so nothing encrypted with it can be read or written. The operator does not re-enable it.",
		"Re-enable the key if that is intended, or point the resource at another key."))
	KeyUnavailable = ownEvent(register("KeyUnavailable", Failing,
		"The KMS key is in a state that cannot encrypt or decrypt.",
		"Check the key in AWS."))
	EncryptionAsDeclared = register("EncryptionAsDeclared", Info,
		"No resource is encrypted with a KMS key that its spec no longer asks for.", "")
	EncryptionRemovalIgnored = ownEvent(register("EncryptionRemovalIgnored", Failing,
		"A queue, topic, table or bucket is still encrypted with a KMS key, but its spec no longer asks for encryption. The operator never removes encryption, so the resource keeps its key and the spec and AWS differ.",
		"Set encryption back, or remove the entry, wait for it to be deleted and add it again."))
	KeyMissing = ownEvent(register("KeyMissing", Failing,
		"The ledger knows a KMS key that AWS no longer has. Everything encrypted under it is unrecoverable, and the operator does not create a replacement.",
		"Once you have confirmed that is acceptable, remove the entry and add it again to provision a new key."))
)

// Features the operator does not have yet.
var (
	NotSupported = register("NotSupported", Failing,
		"The spec asks for a feature the operator does not implement, such as replication.",
		"Remove the field.")
)

// References to other CRs and objects.
var (
	AllProducersExist = register("AllProducersExist", Info,
		"Every reference to another CR's resource resolves: the CR exists, declares the resource and has listed this CR in sharedWith.", "")
	AllReferencesValid = register("AllReferencesValid", Info,
		"Every sharedWith entry names a CR that currently exists.", "")
	StaleReferencesFound = register("StaleReferencesFound", Waiting,
		"A sharedWith entry names a CR that does not exist, either not created yet or deleted. Nothing is granted to it.",
		"Create the consumer, or remove the entry from sharedWith.")
	ProducerNotFoundYet = register("ProducerNotFoundYet", Waiting,
		"A reference names a CR that does not exist yet. This is normal while a producer and consumer are applied together.",
		"Apply the producer. After 30 minutes it is reported as ProducerLikelyMisconfigured.")
	ProducerLikelyMisconfigured = register("ProducerLikelyMisconfigured", Failing,
		"A reference has named a CR that does not exist for over 30 minutes.",
		"Check the namespace and name in the reference.")
	ConsumedResourceNotDeclared = register("ConsumedResourceNotDeclared", Waiting,
		"A reference names a CR that exists but does not declare the resource, such as a misspelled resourceName.",
		"Check the resourceName in the reference, or declare the resource in the producer.")
	ConsumeNotAuthorized = register("ConsumeNotAuthorized", Waiting,
		"A reference names a resource that exists, but its owner has not listed this CR in its sharedWith. Nothing is granted.",
		"Ask the owner to add this CR to the resource's sharedWith.")
	ReferencesUnknown = register("ReferencesUnknown", Waiting,
		"A referenced CR could not be read, so whether the references resolve is not known. This is not a finding about the references.",
		"None, unless it lasts; then read the message.")
	ServiceAccountNotFound = register("ServiceAccountNotFound", Waiting,
		"spec.serviceAccountName names a ServiceAccount that does not exist.",
		"Create it, or leave the name unset to have one created.")
	ServiceAccountNotClaimable = register("ServiceAccountNotClaimable", Failing,
		"The ServiceAccount was not created by this operator, or it is the namespace's default ServiceAccount. The operator takes over only an existing ServiceAccount whose owner consents, and never the default one, which every pod without a ServiceAccount of its own runs as.",
		"For an existing ServiceAccount, annotate it with cloudctl.io/allow-claim=<cr-name>. For default, give the CR a ServiceAccount of its own.")
	ServiceAccountInUse = register("ServiceAccountInUse", Failing,
		"The ServiceAccount is already claimed by a different CR.",
		"Use another ServiceAccount.")
	ConnectionKeyCollision = register("ConnectionKeyCollision", Failing,
		"Two sources map to one ConfigMap or Secret key once upper-cased with other characters turned into underscores.",
		"Rename one of the resources.")
	RegionChanged = register("RegionChanged", Failing,
		"The CR has resources in a different region than this operator's. Nothing was touched.",
		"Run an operator in that region for it. To delete the CR from here, leaving its resources in AWS as they are, annotate it with cloudctl.io/abandon-other-region=true.")
	WaitingForRuleRelease = register("WaitingForRuleRelease", Waiting,
		"A security group cannot be deleted yet because another group's rule or a running pod still uses it.",
		"None; it is retried.")
)

// Deletion.
var (
	ResourcesNotDrained = register("ResourcesNotDrained", Waiting,
		"Deletion of the CR has not finished: a resource is waiting on its non-empty check or its quiet window.",
		"Empty the resource, or set force: true on it.")
	DeletionBlocked = register("DeletionBlocked", Waiting,
		"Ready is false because deletion of the CR has not finished.",
		"See the DeletionBlocked condition for what is waiting.")
)

func init() {
	registerFamily("Instance", Waiting,
		"AWS reports the RDS instance is not available yet, for example InstanceCreating or InstanceModifying.",
		"None; it is polled until it is available.")
}

// Markdown renders the catalogue as the reference table in docs/reasons.md.
func Markdown() string {
	var b strings.Builder
	b.WriteString("# Condition reasons\n\n")
	b.WriteString("Generated from `internal/reasons`; do not edit by hand. Regenerate with\n")
	b.WriteString("`go test ./internal/reasons -run Doc -update`.\n\n")
	b.WriteString("A condition's `reason` says what is wrong. *Waiting* clears by itself, *Failing* needs a\n")
	b.WriteString("person, *Info* is healthy. `Ready` carries the first section that is not ready,\n")
	b.WriteString("with that section's own reason. A failing section also records a Warning event under\n")
	b.WriteString("the same reason, with the section at the start of its text.\n\n")
	b.WriteString("| Reason | Kind | Meaning | What to do |\n|---|---|---|---|\n")
	for _, r := range catalogue {
		name := "`" + r.Name + "`"
		if r.Prefix {
			name = "`" + r.Name + "…`"
		}
		action := r.Action
		if action == "" {
			action = "—"
		}
		b.WriteString("| " + name + " | " + string(r.Kind) + " | " + escape(r.Meaning) + " | " + escape(action) + " |\n")
	}
	return b.String()
}

func escape(s string) string { return strings.ReplaceAll(s, "|", "\\|") }
