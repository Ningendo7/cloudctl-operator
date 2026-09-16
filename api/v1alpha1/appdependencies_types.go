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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DeletionPolicy controls what happens to the underlying AWS resource when a
// spec entry is removed or the owning CR is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type DeletionPolicy string

const (
	// DeletionPolicyRetain leaves the AWS resource in place and marks it
	// orphaned in status. Default for anything that can hold data.
	DeletionPolicyRetain DeletionPolicy = "Retain"
	// DeletionPolicyDelete removes the AWS resource, subject to a non-empty
	// guard (queues/buckets) unless Force is set.
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

// SharedWithEntry grants another AppDependencies CR permission to consume a
// resource owned by this one. Referencing a resource is not sufficient on
// its own — the owner must explicitly list the consumer here.
type SharedWithEntry struct {
	// namespace of the consuming AppDependencies CR.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// name of the consuming AppDependencies CR.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// ConsumeRef references a resource owned by another AppDependencies CR. The
// owning CR must list this CR in the target resource's sharedWith grants, or
// the reference is reported as unauthorized rather than silently honored.
type ConsumeRef struct {
	// namespace of the AppDependencies CR that owns the resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// name of the AppDependencies CR that owns the resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// resourceName is the `name` of the specific resource entry within the
	// owning CR's section (e.g. the queue name under its sqs.resources list).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=80
	ResourceName string `json:"resourceName"`
}

// ---------------------------------------------------------------------------
// SQS
// ---------------------------------------------------------------------------

// SQSOverrides exposes advanced, non-default SQS configuration.
type SQSOverrides struct {
	// visibilityTimeoutSeconds overrides the default visibility timeout.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=43200
	VisibilityTimeoutSeconds *int32 `json:"visibilityTimeoutSeconds,omitempty"`

	// maxReceiveCount overrides the default DLQ redrive maxReceiveCount.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	MaxReceiveCount *int32 `json:"maxReceiveCount,omitempty"`
}

// SQSQueueSpec declares a single SQS queue this app owns.
type SQSQueueSpec struct {
	// name of the queue, used to derive the actual AWS queue name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=80
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_-]+$`
	Name string `json:"name"`

	// dlq enables a dead-letter queue with a standard redrive policy.
	// +optional
	DLQ bool `json:"dlq,omitempty"`

	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// force allows deleting a non-empty queue when deletionPolicy is Delete.
	// +optional
	Force bool `json:"force,omitempty"`

	// sharedWith grants other AppDependencies CRs permission to consume this queue.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=namespace
	// +listMapKey=name
	SharedWith []SharedWithEntry `json:"sharedWith,omitempty"`

	// adopt allows this CR to take ownership of a pre-existing AWS resource
	// found under this entry's deterministic name that isn't already tagged
	// as owned by this CR. Existence-by-name alone is never treated as
	// ownership — adopt:true is the explicit, deliberate opt-in required
	// before the controller will tag and start managing something it did
	// not create. Refused if the resource is already owned by a *different*
	// AppDependencies CR — that's a naming collision, not an adoption
	// target, and adopt:true must never silently paper over that.
	// +optional
	Adopt bool `json:"adopt,omitempty"`

	// overrides allows tuning advanced settings beyond the opinionated default.
	// +optional
	Overrides *SQSOverrides `json:"overrides,omitempty"`
}

// SQSSpec is the sqs section of an AppDependencies spec.
type SQSSpec struct {
	// resources this app owns.
	// +optional
	// +kubebuilder:validation:MaxItems=50
	// +listType=map
	// +listMapKey=name
	Resources []SQSQueueSpec `json:"resources,omitempty"`

	// consumes references queues owned by other AppDependencies CRs.
	// +optional
	// +kubebuilder:validation:MaxItems=50
	Consumes []ConsumeRef `json:"consumes,omitempty"`
}

// ---------------------------------------------------------------------------
// SNS
// ---------------------------------------------------------------------------

type SNSTopicSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_-]+$`
	Name string `json:"name"`

	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// +optional
	Force bool `json:"force,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=namespace
	// +listMapKey=name
	SharedWith []SharedWithEntry `json:"sharedWith,omitempty"`
}

type SNSSpec struct {
	// +optional
	// +kubebuilder:validation:MaxItems=50
	// +listType=map
	// +listMapKey=name
	Resources []SNSTopicSpec `json:"resources,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=50
	Consumes []ConsumeRef `json:"consumes,omitempty"`
}

// ---------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------

// S3BackupSpec enables versioning + lifecycle-based point-in-time recovery.
// Deliberately separate from S3ReplicationSpec — see the design notes on
// why HA/backup/DR are kept as independent, explicit toggles.
type S3BackupSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

// S3ReplicationSpec configures cross-region replication. Region is required
// when enabled — this is a self-contained check (only needs fields on this
// same object), so it's enforced here via CEL. The separate check that
// region must differ from the cluster's *primary* region needs controller
// configuration this object can't see, so that one is enforced at reconcile
// time instead, not here.
// +kubebuilder:validation:XValidation:rule="!self.enabled || size(self.region) > 0",message="region is required when replication is enabled"
type S3ReplicationSpec struct {
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxLength=20
	Region string `json:"region,omitempty"`
}

// S3LifecycleRule overrides a single lifecycle transition/expiration rule.
// +kubebuilder:validation:XValidation:rule="!has(self.transitionStorageClass) || has(self.transitionAfterDays)",message="transitionAfterDays is required when transitionStorageClass is set"
type S3LifecycleRule struct {
	// id is a human-readable identifier for this rule.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=255
	ID string `json:"id"`

	// transitionAfterDays moves objects to transitionStorageClass after this
	// many days. Both fields must be set together.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TransitionAfterDays *int32 `json:"transitionAfterDays,omitempty"`

	// transitionStorageClass is the destination storage class for the
	// transition above.
	// +optional
	// +kubebuilder:validation:Enum=STANDARD_IA;ONEZONE_IA;GLACIER;DEEP_ARCHIVE;INTELLIGENT_TIERING
	TransitionStorageClass string `json:"transitionStorageClass,omitempty"`

	// expirationAfterDays permanently deletes objects after this many days.
	// +optional
	// +kubebuilder:validation:Minimum=1
	ExpirationAfterDays *int32 `json:"expirationAfterDays,omitempty"`
}

// S3Overrides exposes advanced, non-default S3 configuration beyond the
// opinionated backup/replication toggles.
type S3Overrides struct {
	// versioningEnabled overrides the default versioning behavior. backup
	// and replication both imply versioning on their own; this lets
	// versioning be turned on independently of either, e.g. for a bucket
	// with neither backup nor replication enabled.
	// +optional
	VersioningEnabled *bool `json:"versioningEnabled,omitempty"`

	// lifecycleRules overrides the standard lifecycle policy applied when
	// backup is enabled.
	// +optional
	// +kubebuilder:validation:MaxItems=10
	// +listType=map
	// +listMapKey=id
	LifecycleRules []S3LifecycleRule `json:"lifecycleRules,omitempty"`
}

type S3BucketSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9.-]+$`
	Name string `json:"name"`

	// +optional
	Backup *S3BackupSpec `json:"backup,omitempty"`

	// +optional
	Replication *S3ReplicationSpec `json:"replication,omitempty"`

	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// +optional
	Force bool `json:"force,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=namespace
	// +listMapKey=name
	SharedWith []SharedWithEntry `json:"sharedWith,omitempty"`

	// overrides allows tuning advanced S3 settings (versioning, lifecycle
	// rules) beyond the opinionated backup/replication defaults.
	// +optional
	Overrides *S3Overrides `json:"overrides,omitempty"`
}

type S3Spec struct {
	// +optional
	// +kubebuilder:validation:MaxItems=50
	// +listType=map
	// +listMapKey=name
	Resources []S3BucketSpec `json:"resources,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=50
	Consumes []ConsumeRef `json:"consumes,omitempty"`
}

// ---------------------------------------------------------------------------
// DynamoDB
// ---------------------------------------------------------------------------

type DynamoDBBackupSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

type DynamoDBTableSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_.-]+$`
	Name string `json:"name"`

	// partitionKey is the table's partition key attribute name. AWS does not
	// allow changing a table's key schema after creation, so this is
	// immutable once set.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="partitionKey is immutable; AWS does not support changing a table's key schema after creation"
	PartitionKey string `json:"partitionKey"`

	// sortKey is the table's optional sort key attribute name. Immutable
	// once set, for the same reason as partitionKey.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="sortKey is immutable; AWS does not support changing a table's key schema after creation"
	SortKey string `json:"sortKey,omitempty"`

	// +optional
	Backup *DynamoDBBackupSpec `json:"backup,omitempty"`

	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// +optional
	Force bool `json:"force,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=namespace
	// +listMapKey=name
	SharedWith []SharedWithEntry `json:"sharedWith,omitempty"`
}

type DynamoDBSpec struct {
	// +optional
	// +kubebuilder:validation:MaxItems=50
	// +listType=map
	// +listMapKey=name
	Resources []DynamoDBTableSpec `json:"resources,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=50
	Consumes []ConsumeRef `json:"consumes,omitempty"`
}

// ---------------------------------------------------------------------------
// KMS
// ---------------------------------------------------------------------------

type KMSKeySpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_/-]+$`
	Name string `json:"name"`

	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=namespace
	// +listMapKey=name
	SharedWith []SharedWithEntry `json:"sharedWith,omitempty"`
}

type KMSSpec struct {
	// +optional
	// +kubebuilder:validation:MaxItems=50
	// +listType=map
	// +listMapKey=name
	Resources []KMSKeySpec `json:"resources,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxItems=50
	Consumes []ConsumeRef `json:"consumes,omitempty"`
}

// ---------------------------------------------------------------------------
// Alarms
// ---------------------------------------------------------------------------

// AlarmsSpec turns on standard CloudWatch alarms (queue depth/age, error
// rates, throttling) for whatever resources are declared in this CR.
type AlarmsSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

// ---------------------------------------------------------------------------
// Spec / Status
// ---------------------------------------------------------------------------

// AppDependenciesSpec defines the desired state of AppDependencies
type AppDependenciesSpec struct {
	// serviceAccountName is the ServiceAccount to annotate with the derived
	// IAM role's ARN (IRSA). If unset, the controller creates and owns one
	// named after this CR. Always resolved in this CR's own namespace —
	// cross-namespace targeting would let a CR attach its IAM permissions to
	// an identity it doesn't control, so there is no namespace field here.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// +optional
	SQS *SQSSpec `json:"sqs,omitempty"`

	// +optional
	SNS *SNSSpec `json:"sns,omitempty"`

	// +optional
	S3 *S3Spec `json:"s3,omitempty"`

	// +optional
	DynamoDB *DynamoDBSpec `json:"dynamodb,omitempty"`

	// +optional
	KMS *KMSSpec `json:"kms,omitempty"`

	// +optional
	Alarms *AlarmsSpec `json:"alarms,omitempty"`
}

// ManagedResourceState reflects the ownership-ledger trust window for a
// single managed resource.
// +kubebuilder:validation:Enum=Creating;TagPending;Verified;NeedsVerification
type ManagedResourceState string

const (
	ManagedResourceStateCreating          ManagedResourceState = "Creating"
	ManagedResourceStateTagPending        ManagedResourceState = "TagPending"
	ManagedResourceStateVerified          ManagedResourceState = "Verified"
	ManagedResourceStateNeedsVerification ManagedResourceState = "NeedsVerification"
)

// ManagedResource is one entry in the ownership ledger — every AWS resource
// this CR has ever created. Used to detect orphans when a spec entry is
// removed (spec alone can't represent "used to exist, now gone"), and gated
// behind a trust window before being trusted for any mutating action.
type ManagedResource struct {
	// type of resource, e.g. "sqs", "s3", "sns", "dynamodb", "kms".
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// name is the resource-key from spec (e.g. the queue's `name`).
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// arn of the underlying AWS resource.
	// +kubebuilder:validation:Required
	ARN string `json:"arn"`

	// deletionPolicy captured from the spec entry at the time this resource
	// was created/last reconciled, so cleanup still knows what policy
	// applies even after the spec entry is removed — spec alone can't
	// represent "used to exist, now gone," which is this ledger's whole
	// reason for existing.
	// +kubebuilder:validation:Required
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`

	// force mirrors the spec entry's force flag at the same point.
	// +optional
	Force bool `json:"force,omitempty"`

	// state of the ownership ledger entry's trust window.
	// +kubebuilder:validation:Required
	State ManagedResourceState `json:"state"`

	// createdAt is when this controller created the resource.
	// +kubebuilder:validation:Required
	CreatedAt metav1.Time `json:"createdAt"`

	// lastVerifiedAt is the last time the ownership tag was confirmed to
	// still match this CR. Unset if never verified since creation.
	// +optional
	LastVerifiedAt *metav1.Time `json:"lastVerifiedAt,omitempty"`

	// pendingDeletionSince is set the first time this resource was found
	// non-empty while its deletionPolicy was Delete, so cleanup can tell
	// how long it's been stuck rather than retrying forever silently.
	// Cleared if the resource is re-added to spec.
	// +optional
	PendingDeletionSince *metav1.Time `json:"pendingDeletionSince,omitempty"`
}

// AppDependenciesStatus defines the observed state of AppDependencies.
type AppDependenciesStatus struct {
	// conditions reflect per-section readiness (e.g. "SQSReady", "S3Ready",
	// "IAMReady") plus an aggregate "Ready".
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// managedResources is the ownership ledger described above.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +listMapKey=name
	ManagedResources []ManagedResource `json:"managedResources,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AppDependencies is the Schema for the appdependencies API
type AppDependencies struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of AppDependencies
	// +required
	Spec AppDependenciesSpec `json:"spec"`

	// status defines the observed state of AppDependencies
	// +optional
	Status AppDependenciesStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// AppDependenciesList contains a list of AppDependencies
type AppDependenciesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []AppDependencies `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &AppDependencies{}, &AppDependenciesList{})
		return nil
	})
}
