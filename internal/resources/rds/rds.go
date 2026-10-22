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

package rds

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// rdsAPI is the subset of the RDS client this package needs for the core
// instance lifecycle. Deliberately has no DeleteDBInstance - see
// cleanup.go for why Cleanup never calls it.
type rdsAPI interface {
	CreateDBInstance(ctx context.Context, in *rds.CreateDBInstanceInput, optFns ...func(*rds.Options)) (*rds.CreateDBInstanceOutput, error)
	ModifyDBInstance(ctx context.Context, in *rds.ModifyDBInstanceInput, optFns ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error)
	DescribeDBInstances(ctx context.Context, in *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
	ListTagsForResource(ctx context.Context, in *rds.ListTagsForResourceInput, optFns ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error)
	AddTagsToResource(ctx context.Context, in *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error)
	RemoveTagsFromResource(ctx context.Context, in *rds.RemoveTagsFromResourceInput, optFns ...func(*rds.Options)) (*rds.RemoveTagsFromResourceOutput, error)
	DescribeDBSubnetGroups(ctx context.Context, in *rds.DescribeDBSubnetGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSubnetGroupsOutput, error)
	DescribeDBEngineVersions(ctx context.Context, in *rds.DescribeDBEngineVersionsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBEngineVersionsOutput, error)
	CreateDBSnapshot(ctx context.Context, in *rds.CreateDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error)
	DescribeDBSnapshots(ctx context.Context, in *rds.DescribeDBSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error)
	DeleteDBSnapshot(ctx context.Context, in *rds.DeleteDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.DeleteDBSnapshotOutput, error)
}

const resourceType = "rds"

// masterUsername is fixed and project-chosen, not user-configurable - the
// app never needs to know or manage it, since it reads the full
// credential set (including this value) from the mirrored Secret rather
// than supplying its own.
const masterUsername = "cloudctl_admin"

// Values this package matches against AWS responses and inputs.
const (
	statusAvailable = "available"
	statusCreating  = "creating"
	statusFailed    = "failed"
	enginePostgres  = "postgres"

	errCodeDependencyViolation        = "DependencyViolation"
	errCodeInvalidDBInstanceState     = "InvalidDBInstanceState"
	errCodeInvalidGroupDuplicate      = "InvalidGroup.Duplicate"
	errCodeInvalidGroupNotFound       = "InvalidGroup.NotFound"
	errCodeInvalidPermissionNotFound  = "InvalidPermission.NotFound"
	errCodeInvalidPermissionDuplicate = "InvalidPermission.Duplicate"
)

// defaultBackupRetentionDays is AWS's own long-standing default retention
// window for automated backups, applied when backup.enabled is true.
const defaultBackupRetentionDays = int32(7)

// ErrReplicationNotSupported is returned for any instance requesting
// replication. A cross-region read replica needs a client calling the RDS
// API in the destination region (CreateDBInstanceReadReplica has to run
// against that region's own endpoint, unlike S3's PutBucketReplication,
// which stays a same-region call referencing the destination bucket only
// by ARN) - this operator's Clients holds exactly one region for the
// whole controller process, the same single-region assumption that
// already blocks S3's own cross-region replication and DynamoDB Global
// Tables. Left unimplemented rather than half-built until there's a real
// design for multi-region client support, and surfaced as a hard,
// non-retryable error rather than silently ignored - retrying forever on
// a feature that can never succeed would look indistinguishable from a
// slow-but-working reconcile.
var ErrReplicationNotSupported = errors.New("replication is not implemented yet - it needs cross-region awsClient support this operator doesn't have")

type instanceOptions struct {
	deletionPolicy    depsv1alpha1.DeletionPolicy
	adopt             bool
	engine            string
	engineVersion     string
	instanceClass     string
	allocatedStorage  int32
	applyImmediately  bool
	allowMajor        bool
	dbSubnetGroupName string
	multiAZ           bool
	backupEnabled     bool
	kmsKeyARN         *string
	// encryptionOff is an explicit encryption: {enabled: false}; without
	// it, an instance with no key is encrypted with AWS's RDS key.
	encryptionOff      bool
	deletionProtection *bool
	securityGroupID    string
}

// Ensure reconciles every declared RDS instance against AWS, updating the
// ownership ledger as it goes. kmsClient is only ever touched when a
// resource actually declares encryption.enabled or encryption.kmsKeyRef -
// a CR that never uses either can pass nil. region/accountID are needed
// to construct this instance's dedicated security group's ARN ourselves,
// since EC2 never hands one back directly, unlike every other service
// this operator calls.
func Ensure(
	ctx context.Context,
	awsClient rdsAPI,
	kmsClient cloudctlaws.KMSClient,
	ec2Client cloudctlaws.EC2Client,
	k8sClient client.Client,
	namespace, crName, crUID, region, accountID string,
	spec *depsv1alpha1.RDSSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	var grants []depsv1alpha1.RDSSubnetGroupGrant
	var grantsFetched bool
	var grantsErr error
	for _, r := range spec.Resources {
		if r.Replication != nil && r.Replication.Enabled {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: %w", r.Name, ErrReplicationNotSupported)
			}
			continue
		}

		if !grantsFetched {
			grants, grantsErr = listSubnetGroupGrants(ctx, k8sClient)
			grantsFetched = true
		}
		if grantsErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: checking subnet group authorization: %w", r.Name, grantsErr)
			}
			continue
		}
		if !isAuthorizedByGrants(grants, namespace, r.DBSubnetGroupName) {
			if firstErr == nil {
				firstErr = &cloudctlaws.ReconcileError{
					Err: fmt.Errorf("instance %q: namespace %q is not authorized to use DB subnet group %q - "+
						"a platform admin must create an RDSSubnetGroupGrant covering it", r.Name, namespace, r.DBSubnetGroupName),
					Retryable:     true,
					WakesOnChange: true,
				}
			}
			continue
		}

		if err := checkAdoptable(ctx, awsClient, kmsClient, namespace, crName, r, ledger); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: %w", r.Name, err)
			}
			continue
		}

		recordedSG, _ := status.FindManagedResource(ledger, podNetworkIdentityResourceType, instanceSecurityGroupLedgerName(r.Name))
		sgARN, sgErr := ensureInstanceSecurityGroup(ctx, awsClient, ec2Client, k8sClient, namespace, crName, crUID, r.Name, r.DBSubnetGroupName, r.Engine, region, accountID, r.Adopt, r.SharedWith, recordedSG.ARN)
		if sgErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: security group: %w", r.Name, sgErr)
			}
			continue
		}

		ledger = recordInstanceSecurityGroup(ledger, r.Name, sgARN)

		opts := instanceOptions{
			deletionPolicy:     r.DeletionPolicy,
			adopt:              r.Adopt,
			engine:             r.Engine,
			engineVersion:      r.EngineVersion,
			instanceClass:      r.InstanceClass,
			allocatedStorage:   r.AllocatedStorage,
			applyImmediately:   r.ApplyImmediately,
			allowMajor:         r.AllowMajorVersionUpgrade,
			dbSubnetGroupName:  r.DBSubnetGroupName,
			securityGroupID:    SecurityGroupIDFromARN(sgARN),
			encryptionOff:      r.Encryption != nil && !r.Encryption.Enabled && r.Encryption.KMSKeyRef == "",
			deletionProtection: r.DeletionProtection,
		}
		if r.HighAvailability != nil {
			opts.multiAZ = r.HighAvailability.Enabled
		}
		if r.Backup != nil {
			opts.backupEnabled = r.Backup.Enabled
		}
		if r.Encryption != nil {
			if r.Encryption.KMSKeyRef != "" {
				arn, ok := kms.SharedKeyARN(ledger, r.Encryption.KMSKeyRef)
				if !ok {
					if firstErr == nil {
						firstErr = &cloudctlaws.ReconcileError{
							Err: fmt.Errorf("instance %q: encryption.kmsKeyRef %q is not ready yet - declare it under kms.resources",
								r.Name, r.Encryption.KMSKeyRef),
							Retryable:     true,
							WakesOnChange: true,
						}
					}
					continue
				}
				opts.kmsKeyARN = &arn
			}
			if r.Encryption.Enabled {
				arn, updatedLedger, err := kms.EnsureDedicatedKey(ctx, kmsClient, namespace, crName, crUID, resourceType, r.Name, r.DeletionPolicy, ledger, checkpoint, recordEvent)
				ledger = updatedLedger
				if err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("instance %q: encryption key: %w", r.Name, err)
					}
					continue
				}
				opts.kmsKeyARN = &arn
			}
		}

		var err error
		ledger, err = ensureInstance(ctx, awsClient, namespace, crName, crUID, r.Name, opts, ledger, recordEvent)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("instance %q: %w", r.Name, err)
		}
	}
	return ledger, firstErr
}

// listSubnetGroupGrants fetches every RDSSubnetGroupGrant (cluster-
// scoped) once - callers reuse the result across every resource in one
// Ensure pass rather than re-listing per resource, since the same grant
// set authorizes all of them.
func listSubnetGroupGrants(ctx context.Context, k8sClient client.Client) ([]depsv1alpha1.RDSSubnetGroupGrant, error) {
	var grants depsv1alpha1.RDSSubnetGroupGrantList
	if err := k8sClient.List(ctx, &grants); err != nil {
		return nil, err
	}
	return grants.Items, nil
}

// isAuthorizedByGrants checks grants for one covering dbSubnetGroupName
// that also lists namespace in allowedNamespaces. No grant covering this
// subnet group at all, or one that exists but doesn't list this
// namespace, both mean not authorized - there's no CR on the other end
// of this grant to default-trust the way sharedWith always has one.
func isAuthorizedByGrants(grants []depsv1alpha1.RDSSubnetGroupGrant, namespace, dbSubnetGroupName string) bool {
	for _, g := range grants {
		if g.Spec.DBSubnetGroupName != dbSubnetGroupName {
			continue
		}
		if slices.Contains(g.Spec.AllowedNamespaces, namespace) {
			return true
		}
	}
	return false
}

// ensureInstance records the AWS-reported status on the ledger entry on
// every path, including the retryable/failed ones that return early.
func ensureInstance(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName, crUID, resourceName string,
	opts instanceOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	var observed string
	ledger, err := ensureInstanceObserving(ctx, awsClient, namespace, crName, crUID, resourceName, opts, ledger, recordEvent, &observed)
	recordObservedStatus(&ledger, resourceName, observed)
	return ledger, err
}

func ensureInstanceObserving(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName, crUID, resourceName string,
	opts instanceOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
	observed *string,
) ([]depsv1alpha1.ManagedResource, error) {
	instanceID := cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, namespace, crName, resourceType, resourceName, 63)

	describeOut, err := describeDBInstanceCached(ctx, awsClient, instanceID)

	create := func() ([]depsv1alpha1.ManagedResource, error) {
		created, createErr := createInstance(ctx, awsClient, namespace, crName, crUID, instanceID, resourceName, opts, ledger, recordEvent)
		if createErr == nil {
			*observed = statusCreating
		}
		return created, createErr
	}

	// Creation only ever happens for an instance this CR has never had. One
	// the ledger knows but AWS no longer has was deleted out-of-band, and a
	// replacement would be an empty database reported Ready.
	missing := func() ([]depsv1alpha1.ManagedResource, error) {
		if entry, found := trackedInstance(ledger, resourceName); found {
			return ledger, instanceMissingError(entry, instanceID, recordEvent)
		}
		return create()
	}

	var notFound *types.DBInstanceNotFoundFault
	if errors.As(err, &notFound) {
		return missing()
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up instance")
	}
	if len(describeOut.DBInstances) == 0 {
		return missing()
	}

	instance := describeOut.DBInstances[0]
	instanceStatus := aws.ToString(instance.DBInstanceStatus)
	*observed = instanceStatus
	instanceARN := aws.ToString(instance.DBInstanceArn)
	existingBeforeCheck, existingBeforeCheckFound := status.FindManagedResource(ledger, resourceType, resourceName)

	// Ownership is verified (or re-verified past the trust window) before
	// the status check below, not after - tags are set atomically on
	// CreateDBInstance, so they're queryable the instant the instance
	// exists, regardless of its provisioning status. Checking this first
	// means a foreign instance, or one owned by a different CR, is
	// reported immediately instead of only after waiting out the entire
	// "still creating" or "still modifying" window to learn the real
	// problem was never going to resolve on its own.
	needsOwnershipCheck := !existingBeforeCheckFound || status.NeedsRevalidation(existingBeforeCheck)
	if needsOwnershipCheck {
		if err := ensureInstanceOwnership(ctx, awsClient, namespace, crName, crUID, instanceID, instanceARN, opts.adopt, recordEvent); err != nil {
			return ledger, err
		}
	}

	// The CRD forbids editing an instance's engine, subnet group or
	// encryption, but removing a Delete-policy entry and adding it back
	// passes that rule while the instance is still tracked, since it is never
	// deleted.
	if existingBeforeCheckFound && existingBeforeCheck.State != depsv1alpha1.ManagedResourceStateRelinquished {
		if err := checkImmutableSettings(instance, instanceID, opts); err != nil {
			return ledger, err
		}
	}

	// Only a few statuses are known-terminal or known-serving; anything
	// unrecognized is retried rather than assumed ready, since RDS's status
	// set is large and open-ended (unlike DynamoDB's).
	servingOnly := false
	switch instanceStatus {
	case statusFailed:
		return ledger, &cloudctlaws.ReconcileError{
			Err:    fmt.Errorf("instance %q entered status %q - this requires manual investigation, not an automatic retry", instanceID, instanceStatus),
			Reason: reasons.InstanceStatus(instanceStatus),
		}
	case statusAvailable:
	case "backing-up", "storage-optimization":
		// Still serving connections, but AWS limits modifications meanwhile.
		servingOnly = true
	default:
		return ledger, &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("instance %q is still %s", instanceID, instanceStatus),
			Retryable: true,
			Reason:    reasons.InstanceStatus(instanceStatus),
		}
	}

	// Reported, not reverted: a public flag may be someone's deliberate
	// change, and this operator's security group is the only access it
	// controls.
	if aws.ToBool(instance.PubliclyAccessible) && recordEvent != nil {
		recordEvent("Warning", "InstancePubliclyAccessible", fmt.Sprintf("RDS instance %s is publicly accessible; this operator creates instances as private and never changes the setting - turn it off in AWS if unintended", instanceARN))
	}

	// Re-asserted every pass, independent of the ownership trust window.
	if !servingOnly {
		if err := reconcileInstanceAttributes(ctx, awsClient, instanceID, instanceARN, instance, opts, recordEvent); err != nil {
			return ledger, err
		}
	}

	if !needsOwnershipCheck {
		updated := existingBeforeCheck
		updated.DeletionPolicy = opts.deletionPolicy
		status.UpsertManagedResource(&ledger, updated)
		return ledger, nil
	}

	if recordEvent != nil && existingBeforeCheckFound && existingBeforeCheck.State == depsv1alpha1.ManagedResourceStateCreating {
		recordEvent("Normal", "InstanceAvailable", fmt.Sprintf("RDS instance %s is now available", instanceARN))
	}

	return recordVerified(ledger, resourceName, instanceARN, opts.deletionPolicy), nil
}

func createInstance(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName, crUID, instanceID, resourceName string,
	opts instanceOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	input := &rds.CreateDBInstanceInput{
		DBInstanceIdentifier:     &instanceID,
		Engine:                   &opts.engine,
		EngineVersion:            &opts.engineVersion,
		DBInstanceClass:          &opts.instanceClass,
		AllocatedStorage:         &opts.allocatedStorage,
		DBSubnetGroupName:        &opts.dbSubnetGroupName,
		VpcSecurityGroupIds:      []string{opts.securityGroupID},
		MasterUsername:           aws.String(masterUsername),
		ManageMasterUserPassword: aws.Bool(true),
		// Set explicitly: AWS defaults to public in a default VPC.
		PubliclyAccessible: aws.Bool(false),
		MultiAZ:            aws.Bool(opts.multiAZ),
		Tags:               mapToTags(ownerTags(namespace, crName, crUID)),
	}
	if opts.backupEnabled {
		input.BackupRetentionPeriod = aws.Int32(defaultBackupRetentionDays)
	} else {
		input.BackupRetentionPeriod = aws.Int32(0)
	}
	switch {
	case opts.kmsKeyARN != nil:
		input.StorageEncrypted = aws.Bool(true)
		input.KmsKeyId = opts.kmsKeyARN
	case !opts.encryptionOff:
		// Encryption can't be added later, so it is on unless the spec
		// opts out; with no key named, RDS uses its own managed key.
		input.StorageEncrypted = aws.Bool(true)
	}
	input.DeletionProtection = aws.Bool(opts.deletionProtection == nil || *opts.deletionProtection)

	createOut, err := awsClient.CreateDBInstance(ctx, input)
	if err != nil {
		return ledger, wrapAWSError(err, "creating instance")
	}
	instanceARN := aws.ToString(createOut.DBInstance.DBInstanceArn)
	if recordEvent != nil {
		recordEvent("Normal", "InstanceCreating", fmt.Sprintf("Creating RDS instance %s (waiting for available)", instanceARN))
	}

	now := metav1.Now()
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           resourceName,
		ARN:            instanceARN,
		State:          depsv1alpha1.ManagedResourceStateCreating,
		DeletionPolicy: opts.deletionPolicy,
		CreatedAt:      now,
	})

	return ledger, nil
}

func reconcileInstanceAttributes(ctx context.Context, awsClient rdsAPI, instanceID, instanceARN string, instance types.DBInstance, opts instanceOptions, recordEvent status.EventRecorder) error {
	modify := &rds.ModifyDBInstanceInput{DBInstanceIdentifier: &instanceID}
	storageGrowth, otherChange := false, false
	cur := effectiveAttributes(instance)

	if cur.instanceClass != opts.instanceClass {
		modify.DBInstanceClass = &opts.instanceClass
		otherChange = true
	}
	if cur.multiAZ != opts.multiAZ {
		modify.MultiAZ = aws.Bool(opts.multiAZ)
		otherChange = true
	}
	// Backups are only ever raised: setting retention to 0 deletes the
	// instance's automated backups, and lowering it deletes the older ones.
	desiredBackupRetention := int32(0)
	if opts.backupEnabled {
		desiredBackupRetention = defaultBackupRetentionDays
	}
	switch {
	case cur.backupRetention < desiredBackupRetention:
		modify.BackupRetentionPeriod = aws.Int32(desiredBackupRetention)
		otherChange = true
	case cur.backupRetention > 0 && !opts.backupEnabled && recordEvent != nil:
		recordEvent("Warning", "BackupsLeftEnabled", fmt.Sprintf("RDS instance %s has automated backups but its spec disables them; not turning them off, since that deletes the existing backups - disable them in AWS if intended", instanceARN))
	}
	// AWS rejects a decrease outright, so only growth is ever sent. A smaller
	// spec still gets here, when the instance has grown on its own or the
	// entry was removed and added back, and is reported.
	reportStorageLargerThanSpec(instanceARN, cur.allocatedStorage, opts.allocatedStorage, recordEvent)
	if opts.allocatedStorage > cur.allocatedStorage {
		modify.AllocatedStorage = &opts.allocatedStorage
		// Provisioned-IOPS volumes require Iops alongside a storage change.
		if st := aws.ToString(instance.StorageType); (st == "io1" || st == "io2") && instance.Iops != nil {
			modify.Iops = instance.Iops
		}
		storageGrowth = true
	}
	upgrade, major, err := engineVersionUpgrade(ctx, awsClient, opts.engine, cur.engineVersion, opts.engineVersion)
	if err != nil {
		return err
	}
	if upgrade {
		if major && !opts.allowMajor {
			return fmt.Errorf("engineVersion %q is a major upgrade from %s - set allowMajorVersionUpgrade: true to allow it", opts.engineVersion, cur.engineVersion)
		}
		modify.EngineVersion = &opts.engineVersion
		if major {
			modify.AllowMajorVersionUpgrade = aws.Bool(true)
		}
		otherChange = true
	}

	groups, groupMissing := withSecurityGroup(instance, opts.securityGroupID)
	if groupMissing {
		modify.VpcSecurityGroupIds = groups
	}

	protectionChange := reconcileDeletionProtection(modify, instance, instanceARN, opts, recordEvent)

	if !storageGrowth && !otherChange && !groupMissing && !protectionChange {
		return nil
	}
	// An immediate modify also applies every change already pending, so
	// changes default to the weekly maintenance window. Storage growth alone
	// causes no downtime and is often urgent, so it goes at once - but only
	// when nothing else is pending for it to drag along.
	growthOnly := storageGrowth && !otherChange && instance.PendingModifiedValues == nil
	immediate := opts.applyImmediately || growthOnly
	modify.ApplyImmediately = aws.Bool(immediate)
	if _, err := awsClient.ModifyDBInstance(ctx, modify); err != nil {
		return wrapAWSError(err, "updating instance attributes")
	}
	if (storageGrowth || otherChange) && !immediate && recordEvent != nil {
		recordEvent("Normal", "ChangeDeferred", fmt.Sprintf("Changes to RDS instance %s will apply in its next maintenance window (set applyImmediately to apply now)", instanceARN))
	}
	return nil
}

// reconcileDeletionProtection keeps deletion protection in line with the
// spec, on by default, for an instance this operator created. An adopted
// instance's own setting is only changed when the spec sets it, and only
// reported otherwise. The change applies at once, with no maintenance
// window or downtime.
func reconcileDeletionProtection(modify *rds.ModifyDBInstanceInput, instance types.DBInstance, instanceARN string, opts instanceOptions, recordEvent status.EventRecorder) bool {
	have := aws.ToBool(instance.DeletionProtection)
	if opts.adopt && opts.deletionProtection == nil {
		if !have && recordEvent != nil {
			recordEvent("Warning", "DeletionProtectionOff", fmt.Sprintf("Adopted RDS instance %s has deletion protection off; the operator leaves an adopted instance's setting alone unless deletionProtection is set in its spec", instanceARN))
		}
		return false
	}
	want := opts.deletionProtection == nil || *opts.deletionProtection
	if have == want {
		return false
	}
	modify.DeletionProtection = aws.Bool(want)
	if want && recordEvent != nil {
		recordEvent("Warning", "DeletionProtectionEnabled", fmt.Sprintf("Turned on deletion protection for RDS instance %s; set deletionProtection: false to allow deleting it", instanceARN))
	}
	return true
}

// withSecurityGroup returns the instance's current security groups plus
// groupID, and whether groupID was missing. Existing groups are kept: an
// adopted instance's own connectivity must not be replaced.
func withSecurityGroup(instance types.DBInstance, groupID string) (groups []string, missing bool) {
	missing = true
	for _, m := range instance.VpcSecurityGroups {
		if aws.ToString(m.Status) == "removing" {
			continue
		}
		id := aws.ToString(m.VpcSecurityGroupId)
		groups = append(groups, id)
		if id == groupID {
			missing = false
		}
	}
	if missing {
		groups = append(groups, groupID)
	}
	return groups, missing
}

type instanceAttributes struct {
	instanceClass    string
	engineVersion    string
	multiAZ          bool
	backupRetention  int32
	allocatedStorage int32
}

// effectiveAttributes is the instance's attributes once every change AWS
// has already accepted lands, so a modify still in flight isn't re-sent.
func effectiveAttributes(instance types.DBInstance) instanceAttributes {
	attrs := instanceAttributes{
		instanceClass:    aws.ToString(instance.DBInstanceClass),
		engineVersion:    aws.ToString(instance.EngineVersion),
		multiAZ:          aws.ToBool(instance.MultiAZ),
		backupRetention:  aws.ToInt32(instance.BackupRetentionPeriod),
		allocatedStorage: aws.ToInt32(instance.AllocatedStorage),
	}
	pending := instance.PendingModifiedValues
	if pending == nil {
		return attrs
	}
	if pending.DBInstanceClass != nil {
		attrs.instanceClass = *pending.DBInstanceClass
	}
	if pending.EngineVersion != nil {
		attrs.engineVersion = *pending.EngineVersion
	}
	if pending.MultiAZ != nil {
		attrs.multiAZ = *pending.MultiAZ
	}
	if pending.BackupRetentionPeriod != nil {
		attrs.backupRetention = *pending.BackupRetentionPeriod
	}
	if pending.AllocatedStorage != nil {
		attrs.allocatedStorage = *pending.AllocatedStorage
	}
	return attrs
}

// creationVisibilityGrace is how long a just-created instance may be absent
// from DescribeDBInstances before that is treated as it having been deleted.
const creationVisibilityGrace = 5 * time.Minute

func instanceMissingError(entry depsv1alpha1.ManagedResource, instanceID string, recordEvent status.EventRecorder) error {
	if entry.State == depsv1alpha1.ManagedResourceStateCreating && time.Since(entry.CreatedAt.Time) < creationVisibilityGrace {
		return &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("instance %q was just created and is not visible yet", instanceID),
			Retryable: true,
		}
	}
	if recordEvent != nil {
		recordEvent("Warning", "InstanceMissing", fmt.Sprintf("RDS instance %s no longer exists in AWS and will not be recreated", entry.ARN))
	}
	return &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("RDS instance %q (%s) no longer exists in AWS: it was deleted out-of-band, and this operator will not create an empty "+
			"replacement. Restore it under the same identifier from a snapshot or point-in-time backup and set adopt: true, "+
			"or remove it from spec and add it again to start fresh", instanceID, entry.ARN),
		Reason: reasons.InstanceMissing,
	}
}

// recordObservedStatus stamps the ledger entry with the AWS-reported
// status. The timestamp moves only when the value changes, so steady
// polling never rewrites status.
func recordObservedStatus(ledger *[]depsv1alpha1.ManagedResource, resourceName, observed string) {
	if observed == "" {
		return
	}
	entry, found := status.FindManagedResource(*ledger, resourceType, resourceName)
	if !found || entry.ObservedStatus == observed {
		return
	}
	now := metav1.Now()
	entry.ObservedStatus = observed
	entry.ObservedStatusSince = &now
	status.UpsertManagedResource(ledger, entry)
}

func recordVerified(ledger []depsv1alpha1.ManagedResource, ledgerName, arn string, deletionPolicy depsv1alpha1.DeletionPolicy) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	entry := depsv1alpha1.ManagedResource{CreatedAt: now}
	if existing, existingFound := status.FindManagedResource(ledger, resourceType, ledgerName); existingFound {
		entry.CreatedAt = existing.CreatedAt
		entry.ObservedStatus = existing.ObservedStatus
		entry.ObservedStatusSince = existing.ObservedStatusSince
	}
	entry.Type = resourceType
	entry.Name = ledgerName
	entry.ARN = arn
	entry.State = depsv1alpha1.ManagedResourceStateVerified
	entry.DeletionPolicy = deletionPolicy
	entry.LastVerifiedAt = &now
	status.UpsertManagedResource(&ledger, entry)
	return ledger
}

func ownerTags(namespace, crName, crUID string) map[string]string {
	return map[string]string{
		cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue(namespace, crName),
		cloudctlaws.OwnerUIDTagKey: crUID,
	}
}

func mapToTags(m map[string]string) []types.Tag {
	tags := make([]types.Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return tags
}

func tagsToMap(tags []types.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key != nil && t.Value != nil {
			m[*t.Key] = *t.Value
		}
	}
	return m
}

func wrapAWSError(err error, errContext string) error {
	if err == nil {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:       fmt.Errorf("%s: %w", errContext, err),
		Retryable: cloudctlaws.IsRetryable(err) || isInTransitionError(err),
	}
}

// isInTransitionError reports AWS rejecting a call because the instance or
// snapshot is mid-operation. RDS can keep reporting "available" for a few
// seconds after accepting a change, so this clears on its own.
func isInTransitionError(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case errCodeInvalidDBInstanceState, "InvalidDBSnapshotState":
		return true
	}
	return false
}

// trackedInstance returns the ledger entry of an instance this operator is
// still responsible for. A relinquished one is not: it is no longer checked
// against AWS, so its absence says nothing about this CR's data.
func trackedInstance(ledger []depsv1alpha1.ManagedResource, resourceName string) (depsv1alpha1.ManagedResource, bool) {
	entry, found := status.FindManagedResource(ledger, resourceType, resourceName)
	return entry, found && entry.State != depsv1alpha1.ManagedResourceStateRelinquished
}

// ensureInstanceOwnership refuses an instance tagged for another CR, or for
// an earlier UID this CR wasn't told to take over, and tags an untagged one
// when adopt is set.
func ensureInstanceOwnership(ctx context.Context, awsClient rdsAPI, namespace, crName, crUID, instanceID, instanceARN string, adopt bool, recordEvent status.EventRecorder) error {
	tagsOut, tErr := awsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: &instanceARN})
	if tErr != nil {
		return wrapAWSError(tErr, "reading instance tags")
	}
	currentTags := tagsToMap(tagsOut.TagList)

	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		if existingOwner, ok := currentTags[cloudctlaws.OwnerTagKey]; ok && existingOwner != cloudctlaws.OwnerTagValue(namespace, crName) {
			return cloudctlaws.OwnedByAnotherCRError("instance", instanceID, existingOwner)
		}
		transferred := false
		if staleUID, stale := cloudctlaws.IsStaleUID(currentTags, namespace, crName, crUID); stale {
			if !cloudctlaws.MayTransferFrom(ctx, staleUID) {
				return cloudctlaws.PreviousIncarnationError("instance", instanceID, staleUID)
			}
			transferred = true
		}
		if !transferred && !adopt {
			return cloudctlaws.AdoptRequiredError("instance", instanceID)
		}

		merged := cloudctlaws.MergeTags(currentTags, ownerTags(namespace, crName, crUID))
		if _, tagErr := awsClient.AddTagsToResource(ctx, &rds.AddTagsToResourceInput{
			ResourceName: &instanceARN,
			Tags:         mapToTags(merged),
		}); tagErr != nil {
			return wrapAWSError(tagErr, "adopting instance (tagging)")
		}
		if cloudctlaws.HasReleaseTags(currentTags) {
			if _, untagErr := awsClient.RemoveTagsFromResource(ctx, &rds.RemoveTagsFromResourceInput{ResourceName: &instanceARN, TagKeys: cloudctlaws.ReleasedTagKeys()}); untagErr != nil {
				cloudctlaws.ReleaseTagFailed(recordEvent, "instance", instanceID, untagErr)
			}
		}
		if recordEvent != nil {
			if transferred {
				recordEvent("Normal", cloudctlaws.EventOwnershipTransferred, fmt.Sprintf("Took over RDS instance %s from a previous incarnation of this CR", instanceARN))
			} else {
				recordEvent("Normal", "InstanceAdopted", fmt.Sprintf("Adopted existing RDS instance %s under management", instanceARN))
			}
		}
	}
	return nil
}

// reasons.ImmutableSettingChanged marks a tracked instance whose spec asks for
// an engine, subnet group or storage encryption it can't have.

func checkImmutableSettings(instance types.DBInstance, instanceID string, opts instanceOptions) error {
	actualSubnetGroup := ""
	if instance.DBSubnetGroup != nil {
		actualSubnetGroup = aws.ToString(instance.DBSubnetGroup.DBSubnetGroupName)
	}
	var changed []string
	if actual := aws.ToString(instance.Engine); actual != opts.engine {
		changed = append(changed, fmt.Sprintf("engine is %s (spec: %s)", actual, opts.engine))
	}
	if actualSubnetGroup != opts.dbSubnetGroupName {
		changed = append(changed, fmt.Sprintf("dbSubnetGroupName is %s (spec: %s)", orNone(actualSubnetGroup), opts.dbSubnetGroupName))
	}
	if encrypted := aws.ToBool(instance.StorageEncrypted); encrypted == opts.encryptionOff {
		changed = append(changed, fmt.Sprintf("storage encryption is %v (spec: %v)", encrypted, !opts.encryptionOff))
	} else if actual := aws.ToString(instance.KmsKeyId); opts.kmsKeyARN != nil && encrypted && actual != *opts.kmsKeyARN {
		changed = append(changed, fmt.Sprintf("its storage is encrypted with KMS key %s (spec: %s)", actual, *opts.kmsKeyARN))
	}
	if len(changed) == 0 {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err:    fmt.Errorf("instance %q can't change: %s; set them back, or move the data to a new entry", instanceID, strings.Join(changed, "; ")),
		Reason: reasons.ImmutableSettingChanged,
	}
}

func reportStorageLargerThanSpec(instanceARN string, actual, want int32, recordEvent status.EventRecorder) {
	if want >= actual || recordEvent == nil {
		return
	}
	recordEvent("Warning", "StorageLargerThanSpec", fmt.Sprintf("RDS instance %s has %d GiB but its spec says %d; the operator never shrinks storage - raise the spec to match", instanceARN, actual, want))
}
