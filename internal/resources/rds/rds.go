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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
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

	errCodeInvalidDBInstanceState    = "InvalidDBInstanceState"
	errCodeInvalidGroupNotFound      = "InvalidGroup.NotFound"
	errCodeInvalidPermissionNotFound = "InvalidPermission.NotFound"
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
	securityGroupID   string
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
					Retryable: true,
				}
			}
			continue
		}

		if err := checkAdoptable(ctx, awsClient, namespace, crName, r, ledger); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: %w", r.Name, err)
			}
			continue
		}

		sgARN, sgErr := EnsureSecurityGroup(ctx, awsClient, ec2Client, k8sClient, namespace, crName, crUID, r.Name, r.DBSubnetGroupName, r.Engine, region, accountID, r.Adopt, r.SharedWith)
		if sgErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: security group: %w", r.Name, sgErr)
			}
			continue
		}

		opts := instanceOptions{
			deletionPolicy:    r.DeletionPolicy,
			adopt:             r.Adopt,
			engine:            r.Engine,
			engineVersion:     r.EngineVersion,
			instanceClass:     r.InstanceClass,
			allocatedStorage:  r.AllocatedStorage,
			applyImmediately:  r.ApplyImmediately,
			allowMajor:        r.AllowMajorVersionUpgrade,
			dbSubnetGroupName: r.DBSubnetGroupName,
			securityGroupID:   SecurityGroupIDFromARN(sgARN),
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
							Retryable: true,
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
	instanceID := cloudctlaws.ResourceName(namespace, crName, resourceType, resourceName, 63)

	describeOut, err := describeDBInstanceCached(ctx, awsClient, instanceID)

	create := func() ([]depsv1alpha1.ManagedResource, error) {
		created, createErr := createInstance(ctx, awsClient, namespace, crName, crUID, instanceID, resourceName, opts, ledger, recordEvent)
		if createErr == nil {
			*observed = statusCreating
		}
		return created, createErr
	}

	var notFound *types.DBInstanceNotFoundFault
	if errors.As(err, &notFound) {
		return create()
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up instance")
	}
	if len(describeOut.DBInstances) == 0 {
		return create()
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
		tagsOut, tErr := awsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: &instanceARN})
		if tErr != nil {
			return ledger, wrapAWSError(tErr, "reading instance tags")
		}
		currentTags := tagsToMap(tagsOut.TagList)

		if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
			if existingOwner, ok := currentTags[cloudctlaws.OwnerTagKey]; ok && existingOwner != cloudctlaws.OwnerTagValue(namespace, crName) {
				return ledger, fmt.Errorf("instance %q is already owned by a different AppDependencies CR (%s) - this looks like a naming collision, not adopting", instanceID, existingOwner)
			}
			if staleUID, stale := cloudctlaws.IsStaleUID(currentTags, namespace, crName, crUID); stale {
				return ledger, fmt.Errorf("instance %q is tagged with this CR's name but a different UID (%s) - likely a stale resource from a deleted-and-recreated CR, refusing to adopt automatically", instanceID, staleUID)
			}
			if !opts.adopt {
				return ledger, fmt.Errorf("instance %q exists but is not tagged as owned by this CR - set adopt:true to bring it under management", instanceID)
			}

			merged := cloudctlaws.MergeTags(currentTags, ownerTags(namespace, crName, crUID))
			if _, tagErr := awsClient.AddTagsToResource(ctx, &rds.AddTagsToResourceInput{
				ResourceName: &instanceARN,
				Tags:         mapToTags(merged),
			}); tagErr != nil {
				return ledger, wrapAWSError(tagErr, "adopting instance (tagging)")
			}
			if recordEvent != nil {
				recordEvent("Normal", "InstanceAdopted", fmt.Sprintf("Adopted existing RDS instance %s under management", instanceARN))
			}
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
			Reason: instanceStatusReason(instanceStatus),
		}
	case statusAvailable:
	case "backing-up", "storage-optimization":
		// Still serving connections, but AWS limits modifications meanwhile.
		servingOnly = true
	default:
		return ledger, &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("instance %q is still %s", instanceID, instanceStatus),
			Retryable: true,
			Reason:    instanceStatusReason(instanceStatus),
		}
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
		MultiAZ:                  aws.Bool(opts.multiAZ),
		Tags:                     mapToTags(ownerTags(namespace, crName, crUID)),
	}
	if opts.backupEnabled {
		input.BackupRetentionPeriod = aws.Int32(defaultBackupRetentionDays)
	} else {
		input.BackupRetentionPeriod = aws.Int32(0)
	}
	if opts.kmsKeyARN != nil {
		input.StorageEncrypted = aws.Bool(true)
		input.KmsKeyId = opts.kmsKeyARN
	}

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
	// AWS rejects a decrease outright, and the CEL rule on the field
	// already keeps spec from requesting one - only growth is ever sent.
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

	if !storageGrowth && !otherChange && !groupMissing {
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

// instanceStatusReason turns a DBInstanceStatus such as
// "configuring-enhanced-monitoring" into a valid condition reason
// ("InstanceConfiguringEnhancedMonitoring").
func instanceStatusReason(instanceStatus string) string {
	var b strings.Builder
	b.WriteString("Instance")
	for _, word := range strings.FieldsFunc(instanceStatus, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
	}) {
		b.WriteString(strings.ToUpper(word[:1]) + word[1:])
	}
	return b.String()
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
