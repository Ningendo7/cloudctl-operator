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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
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
	DescribeDBInstances(ctx context.Context, in *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
	ListTagsForResource(ctx context.Context, in *rds.ListTagsForResourceInput, optFns ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error)
	AddTagsToResource(ctx context.Context, in *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error)
	RemoveTagsFromResource(ctx context.Context, in *rds.RemoveTagsFromResourceInput, optFns ...func(*rds.Options)) (*rds.RemoveTagsFromResourceOutput, error)
	DescribeDBSubnetGroups(ctx context.Context, in *rds.DescribeDBSubnetGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSubnetGroupsOutput, error)
	CreateDBSnapshot(ctx context.Context, in *rds.CreateDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error)
	DescribeDBSnapshots(ctx context.Context, in *rds.DescribeDBSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error)
}

const resourceType = "rds"

// masterUsername is fixed and project-chosen, not user-configurable - the
// app never needs to know or manage it, since it reads the full
// credential set (including this value) from the mirrored Secret rather
// than supplying its own.
const masterUsername = "cloudctl_admin"

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
	for _, r := range spec.Resources {
		if r.Replication != nil && r.Replication.Enabled {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: %w", r.Name, ErrReplicationNotSupported)
			}
			continue
		}

		authorized, authErr := isSubnetGroupAuthorized(ctx, k8sClient, namespace, r.DBSubnetGroupName)
		if authErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("instance %q: checking subnet group authorization: %w", r.Name, authErr)
			}
			continue
		}
		if !authorized {
			if firstErr == nil {
				firstErr = &cloudctlaws.ReconcileError{
					Err: fmt.Errorf("instance %q: namespace %q is not authorized to use DB subnet group %q - "+
						"a platform admin must create an RDSSubnetGroupGrant covering it", r.Name, namespace, r.DBSubnetGroupName),
					Retryable: true,
				}
			}
			continue
		}

		sgARN, sgErr := EnsureSecurityGroup(ctx, awsClient, ec2Client, k8sClient, namespace, crName, crUID, r.Name, r.DBSubnetGroupName, r.Engine, region, accountID, r.SharedWith)
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
			if r.Encryption.KMSKeyRef != nil {
				arn, ok := kms.ResolveSharedKeyARN(ctx, k8sClient, namespace, crName, *r.Encryption.KMSKeyRef)
				if !ok {
					if firstErr == nil {
						firstErr = &cloudctlaws.ReconcileError{
							Err: fmt.Errorf("instance %q: encryption.kmsKeyRef %s/%s/%s is not yet authorized "+
								"(producer must list this CR in the key's sharedWith) or does not exist yet",
								r.Name, r.Encryption.KMSKeyRef.Namespace, r.Encryption.KMSKeyRef.Name, r.Encryption.KMSKeyRef.ResourceName),
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

// isSubnetGroupAuthorized checks every RDSSubnetGroupGrant (cluster-
// scoped) for one covering dbSubnetGroupName that also lists namespace in
// allowedNamespaces. No grant covering this subnet group at all, or one
// that exists but doesn't list this namespace, both mean not authorized -
// there's no CR on the other end of this grant to default-trust the way
// sharedWith always has one.
func isSubnetGroupAuthorized(ctx context.Context, k8sClient client.Client, namespace, dbSubnetGroupName string) (bool, error) {
	var grants depsv1alpha1.RDSSubnetGroupGrantList
	if err := k8sClient.List(ctx, &grants); err != nil {
		return false, err
	}
	for _, g := range grants.Items {
		if g.Spec.DBSubnetGroupName != dbSubnetGroupName {
			continue
		}
		for _, ns := range g.Spec.AllowedNamespaces {
			if ns == namespace {
				return true, nil
			}
		}
	}
	return false, nil
}

func ensureInstance(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName, crUID, resourceName string,
	opts instanceOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	instanceID := cloudctlaws.ResourceName(namespace, crName, resourceType, resourceName, 63)

	describeOut, err := awsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &instanceID,
	})

	var notFound *types.DBInstanceNotFoundFault
	if errors.As(err, &notFound) {
		return createInstance(ctx, awsClient, namespace, crName, crUID, instanceID, resourceName, opts, ledger, recordEvent)
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up instance")
	}
	if len(describeOut.DBInstances) == 0 {
		return createInstance(ctx, awsClient, namespace, crName, crUID, instanceID, resourceName, opts, ledger, recordEvent)
	}

	instance := describeOut.DBInstances[0]
	instanceStatus := aws.ToString(instance.DBInstanceStatus)

	// "available" is the only status treated as usable; a small, explicit
	// set of genuinely terminal-failure statuses short-circuits with a
	// hard error. Everything else - including any status this operator
	// doesn't recognize - is treated as still transitioning and retried.
	// Deliberately the opposite default from DynamoDB's own status
	// handling: RDS's status is a free-form string, not a closed SDK
	// enum, with a much larger and less predictable set of non-terminal
	// values (e.g. "storage-optimization", "maintenance") than
	// DynamoDB's four states - silently treating an unrecognized one as
	// ready risks acting on a half-provisioned instance.
	switch instanceStatus {
	case "failed":
		return ledger, fmt.Errorf("instance %q entered status %q - this requires manual investigation, not an automatic retry", instanceID, instanceStatus)
	case "available":
		// proceed below
	default:
		return ledger, &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("instance %q is still %s", instanceID, instanceStatus),
			Retryable: true,
		}
	}

	instanceARN := aws.ToString(instance.DBInstanceArn)

	existingBeforeCheck := status.FindManagedResource(ledger, resourceType, resourceName)

	if existing := existingBeforeCheck; existing != nil && !status.NeedsRevalidation(*existing) {
		updated := *existing
		updated.DeletionPolicy = opts.deletionPolicy
		status.UpsertManagedResource(&ledger, updated)
		return ledger, nil
	}

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

	if recordEvent != nil && existingBeforeCheck != nil && existingBeforeCheck.State == depsv1alpha1.ManagedResourceStateCreating {
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

func recordVerified(ledger []depsv1alpha1.ManagedResource, ledgerName, arn string, deletionPolicy depsv1alpha1.DeletionPolicy) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	if existing := status.FindManagedResource(ledger, resourceType, ledgerName); existing != nil {
		createdAt = existing.CreatedAt
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           ledgerName,
		ARN:            arn,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		DeletionPolicy: deletionPolicy,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
	})
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
		Retryable: cloudctlaws.IsRetryable(err),
	}
}
