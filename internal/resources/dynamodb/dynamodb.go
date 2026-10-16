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

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// dynamodbAPI is the subset of the DynamoDB client this package needs.
// Stays private here until a second real consumer (controller-level
// envtest fakes) needs to substitute a fake here too.
type dynamodbAPI interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, in *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	UpdateTable(ctx context.Context, in *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	ListTagsOfResource(ctx context.Context, in *dynamodb.ListTagsOfResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error)
	TagResource(ctx context.Context, in *dynamodb.TagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error)
	UntagResource(ctx context.Context, in *dynamodb.UntagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error)
	UpdateContinuousBackups(ctx context.Context, in *dynamodb.UpdateContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error)
	DescribeContinuousBackups(ctx context.Context, in *dynamodb.DescribeContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

const resourceType = "dynamodb"

// keyAttributeType is the attribute type used for both partition and sort
// keys. The CRD only exposes key attribute *names*, not types, keeping the
// schema surface minimal for the overwhelmingly common case; a typed
// override can be added later if a real need for numeric/binary keys shows
// up.
const keyAttributeType = types.ScalarAttributeTypeS

// provisionedDefaultCapacityUnits is the read/write capacity used when a
// table opts into Provisioned billing mode. This operator doesn't expose
// raw RCU/WCU tuning — that's exactly the kind of "Terraform-in-YAML"
// configuration surface the CRD avoids — so Provisioned mode gets AWS's
// own long-standing default capacity rather than a number this package
// would otherwise have to invent an opinion on. A workload that outgrows
// this should scale from here via Application Auto Scaling directly.
const provisionedDefaultCapacityUnits = int64(5)

// defaultPITRRetentionDays is AWS's own default retention window, applied
// when spec.backup.retentionDays is left unset.
const defaultPITRRetentionDays = int32(35)

type tableOptions struct {
	deletionPolicy depsv1alpha1.DeletionPolicy
	force, adopt   bool
	partitionKey   string
	sortKey        string
	backupEnabled  bool
	retentionDays  *int32
	billingMode    depsv1alpha1.DynamoDBBillingMode
	kmsKeyARN      *string
	// checkAdoptedKey vets the key an existing table is encrypted with
	// before it is adopted; nil skips the check.
	checkAdoptedKey func(ctx context.Context, currentKey string) error
}

// billingModeFor maps the CRD's decision-level billing mode to the SDK's
// type, defaulting to on-demand for the zero value (Overrides unset, or
// Overrides.BillingMode left empty).
func billingModeFor(m depsv1alpha1.DynamoDBBillingMode) types.BillingMode {
	if m == depsv1alpha1.DynamoDBBillingModeProvisioned {
		return types.BillingModeProvisioned
	}
	return types.BillingModePayPerRequest
}

// Ensure reconciles every declared DynamoDB table against AWS, updating the
// ownership ledger as it goes. kmsClient is only ever touched when a
// resource actually declares encryption.enabled — a CR that never uses it
// can pass nil.
func Ensure(
	ctx context.Context,
	awsClient dynamodbAPI,
	kmsClient cloudctlaws.KMSClient,
	k8sClient client.Client,
	namespace,
	crName,
	crUID string,
	spec *depsv1alpha1.DynamoDBSpec,
	ledger []depsv1alpha1.ManagedResource,
	checkpoint status.Checkpoint,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	if spec == nil {
		return ledger, nil
	}

	var firstErr error
	for _, t := range spec.Resources {
		opts := tableOptions{
			deletionPolicy: t.DeletionPolicy,
			force:          t.Force,
			adopt:          t.Adopt,
			partitionKey:   t.PartitionKey,
			sortKey:        t.SortKey,
		}
		if t.Backup != nil {
			opts.backupEnabled = t.Backup.Enabled
			opts.retentionDays = t.Backup.RetentionDays
		}
		if t.Overrides != nil {
			opts.billingMode = t.Overrides.BillingMode
		}
		if t.Encryption != nil {
			if t.Encryption.KMSKeyRef != "" {
				arn, ok := kms.SharedKeyARN(ledger, t.Encryption.KMSKeyRef)
				if !ok {
					if firstErr == nil {
						firstErr = &cloudctlaws.ReconcileError{
							Err:       fmt.Errorf("table %q: encryption.kmsKeyRef %q is not ready yet - declare it under kms.resources", t.Name, t.Encryption.KMSKeyRef),
							Retryable: true,
						}
					}
					continue
				}
				opts.kmsKeyARN = &arn
			}
			if t.Encryption.Enabled {
				arn, updatedLedger, err := kms.EnsureDedicatedKey(ctx, kmsClient, namespace, crName, crUID, resourceType, t.Name, t.DeletionPolicy, ledger, checkpoint, recordEvent)
				ledger = updatedLedger
				if err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("table %q: encryption key: %w", t.Name, err)
					}
					continue
				}
				opts.kmsKeyARN = &arn
			}
		}

		opts.checkAdoptedKey = kms.AdoptedKeyCheck(kmsClient, opts.kmsKeyARN)

		var err error
		ledger, err = ensureTable(ctx, awsClient, namespace, crName, crUID, t.Name, opts, ledger, recordEvent)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("table %q: %w", t.Name, err)
		}
	}
	return ledger, firstErr
}

func ensureTable(
	ctx context.Context,
	awsClient dynamodbAPI,
	namespace,
	crName,
	crUID,
	resourceName string,
	opts tableOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, resourceName, 255)

	describeOut, err := awsClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})

	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		// Creation only ever happens for a table this CR has never had. One
		// the ledger knows but DynamoDB no longer has was deleted out-of-band,
		// and a replacement would be an empty table reported Ready.
		if entry, found := status.FindManagedResource(ledger, resourceType, resourceName); found {
			return ledger, tableMissingError(entry, tableName, recordEvent)
		}
		return createTable(ctx, awsClient, namespace, crName, crUID, tableName, resourceName, opts, ledger, recordEvent)
	}
	if err != nil {
		return ledger, wrapAWSError(err, "looking up table")
	}

	table := describeOut.Table
	switch table.TableStatus {
	case types.TableStatusCreating, types.TableStatusUpdating:
		// Not usable yet — tags, ownership, and drift-correction all wait
		// for ACTIVE. Retryable, not an error: this resolves on its own
		// within the normal drift-detection requeue interval.
		return ledger, &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("table %q is still %s", tableName, table.TableStatus),
			Retryable: true,
		}
	case types.TableStatusDeleting:
		return ledger, &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("table %q is being deleted (by this operator or out-of-band) — refusing to recreate or adopt while DELETING", tableName),
			Retryable: true,
		}
	}

	// ACTIVE (or any status DynamoDB might introduce later — treated the
	// same as ACTIVE rather than blocking on an exhaustive switch).
	tableArn := *table.TableArn

	// Captured before the trust-window check so it's still available further
	// down to distinguish "our own table just finished CREATING" (fires
	// TableActive) from "a foreign table being adopted for the first time"
	// (fires TableAdopted) - the trust-window branch below re-derives its
	// own copy since Go scopes the if-statement's own existing to that block.
	existingBeforeCheck, existingBeforeCheckFound := status.FindManagedResource(ledger, resourceType, resourceName)

	if existingBeforeCheckFound && !status.NeedsRevalidation(existingBeforeCheck) {
		// Still within the trust window - skip re-verifying ownership via
		// tags (a paginated ListTagsOfResource call), but attribute drift
		// correction is a different concern and still runs every reconcile
		// regardless of the trust window - unlike SQS/SNS, DynamoDB always
		// has something to compare (billing mode and PITR are never both
		// "unconfigured"), so there's no legitimate case where this whole
		// round trip could be skipped outright. Local-only fields
		// (deletionPolicy/force) can still change from a spec edit with no
		// AWS call needed, so refresh those against the cached entry;
		// LastVerifiedAt stays as it was until the window actually expires
		// and a real ownership check runs again.
		updated := existingBeforeCheck
		updated.DeletionPolicy = opts.deletionPolicy
		updated.Force = opts.force
		status.UpsertManagedResource(&ledger, updated)

		if err := reconcileTableAttributes(ctx, awsClient, tableName, opts); err != nil {
			return ledger, wrapAWSError(err, "reconciling table attributes")
		}
		return ledger, nil
	}

	tags, tErr := listAllTags(ctx, awsClient, tableArn)
	if tErr != nil {
		return ledger, wrapAWSError(tErr, "reading table tags")
	}
	currentTags := tagsToMap(tags)

	if !cloudctlaws.IsOwnedBy(currentTags, namespace, crName, crUID) {
		transferred, ownErr := cloudctlaws.CheckExistingOwnership(ctx, "table", tableName, currentTags, namespace, crName, crUID, opts.adopt)
		if ownErr != nil {
			return ledger, ownErr
		}

		// Refuse the adoption outright on a key-schema mismatch, rather than
		// adopting anyway and only failing later, opaquely, at the first
		// PutItem/Query the workload makes. AWS never allows changing a
		// table's key schema after creation, and the CRD's own CEL rules
		// already block editing partitionKey/sortKey on an already-
		// reconciled CR — so the only way this can happen is adopting a
		// pre-existing table that happens to sit at this CR's deterministic
		// name with a different schema.
		if actualPartitionKey, actualSortKey := tableKeySchema(table.KeySchema); actualPartitionKey != opts.partitionKey || actualSortKey != opts.sortKey {
			return ledger, fmt.Errorf(
				"table %q exists with key schema (partitionKey=%q, sortKey=%q) that doesn't match this CR's declared (partitionKey=%q, sortKey=%q) — AWS doesn't support changing a table's key schema, so it can't be adopted with mismatched keys",
				tableName, actualPartitionKey, actualSortKey, opts.partitionKey, opts.sortKey,
			)
		}

		if opts.checkAdoptedKey != nil {
			currentKey := ""
			if table.SSEDescription != nil {
				currentKey = aws.ToString(table.SSEDescription.KMSMasterKeyArn)
			}
			if err := opts.checkAdoptedKey(ctx, currentKey); err != nil {
				return ledger, fmt.Errorf("table %q can't be adopted: %w", tableName, err)
			}
		}

		merged := cloudctlaws.MergeTags(currentTags, ownerTags(namespace, crName, crUID))
		if _, tagErr := awsClient.TagResource(ctx, &dynamodb.TagResourceInput{
			ResourceArn: &tableArn,
			Tags:        mapToTags(merged),
		}); tagErr != nil {
			return ledger, wrapAWSError(tagErr, "adopting table (tagging)")
		}
		if recordEvent != nil {
			if transferred {
				recordEvent("Normal", cloudctlaws.EventOwnershipTransferred, fmt.Sprintf("Took over DynamoDB table %s from a previous incarnation of this CR", tableArn))
			} else {
				recordEvent("Normal", "TableAdopted", fmt.Sprintf("Adopted existing DynamoDB table %s under management", tableArn))
			}
		}
	}

	if err := reconcileTableAttributes(ctx, awsClient, tableName, opts); err != nil {
		return ledger, wrapAWSError(err, "reconciling table attributes")
	}

	if recordEvent != nil && existingBeforeCheckFound && existingBeforeCheck.State == depsv1alpha1.ManagedResourceStateCreating {
		recordEvent("Normal", "TableActive", fmt.Sprintf("DynamoDB table %s is now ACTIVE", tableArn))
	}

	return recordVerified(ledger, resourceName, tableArn, opts.deletionPolicy, opts.force), nil
}

// tableKeySchema extracts the partition (HASH) and sort (RANGE) key
// attribute names from a table's key schema, as DescribeTable reports it.
func tableKeySchema(schema []types.KeySchemaElement) (partitionKey, sortKey string) {
	for _, e := range schema {
		name := aws.ToString(e.AttributeName)
		switch e.KeyType {
		case types.KeyTypeHash:
			partitionKey = name
		case types.KeyTypeRange:
			sortKey = name
		}
	}
	return partitionKey, sortKey
}

func createTable(
	ctx context.Context,
	awsClient dynamodbAPI,
	namespace, crName, crUID, tableName, resourceName string,
	opts tableOptions,
	ledger []depsv1alpha1.ManagedResource,
	recordEvent status.EventRecorder,
) ([]depsv1alpha1.ManagedResource, error) {
	attrDefs := []types.AttributeDefinition{
		{AttributeName: aws.String(opts.partitionKey), AttributeType: keyAttributeType},
	}
	keySchema := []types.KeySchemaElement{
		{AttributeName: aws.String(opts.partitionKey), KeyType: types.KeyTypeHash},
	}
	if opts.sortKey != "" {
		attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: aws.String(opts.sortKey), AttributeType: keyAttributeType})
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: aws.String(opts.sortKey), KeyType: types.KeyTypeRange})
	}

	input := &dynamodb.CreateTableInput{
		TableName:            &tableName,
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		BillingMode:          billingModeFor(opts.billingMode),
		Tags:                 mapToTags(ownerTags(namespace, crName, crUID)),
	}
	if opts.billingMode == depsv1alpha1.DynamoDBBillingModeProvisioned {
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(provisionedDefaultCapacityUnits),
			WriteCapacityUnits: aws.Int64(provisionedDefaultCapacityUnits),
		}
	}
	if opts.kmsKeyARN != nil {
		input.SSESpecification = &types.SSESpecification{
			Enabled:        aws.Bool(true),
			SSEType:        types.SSETypeKms,
			KMSMasterKeyId: opts.kmsKeyARN,
		}
	}

	createOut, err := awsClient.CreateTable(ctx, input)
	if err != nil {
		// ResourceInUseException here (e.g. a very fast delete-then-recreate
		// racing AWS's own cleanup) is covered by the shared IsRetryable
		// classification wrapAWSError uses below, same as every other
		// DynamoDB control-plane call in this package.
		return ledger, wrapAWSError(err, "creating table")
	}
	if recordEvent != nil {
		recordEvent("Normal", "TableCreating", fmt.Sprintf("Creating DynamoDB table %s (waiting for ACTIVE)", *createOut.TableDescription.TableArn))
	}

	now := metav1.Now()
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           resourceName,
		ARN:            *createOut.TableDescription.TableArn,
		State:          depsv1alpha1.ManagedResourceStateCreating,
		DeletionPolicy: opts.deletionPolicy,
		Force:          opts.force,
		CreatedAt:      now,
	})

	// BillingMode and Tags are already set atomically above. PITR and any
	// further drift-correction wait for ACTIVE — the next reconcile (via
	// the normal drift-detection requeue) picks this up through the
	// CREATING/UPDATING branch in ensureTable until DescribeTable reports
	// ACTIVE.
	return ledger, nil
}

// reconcileTableAttributes corrects drift on an already-ACTIVE table's
// mutable configuration: BillingMode (driven by spec.overrides.billingMode,
// defaulting to on-demand) and point-in-time recovery (driven by
// spec.backup.enabled).
func reconcileTableAttributes(ctx context.Context, awsClient dynamodbAPI, tableName string, opts tableOptions) error {
	describeOut, err := awsClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		return fmt.Errorf("reading current table config: %w", err)
	}

	desiredMode := billingModeFor(opts.billingMode)
	// A nil BillingModeSummary means this table predates the field's
	// introduction — every table AWS ever created before PAY_PER_REQUEST
	// existed ran in what's now called Provisioned mode, so nil means
	// Provisioned, not "unset"/PayPerRequest. Treating it as PayPerRequest
	// here would make a legacy adopted table that genuinely wants
	// Provisioned mode fail this comparison and call UpdateTable every
	// single reconcile even though nothing actually needs to change.
	currentMode := types.BillingModeProvisioned
	if describeOut.Table.BillingModeSummary != nil {
		currentMode = describeOut.Table.BillingModeSummary.BillingMode
	}

	if currentMode != desiredMode {
		updateInput := &dynamodb.UpdateTableInput{
			TableName:   &tableName,
			BillingMode: desiredMode,
		}
		if desiredMode == types.BillingModeProvisioned {
			updateInput.ProvisionedThroughput = &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(provisionedDefaultCapacityUnits),
				WriteCapacityUnits: aws.Int64(provisionedDefaultCapacityUnits),
			}
		}
		if _, err := awsClient.UpdateTable(ctx, updateInput); err != nil {
			return fmt.Errorf("correcting billing mode: %w", err)
		}
	}

	// Only ever corrects the "enable, or point at a different key" direction
	// — same accepted gap as SQS/SNS's own encryption drift correction:
	// removing encryption from spec doesn't proactively disable it on an
	// already-encrypted table.
	if opts.kmsKeyARN != nil {
		currentKeyARN := ""
		if describeOut.Table.SSEDescription != nil {
			currentKeyARN = aws.ToString(describeOut.Table.SSEDescription.KMSMasterKeyArn)
		}
		if currentKeyARN != *opts.kmsKeyARN {
			if _, err := awsClient.UpdateTable(ctx, &dynamodb.UpdateTableInput{
				TableName: &tableName,
				SSESpecification: &types.SSESpecification{
					Enabled:        aws.Bool(true),
					SSEType:        types.SSETypeKms,
					KMSMasterKeyId: opts.kmsKeyARN,
				},
			}); err != nil {
				return fmt.Errorf("correcting server-side encryption key: %w", err)
			}
		}
	}

	// DescribeContinuousBackups/UpdateContinuousBackups raise
	// TableNotFoundException for a missing table, not the
	// ResourceNotFoundException DescribeTable and the tagging calls use -
	// this call site doesn't need to distinguish "not found" from any other
	// error, but a future change that does should check the right type.
	backupOut, err := awsClient.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: &tableName})
	if err != nil {
		return fmt.Errorf("reading point-in-time recovery status: %w", err)
	}
	var pitrDesc *types.PointInTimeRecoveryDescription
	if backupOut.ContinuousBackupsDescription != nil {
		pitrDesc = backupOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription
	}
	currentlyEnabled := pitrDesc != nil && pitrDesc.PointInTimeRecoveryStatus == types.PointInTimeRecoveryStatusEnabled

	desiredRetentionDays := defaultPITRRetentionDays
	if opts.retentionDays != nil {
		desiredRetentionDays = *opts.retentionDays
	}

	// RetentionDays only means anything while PITR is actually enabled —
	// comparing it while disabled risks a perpetual, pointless drift
	// correction against whatever AWS happens to report for a disabled
	// table (e.g. a stale value left over from when it was last enabled).
	needsUpdate := currentlyEnabled != opts.backupEnabled
	if opts.backupEnabled && currentlyEnabled && (pitrDesc.RecoveryPeriodInDays == nil || *pitrDesc.RecoveryPeriodInDays != desiredRetentionDays) {
		needsUpdate = true
	}

	if needsUpdate {
		if _, err := awsClient.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
			TableName: &tableName,
			PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
				PointInTimeRecoveryEnabled: aws.Bool(opts.backupEnabled),
				RecoveryPeriodInDays:       aws.Int32(desiredRetentionDays),
			},
		}); err != nil {
			return fmt.Errorf("correcting point-in-time recovery: %w", err)
		}
	}

	return nil
}

func recordVerified(ledger []depsv1alpha1.ManagedResource, ledgerName, arn string, deletionPolicy depsv1alpha1.DeletionPolicy, force bool) []depsv1alpha1.ManagedResource {
	now := metav1.Now()
	createdAt := now
	if existing, existingFound := status.FindManagedResource(ledger, resourceType, ledgerName); existingFound {
		createdAt = existing.CreatedAt
	}
	status.UpsertManagedResource(&ledger, depsv1alpha1.ManagedResource{
		Type:           resourceType,
		Name:           ledgerName,
		ARN:            arn,
		State:          depsv1alpha1.ManagedResourceStateVerified,
		DeletionPolicy: deletionPolicy,
		Force:          force,
		CreatedAt:      createdAt,
		LastVerifiedAt: &now,
	})
	return ledger
}

func listAllTags(ctx context.Context, awsClient dynamodbAPI, resourceArn string) ([]types.Tag, error) {
	var all []types.Tag
	var nextToken *string
	for {
		out, err := awsClient.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
			ResourceArn: &resourceArn,
			NextToken:   nextToken,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, out.Tags...)
		if out.NextToken == nil {
			return all, nil
		}
		nextToken = out.NextToken
	}
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
		tags = append(tags, types.Tag{Key: &k, Value: &v})
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

// creationVisibilityGrace is how long a just-created table may be missing
// from DescribeTable before that is treated as it having been deleted:
// DynamoDB can briefly answer "no such table" right after CreateTable.
const creationVisibilityGrace = 5 * time.Minute

const reasonTableMissing = "TableMissing"

func tableMissingError(entry depsv1alpha1.ManagedResource, tableName string, recordEvent status.EventRecorder) error {
	if entry.State == depsv1alpha1.ManagedResourceStateCreating && time.Since(entry.CreatedAt.Time) < creationVisibilityGrace {
		return &cloudctlaws.ReconcileError{
			Err:       fmt.Errorf("table %q was just created and is not visible yet", tableName),
			Retryable: true,
		}
	}
	if recordEvent != nil {
		recordEvent("Warning", "TableMissing", fmt.Sprintf("DynamoDB table %s no longer exists in AWS and will not be recreated", entry.ARN))
	}
	return &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("DynamoDB table %q (%s) no longer exists in AWS: it was deleted out-of-band, and this operator will not create an empty "+
			"replacement. Restore it under the same name from a backup and set adopt: true, "+
			"or remove it from spec and add it again to start fresh", tableName, entry.ARN),
		Reason: reasonTableMissing,
	}
}
