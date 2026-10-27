//go:build live

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

// Live tests run against a real AWS account - no LocalStack, no Kubernetes
// cluster. DynamoDB's own biggest real-AWS risk is the one no other
// resource type here has: table creation is asynchronous (CREATING then
// ACTIVE), so every test that needs an ACTIVE table has to genuinely wait
// for it rather than assume synchronous creation like SQS/SNS/S3. Beyond
// that, this tier targets: whether a missing table's error really is the
// same ResourceNotFoundException across DescribeTable/
// UpdateContinuousBackups/ListTagsOfResource (unlike SNS, which turned out
// to split "not found" across two different exception types), whether
// billing-mode switching and PITR's RecoveryPeriodInDays behave as the code
// assumes, and the real SSE/key-schema-mismatch paths. Skipped entirely
// unless real credentials resolve via the standard AWS credential chain.
// Run explicitly with whatever already authenticates your AWS CLI:
//
//	go test -tags=live ./internal/resources/dynamodb/... -v
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/livetest"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// skipUnlessLiveAWSCredentials skips the calling test unless the standard
// AWS credential chain actually resolves to something real, checked with a
// genuine, harmless STS call.
func skipUnlessLiveAWSCredentials(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}); err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}
	return cfg
}

func liveRegionAndAccount(t *testing.T, cfg aws.Config) (region, accountID string) {
	t.Helper()
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	return cfg.Region, *out.Account
}

// liveUniqueSuffix keeps each test's table name distinct so repeated runs
// against the same real account never collide. Unlike s3's own version,
// DynamoDB table names allow the same character set Go test names already
// use (letters, digits, underscore, hyphen, period), so no sanitizing is
// needed here.
func liveUniqueSuffix(t *testing.T) string {
	return "live-" + t.Name()[len("TestLive_"):] + "-" + time.Now().UTC().Format("150405")
}

func findEntry(ledger []depsv1alpha1.ManagedResource, name string) *depsv1alpha1.ManagedResource {
	for i := range ledger {
		if ledger[i].Name == name {
			return &ledger[i]
		}
	}
	return nil
}

// deleteTableIfExists retries through a transient rejection (the table
// still settling right after an async UpdateTable completes can briefly
// refuse DeleteTable too) rather than a single best-effort attempt - unlike
// sqs/sns/s3's disposable resources, a DynamoDB table left running from a
// silently swallowed cleanup failure is a real, ongoing cost.
func deleteTableIfExists(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		// The tables the operator creates are deletion protected. Turning it
		// off leaves the table UPDATING for a moment, which the delete below
		// reports as retryable.
		_, _ = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: &tableName, DeletionProtectionEnabled: aws.Bool(false)})
		_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &tableName})
		if err == nil || !cloudctlaws.IsRetryable(err) {
			return
		}
		time.Sleep(3 * time.Second)
	}
}

// waitForTableActive polls DescribeTable until the real table reports
// ACTIVE or timeout elapses - table creation is genuinely asynchronous
// against real AWS, unlike every other resource type in this operator.
func waitForTableActive(t *testing.T, client *dynamodb.Client, tableName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName})
		if err == nil && out.Table.TableStatus == types.TableStatusActive {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("real table %q never became ACTIVE within %s", tableName, timeout)
}

// waitForTableGone polls DescribeTable until it reports ResourceNotFoundException
// or timeout elapses - DeleteTable is itself asynchronous against real AWS,
// so a table checked immediately after a successful DeleteTable call is
// still found in DELETING status rather than actually gone.
func waitForTableGone(t *testing.T, client *dynamodb.Client, tableName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName})
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("real table %q was never actually removed within %s", tableName, timeout)
}

// retryWhileRetryable retries fn, sleeping between attempts, as long as it
// keeps failing with a *cloudctlaws.ReconcileError marked Retryable - for
// real, documented-transient AWS states (a table still UPDATING, PITR still
// being provisioned) that clear on their own within a bounded time rather
// than needing a fixed sleep tuned to guess how long they take.
func retryWhileRetryable(t *testing.T, timeout time.Duration, fn func() error) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable || time.Now().After(deadline) {
			return err
		}
		time.Sleep(3 * time.Second)
	}
}

// TestLive_Ensure_CreatesRealTableAsyncAndBecomesActiveWithTags guards the
// CREATING-then-ACTIVE lifecycle end to end against real AWS timing: the
// first Ensure call must record a ledger entry immediately (before the
// table is usable at all) and report a retryable error, and only once the
// table genuinely reaches ACTIVE does a second call verify ownership and
// tag it.
func TestLive_Ensure_CreatesRealTableAsyncAndBecomesActiveWithTags(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
			t.Fatalf("first Ensure() error = %v, want a retryable CREATING error", err)
		}
	}
	entry := findEntry(ledger, "sessions")
	if entry == nil {
		t.Fatal("expected a ledger entry recorded at CreateTable time, before the table is even ACTIVE")
	}
	if entry.State != depsv1alpha1.ManagedResourceStateCreating {
		t.Errorf("expected initial ledger state Creating, got %v", entry.State)
	}

	waitForTableActive(t, client, tableName, 3*time.Minute)

	ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("second Ensure() (once ACTIVE) error = %v", err)
	}
	entry = findEntry(ledger, "sessions")
	if entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected ledger state Verified once ACTIVE, got %v", entry.State)
	}

	tags, err := listAllTags(ctx, client, entry.ARN)
	if err != nil {
		t.Fatalf("real ListTagsOfResource() error = %v", err)
	}
	if !cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, "uid-1") {
		t.Errorf("real table tags don't satisfy IsOwnedBy: %+v", tags)
	}
}

// TestLive_Ensure_MissingTableErrorsUseDifferentExceptionTypesByOperation
// guards against the exact bug class already found once in sns's own live
// tier: a missing resource raising a different typed exception depending
// on which operation is called. Unlike sns's ListTagsForResource mismatch,
// this isn't a production bug here - nothing in this package currently
// branches on UpdateContinuousBackups' own "not found" error - but the
// split is real: DescribeTable and the tagging calls agree on
// ResourceNotFoundException, while UpdateContinuousBackups raises the
// backup-family-specific TableNotFoundException instead. Documented here
// so a future change relying on the wrong type doesn't repeat sns's
// mistake.
func TestLive_Ensure_MissingTableErrorsUseDifferentExceptionTypesByOperation(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	tableName := cloudctlaws.ResourceName("live", liveUniqueSuffix(t), resourceType, "never-created", 255)

	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	var nf1 *types.ResourceNotFoundException
	if !errors.As(err, &nf1) {
		t.Errorf("real DescribeTable on a nonexistent table: errors.As(ResourceNotFoundException) = false, got %v", err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        &tableName,
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)},
	})
	var nf2 *types.TableNotFoundException
	if !errors.As(err, &nf2) {
		t.Errorf("real UpdateContinuousBackups on a nonexistent table: errors.As(TableNotFoundException) = false, got %v", err)
	}

	arn := fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, accountID, tableName)
	_, err = client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: &arn})
	var nf3 *types.ResourceNotFoundException
	if !errors.As(err, &nf3) {
		t.Errorf("real ListTagsOfResource on a nonexistent table: errors.As(ResourceNotFoundException) = false, got %v", err)
	}
}

// TestLive_Ensure_SwitchesAProvisionedTableToPayPerRequestAndRefusesTheWayBack
// guards UpdateTable's real constraint that ProvisionedThroughput must be
// absent when switching to PayPerRequest (getting this wrong is rejected
// outright), that a billing-mode switch is asynchronous - the table sits in
// UPDATING until it completes, so a second change attempted before then is
// refused with a retryable "still UPDATING" error - and that a real on-demand
// table reports the billing mode the refusal of a switch back to provisioned
// capacity relies on.
func TestLive_Ensure_SwitchesAProvisionedTableToPayPerRequestAndRefusesTheWayBack(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{
		Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true,
		Overrides: &depsv1alpha1.DynamoDBOverrides{BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned},
	}}}
	ledger, _ := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	waitForTableActive(t, client, tableName, 3*time.Minute)
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() (once ACTIVE, provisioned) error = %v", err)
	}
	billingMode := func() *types.BillingModeSummary {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
		if err != nil {
			t.Fatalf("real DescribeTable() error = %v", err)
		}
		return out.Table.BillingModeSummary
	}

	spec.Resources[0].Overrides = nil
	if err := retryWhileRetryable(t, 2*time.Minute, func() error {
		_, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
		return err
	}); err != nil {
		t.Fatalf("Ensure() (switching to PayPerRequest) error = %v", err)
	}
	if summary := billingMode(); summary == nil || summary.BillingMode != types.BillingModePayPerRequest {
		t.Fatalf("expected real billing mode PayPerRequest after switching, got %+v", summary)
	}
	waitForTableActive(t, client, tableName, 3*time.Minute)

	spec.Resources[0].Overrides = &depsv1alpha1.DynamoDBOverrides{BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned}
	_, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.ProvisionedCapacityNeeded {
		t.Fatalf("expected a %s refusal for a real on-demand table, got %v", reasons.ProvisionedCapacityNeeded, err)
	}
}

// TestLive_Ensure_PITRRetentionDaysActuallyAppliesAndReportsBack guards
// RecoveryPeriodInDays as a real, accepted UpdateContinuousBackups
// parameter that DescribeContinuousBackups actually reports back
// unchanged, not just a field this package assumes exists. It also
// exercises a real transient state: continuous backups aren't immediately
// toggleable in the brief window right after a table reaches ACTIVE
// (ContinuousBackupsUnavailableException, "Backups are being enabled...
// Please retry later"), which IsRetryable now classifies as retryable.
func TestLive_Ensure_PITRRetentionDaysActuallyAppliesAndReportsBack(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	retention := int32(20)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{
			Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true,
			Backup: &depsv1alpha1.DynamoDBBackupSpec{Enabled: true, RetentionDays: &retention},
		},
	}}
	ledger, _ := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	waitForTableActive(t, client, tableName, 3*time.Minute)
	if err := retryWhileRetryable(t, 2*time.Minute, func() error {
		_, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
		return err
	}); err != nil {
		t.Fatalf("Ensure() (once ACTIVE, enabling PITR) error = %v", err)
	}

	backupOut, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: &tableName})
	if err != nil {
		t.Fatalf("real DescribeContinuousBackups() error = %v", err)
	}
	pitr := backupOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription
	if pitr == nil || pitr.PointInTimeRecoveryStatus != types.PointInTimeRecoveryStatusEnabled {
		t.Fatalf("expected real PITR to be enabled, got %+v", pitr)
	}
	if pitr.RecoveryPeriodInDays == nil || *pitr.RecoveryPeriodInDays != retention {
		t.Errorf("real RecoveryPeriodInDays = %v, want %d", pitr.RecoveryPeriodInDays, retention)
	}
}

// TestLive_Ensure_SharedKMSKeyEncryptsRealTable confirms a KMS key applies
// correctly through DynamoDB's own SSESpecification/SSEDescription shape. The
// key is the shared live-test key; the dedicated-key path is proven by sqs.
func TestLive_Ensure_SharedKMSKeyEncryptsRealTable(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })
	key := livetest.SharedKey(t, cfg)

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: livetest.SharedKeyName}},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, []depsv1alpha1.ManagedResource{key}, nil, nil)
	if err != nil {
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
			t.Fatalf("first Ensure() error = %v, want a retryable CREATING error", err)
		}
	}

	waitForTableActive(t, client, tableName, 3*time.Minute)
	if _, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() (once ACTIVE) error = %v", err)
	}

	describeOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		t.Fatalf("real DescribeTable() error = %v", err)
	}
	if describeOut.Table.SSEDescription == nil || aws.ToString(describeOut.Table.SSEDescription.KMSMasterKeyArn) != key.ARN {
		t.Errorf("real table's SSE key = %v, want the shared key %q", describeOut.Table.SSEDescription, key.ARN)
	}
}

// TestLive_Ensure_RefusesAdoptingTableWithMismatchedKeySchema confirms
// tableKeySchema correctly parses a real DescribeTable response's
// KeySchema, and that the mismatch refusal actually fires against a
// genuinely real table rather than a fake's hand-built KeySchemaElement
// slice.
func TestLive_Ensure_RefusesAdoptingTableWithMismatchedKeySchema(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            &tableName,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("real CreateTable() (setup) error = %v", err)
	}
	waitForTableActive(t, client, tableName, 3*time.Minute)

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil); err == nil {
		t.Error("expected Ensure to refuse adopting a real table whose key schema doesn't match the declared partitionKey")
	}
}

// TestLive_Cleanup_BlocksDeletingTableWithARealItemThenSucceedsOnceEmpty
// exercises tableIsEmpty's real, strongly-consistent count Scan, not a
// fake's hand-set field.
func TestLive_Cleanup_BlocksDeletingTableWithARealItemThenSucceedsOnceEmpty(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: false},
	}}
	ledger, _ := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	waitForTableActive(t, client, tableName, 3*time.Minute)
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() (once ACTIVE) error = %v", err)
	}

	itemKey := map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "keep-this-table-non-empty"}}
	if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &tableName, Item: itemKey}); err != nil {
		t.Fatalf("real PutItem() error = %v", err)
	}

	// First pass only marks pending (quiet window); backdate it so the next
	// pass evaluates the real emptiness check instead of waiting.
	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	entry, _ := status.FindManagedResource(ledger, resourceType, "sessions")
	past := metav1.NewTime(time.Now().Add(-2 * deletionQuietWindow))
	entry.PendingDeletionSince = &past
	status.UpsertManagedResource(&ledger, entry)

	ledger, _, err = Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if _, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName}); err != nil {
		t.Fatal("expected the real table with a genuine item still in it to survive Cleanup, but it's gone")
	}

	if _, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &tableName, Key: itemKey}); err != nil {
		t.Fatalf("real DeleteItem() error = %v", err)
	}
	if _, _, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil); err != nil {
		t.Fatalf("third Cleanup() error = %v", err)
	}
	// DeleteTable is itself asynchronous, like every other DynamoDB
	// control-plane call here - the table enters DELETING and DescribeTable
	// keeps finding it for a while, so "gone" has to be waited for rather
	// than checked once immediately after.
	waitForTableGone(t, client, tableName, 3*time.Minute)
}

// TestLive_Ensure_RefusesTableOwnedByADifferentRealCR confirms the
// stale-UID/ownership check against real, round-tripped tags read via
// DynamoDB's own paginated ListTagsOfResource shape.
func TestLive_Ensure_RefusesTableOwnedByADifferentRealCR(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	ownerSpec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}
	ledger, _ := Ensure(ctx, client, nil, nil, namespace, crName, "owner-uid", ownerSpec, nil, nil, nil)
	waitForTableActive(t, client, tableName, 3*time.Minute)
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "owner-uid", ownerSpec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() (establishing the real owner, once ACTIVE) error = %v", err)
	}

	intruderSpec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true, Adopt: true},
	}}
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "intruder-uid", intruderSpec, nil, nil, nil); err == nil {
		t.Error("expected Ensure to refuse a real table already owned by a different CR's UID, even with adopt:true")
	}
}

// TestLive_Cleanup_RetainedTableIsLetGoAndAMissingOneIsGone checks that
// letting go removes the tags and the entry, and that a missing table is
// recognized as gone.
func TestLive_Cleanup_RetainedTableIsLetGoAndAMissingOneIsGone(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyRetain},
	}}
	ledger, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil)
	if err != nil {
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
			t.Fatalf("first Ensure() error = %v", err)
		}
	}
	waitForTableActive(t, client, tableName, 3*time.Minute)
	if ledger, err = Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, ledger, nil, nil); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	ledger, results, err := Cleanup(ctx, client, namespace, crName, "uid-1", &depsv1alpha1.DynamoDBSpec{}, ledger, false, false, nil)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(results) != 1 || results[0].Reason != CleanupReasonRetained {
		t.Errorf("expected one Retained result, got %+v", results)
	}
	if findEntry(ledger, "sessions") != nil {
		t.Error("expected the retained table to leave the ledger")
	}

	tableARN := "arn:aws:dynamodb:" + region + ":" + accountID + ":table/" + tableName
	// Tag changes can take a few seconds to show.
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(3 * time.Second) {
		tags, err := listAllTags(ctx, client, tableARN)
		if err != nil {
			t.Fatalf("the retained table should still exist: %v", err)
		}
		if got := tagsToMap(tags); !cloudctlaws.IsOwnedBy(got, namespace, crName, "uid-1") && got[cloudctlaws.ReleasedFromTagKey] != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the ownership tags are still on the retained table after a minute")
		}
	}

	missing := depsv1alpha1.ManagedResource{Type: resourceType, Name: "never-created", ARN: "arn:aws:dynamodb:" + region + ":" + accountID + ":table/" + cloudctlaws.ResourceName(namespace, crName, resourceType, "never-created", 255)}
	_, gone, err := relinquishIfStillTagged(ctx, client, namespace, crName, "uid-1", missing, nil)
	if err != nil || !gone {
		t.Errorf("relinquishIfStillTagged() on a missing table gone=%v err=%v, want gone", gone, err)
	}
}

// TestLive_Ensure_TakesOverATableFromAPreviousUID checks a restored CR is
// refused a table tagged with its old UID, and takes it over when told which.
func TestLive_Ensure_TakesOverATableFromAPreviousUID(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	client := dynamodb.NewFromConfig(cfg)
	region, accountID := liveRegionAndAccount(t, cfg)
	ctx := context.Background()
	namespace, crName := "live", liveUniqueSuffix(t)
	tableName := cloudctlaws.ResourceName(namespace, crName, resourceType, "sessions", 255)
	tableARN := "arn:aws:dynamodb:" + region + ":" + accountID + ":table/" + tableName
	t.Cleanup(func() { deleteTableIfExists(t, client, tableName) })
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete, Force: true},
	}}

	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-1", spec, nil, nil, nil); err != nil {
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
			t.Fatalf("Ensure() (create) error = %v", err)
		}
	}
	waitForTableActive(t, client, tableName, 3*time.Minute)
	if _, err := Ensure(ctx, client, nil, nil, namespace, crName, "uid-2", spec, nil, nil, nil); err == nil {
		t.Fatal("expected a new UID to be refused without adopt-from-uid")
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, "uid-1")
	if _, err := Ensure(takeover, client, nil, nil, namespace, crName, "uid-2", spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (takeover) error = %v", err)
	}
	// Tag changes can take a few seconds to show.
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(3 * time.Second) {
		tags, err := listAllTags(ctx, client, tableARN)
		if err != nil {
			t.Fatalf("listAllTags() error = %v", err)
		}
		if cloudctlaws.IsOwnedBy(tagsToMap(tags), namespace, crName, "uid-2") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the table never got the new UID's tags")
		}
	}
}
