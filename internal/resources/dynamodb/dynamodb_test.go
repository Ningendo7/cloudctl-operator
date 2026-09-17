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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func TestEnsure_CreatesTableWithOwnerTagsAndOnDemandBilling(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"},
	}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	table, ok := client.tables[tableName]
	if !ok {
		t.Fatalf("expected table %q to be created", tableName)
	}
	if table.billingMode != types.BillingModePayPerRequest {
		t.Errorf("expected BillingMode PAY_PER_REQUEST, got %s", table.billingMode)
	}
	if !cloudctlaws.IsOwnedBy(table.tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the table to be tagged as owned by this CR at creation")
	}

	entry := status.FindManagedResource(ledger, "dynamodb", "sessions")
	if entry == nil {
		t.Fatal("expected a ledger entry after creation")
	}
	if entry.State != depsv1alpha1.ManagedResourceStateCreating {
		t.Errorf("expected a freshly-created table to be recorded as Creating (DynamoDB creation is asynchronous), got %s", entry.State)
	}
	if entry.ARN != table.arn {
		t.Errorf("expected the ledger ARN to match the created table's ARN, got %s want %s", entry.ARN, table.arn)
	}
}

func TestEnsure_CreatesCompositeKeyWhenSortKeySet(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "events", PartitionKey: "pk", SortKey: "sk"},
	}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	// The fake doesn't retain KeySchema itself (not needed for any other
	// assertion in this suite), so this test's real value is simply that
	// CreateTable didn't error with a sort key set - a malformed
	// AttributeDefinitions/KeySchema pairing would surface as an error from
	// a real AWS call, which this package's own logic must construct
	// correctly regardless of what the fake checks.
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "events")
	if _, ok := client.tables[tableName]; !ok {
		t.Fatalf("expected table %q to be created", tableName)
	}
}

func TestEnsure_MovesToVerifiedOnceTableIsActive(t *testing.T) {
	// Regression test for the async-creation gap: a table already ACTIVE
	// by the time Ensure is called again (e.g. next reconcile after
	// creation) must progress from Creating to Verified.
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"},
	}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if entry := status.FindManagedResource(ledger, "dynamodb", "sessions"); entry == nil || entry.State != depsv1alpha1.ManagedResourceStateCreating {
		t.Fatalf("test setup broken: expected Creating after first Ensure(), got %+v", entry)
	}

	ledger, err = Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	entry := status.FindManagedResource(ledger, "dynamodb", "sessions")
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Errorf("expected the table to be Verified once ACTIVE, got %+v", entry)
	}
	if entry.LastVerifiedAt == nil {
		t.Error("expected LastVerifiedAt to be set once verified")
	}
}

func TestEnsure_ReturnsRetryableErrorWhileTableIsStillCreating(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:    "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status: types.TableStatusCreating,
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error while the table is still CREATING")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if !reconcileErr.Retryable {
		t.Errorf("expected a still-CREATING table to be reported as retryable, got %v", err)
	}
}

func TestEnsure_EnablesPointInTimeRecoveryWhenBackupRequested(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Backup: &depsv1alpha1.DynamoDBBackupSpec{Enabled: true}},
	}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	// Second pass: table is now ACTIVE, so PITR reconciliation actually runs.
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	if !client.tables[tableName].pitrEnabled {
		t.Error("expected point-in-time recovery to be enabled")
	}
}

func TestEnsure_SetsCustomRetentionDaysWhenBackupEnabled(t *testing.T) {
	client := newFakeDynamoDB()
	retentionDays := int32(14)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Backup: &depsv1alpha1.DynamoDBBackupSpec{
			Enabled: true, RetentionDays: &retentionDays,
		}},
	}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	table := client.tables[tableName]
	if !table.pitrEnabled {
		t.Fatal("expected PITR to be enabled")
	}
	if table.retentionDays != 14 {
		t.Errorf("expected retention days to be set to 14, got %d", table.retentionDays)
	}
}

func TestEnsure_DefaultsToThirtyFiveDayRetentionWhenUnset(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Backup: &depsv1alpha1.DynamoDBBackupSpec{Enabled: true}},
	}}

	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger); err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}

	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	if got := client.tables[tableName].retentionDays; got != 35 {
		t.Errorf("expected AWS's default 35-day retention when unset, got %d", got)
	}
}

func TestEnsure_CorrectsRetentionDaysDriftOnAlreadyEnabledTable(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:           "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:        types.TableStatusActive,
		billingMode:   types.BillingModePayPerRequest,
		pitrEnabled:   true,
		retentionDays: 35,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	retentionDays := int32(7)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Backup: &depsv1alpha1.DynamoDBBackupSpec{
			Enabled: true, RetentionDays: &retentionDays,
		}},
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if got := client.tables[tableName].retentionDays; got != 7 {
		t.Errorf("expected retention days drift to be corrected to 7, got %d", got)
	}
}

func TestEnsure_DoesNotTouchContinuousBackupsWhenNothingChanged(t *testing.T) {
	// Idempotency regression: once PITR is enabled with the desired
	// retention, a repeat reconcile must not call UpdateContinuousBackups
	// at all - proven here by making any such call an unmistakable failure.
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:           "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:        types.TableStatusActive,
		billingMode:   types.BillingModePayPerRequest,
		pitrEnabled:   true,
		retentionDays: 14,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
	client.updateContinuousBackupsErr = errors.New("should not be called: nothing changed")

	retentionDays := int32(14)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Backup: &depsv1alpha1.DynamoDBBackupSpec{
			Enabled: true, RetentionDays: &retentionDays,
		}},
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v — expected no UpdateContinuousBackups call when nothing changed", err)
	}
}

func TestEnsure_DoesNotCompareRetentionDaysWhilePITRDisabled(t *testing.T) {
	// Perpetual-delta guard: retention days is meaningless while PITR is
	// disabled, so it must never be compared (and never trigger a spurious
	// UpdateContinuousBackups call) in that state.
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:         "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:      types.TableStatusActive,
		billingMode: types.BillingModePayPerRequest,
		pitrEnabled: false,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
	client.updateContinuousBackupsErr = errors.New("should not be called: PITR is disabled, retention days is moot")

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"}, // no Backup at all - disabled
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v — expected no UpdateContinuousBackups call while PITR stays disabled", err)
	}
}

func TestEnsure_CorrectsBillingModeDriftOnExistingTable(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:         "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:      types.TableStatusActive,
		billingMode: types.BillingModeProvisioned,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"},
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if client.tables[tableName].billingMode != types.BillingModePayPerRequest {
		t.Errorf("expected billing mode drift to be corrected to PAY_PER_REQUEST, got %s", client.tables[tableName].billingMode)
	}
}

func TestEnsure_CreatesTableWithProvisionedBillingModeWhenRequested(t *testing.T) {
	client := newFakeDynamoDB()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Overrides: &depsv1alpha1.DynamoDBOverrides{
			BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned,
		}},
	}}

	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	if client.tables[tableName].billingMode != types.BillingModeProvisioned {
		t.Errorf("expected billing mode Provisioned, got %s", client.tables[tableName].billingMode)
	}
}

func TestEnsure_CorrectsBillingModeDriftToProvisionedOnExistingTable(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:         "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:      types.TableStatusActive,
		billingMode: types.BillingModePayPerRequest,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Overrides: &depsv1alpha1.DynamoDBOverrides{
			BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned,
		}},
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if client.tables[tableName].billingMode != types.BillingModeProvisioned {
		t.Errorf("expected billing mode drift to be corrected to Provisioned, got %s", client.tables[tableName].billingMode)
	}
}

func TestEnsure_TreatsNilBillingModeSummaryAsAlreadyProvisioned(t *testing.T) {
	// Regression test: a table predating the BillingMode field entirely
	// (nil BillingModeSummary) is implicitly Provisioned - AWS's only mode
	// before PAY_PER_REQUEST existed. A CR that wants Provisioned on such a
	// legacy adopted table must see it as already correct, not call
	// UpdateTable every single reconcile because nil was misread as "not
	// yet PayPerRequest" (the bug this replaced) or "not yet Provisioned".
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:    "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status: types.TableStatusActive,
		// billingMode left at its zero value - the fake reports this as a
		// nil BillingModeSummary in DescribeTable, same as a real legacy table.
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Overrides: &depsv1alpha1.DynamoDBOverrides{
			BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned,
		}},
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	// UpdateTable would have set a non-empty billingMode on the fake if it
	// were (wrongly) called; the zero value surviving proves it wasn't.
	if client.tables[tableName].billingMode != "" {
		t.Errorf("expected no UpdateTable call for an already-Provisioned legacy table, but billing mode changed to %s", client.tables[tableName].billingMode)
	}
}

func TestReconcileTableAttributes_TreatsResourceInUseAsRetryable(t *testing.T) {
	// Regression test for the retry-classification gap: UpdateTable (and
	// UpdateContinuousBackups, DeleteTable) can all hit ResourceInUseException
	// when a table is mid-transition from a previous operation - this must
	// surface as retryable, not a hard failure, the same way CreateTable's
	// own ResourceInUseException already does.
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:         "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:      types.TableStatusActive,
		billingMode: types.BillingModePayPerRequest,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "checkout-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-1",
		},
	}
	client.updateTableErr = &fakeAWSError{code: "ResourceInUseException", fault: smithy.FaultClient}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Overrides: &depsv1alpha1.DynamoDBOverrides{
			BillingMode: depsv1alpha1.DynamoDBBillingModeProvisioned,
		}},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error from the failing UpdateTable call")
	}
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("expected a *cloudctlaws.ReconcileError in the chain, got %v", err)
	}
	if !reconcileErr.Retryable {
		t.Error("expected ResourceInUseException from UpdateTable to be classified as retryable")
	}
}

func TestEnsure_RefusesUnownedTableWithoutAdopt(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:    "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status: types.TableStatusActive,
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error for an untagged pre-existing table without adopt:true")
	}
}

func TestEnsure_AdoptsUntaggedTableWhenRequested(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:          "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:       types.TableStatusActive,
		tags:         map[string]string{"team": "someone-else"},
		partitionKey: "id",
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Adopt: true},
	}}
	if _, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if !cloudctlaws.IsOwnedBy(client.tables[tableName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the table to be tagged as owned by this CR after adoption")
	}
	if client.tables[tableName].tags["team"] != "someone-else" {
		t.Error("expected pre-existing tags to be preserved during adoption")
	}
}

func TestEnsure_RejectsTableOwnedByDifferentCREvenWithAdopt(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:    "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status: types.TableStatusActive,
		tags: map[string]string{
			cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("default", "other-service"),
			cloudctlaws.OwnerUIDTagKey: "uid-2",
		},
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Adopt: true},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a table owned by a different AppDependencies CR")
	}
}

func TestEnsure_RefusesAdoptingTableWithMismatchedKeySchema(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:          "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:       types.TableStatusActive,
		tags:         map[string]string{"team": "someone-else"},
		partitionKey: "userId", // spec below declares "id"
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", Adopt: true},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a table whose actual key schema doesn't match spec")
	}
	if cloudctlaws.IsOwnedBy(client.tables[tableName].tags, "default", "checkout-service", "uid-1") {
		t.Error("expected the mismatched table to not be tagged as owned — adoption must not proceed")
	}
}

func TestEnsure_RefusesAdoptingTableMissingASortKey(t *testing.T) {
	client := newFakeDynamoDB()
	tableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[tableName] = &fakeTable{
		arn:          "arn:aws:dynamodb:us-east-1:123456789012:table/" + tableName,
		status:       types.TableStatusActive,
		tags:         map[string]string{"team": "someone-else"},
		partitionKey: "id", // matches, but spec below also declares a sort key the table doesn't have
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", SortKey: "createdAt", Adopt: true},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected adopt:true to still refuse a table missing a sort key the spec declares")
	}
}

func TestEnsure_RejectsTableNameExceedingDynamoDBLimit(t *testing.T) {
	client := newFakeDynamoDB()
	longKey := strings.Repeat("a", 250)
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: longKey, PartitionKey: "id"},
	}}

	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error for a computed table name exceeding the length limit")
	}
	if _, ok := client.tables[cloudctlaws.ResourceName("default", "checkout-service", longKey)]; ok {
		t.Error("expected no CreateTable call to have been made for an over-length name")
	}
}

func TestEnsure_ClassifiesPermissionErrorsAsNotRetryable(t *testing.T) {
	client := newFakeDynamoDB()
	client.createTableErr = &fakeAWSError{code: "AccessDeniedException", fault: smithy.FaultClient}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"},
	}}
	_, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Error("expected a permission-denied error to be classified as not retryable")
	}
}

func TestEnsure_ContinuesToOtherTablesAfterOneFails(t *testing.T) {
	client := newFakeDynamoDB()
	badTableName := cloudctlaws.ResourceName("default", "checkout-service", "sessions")
	client.tables[badTableName] = &fakeTable{
		arn:    "arn:aws:dynamodb:us-east-1:123456789012:table/" + badTableName,
		status: types.TableStatusActive,
		tags:   map[string]string{"team": "someone-else"},
	}

	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id"}, // fails: untagged, no adopt
		{Name: "orders", PartitionKey: "id"},   // should still succeed
	}}
	ledger, err := Ensure(context.Background(), client, "default", "checkout-service", "uid-1", spec, nil)
	if err == nil {
		t.Fatal("expected an error reported for the unowned sessions table")
	}
	if status.FindManagedResource(ledger, "dynamodb", "orders") == nil {
		t.Error("expected orders to still be created despite sessions failing")
	}
}
