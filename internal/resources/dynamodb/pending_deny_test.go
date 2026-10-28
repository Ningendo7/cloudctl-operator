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
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

func onlyTable(t *testing.T, client *fakeDynamoDB) *fakeTable {
	t.Helper()
	if len(client.tables) != 1 {
		t.Fatalf("expected exactly one table, got %d", len(client.tables))
	}
	for _, table := range client.tables {
		return table
	}
	return nil
}

type policyView struct {
	Statement []struct {
		Sid      string
		Effect   string
		Action   stringOrList
		Resource stringOrList
	}
}

// stringOrList reads a policy field that may be either.
type stringOrList []string

func (s *stringOrList) UnmarshalJSON(b []byte) error {
	var one string
	if json.Unmarshal(b, &one) == nil {
		*s = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

func viewPolicy(t *testing.T, raw string) policyView {
	t.Helper()
	var v policyView
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("not a policy: %v: %s", err, raw)
	}
	return v
}

func cleanupAll(t *testing.T, client *fakeDynamoDB, spec *depsv1alpha1.DynamoDBSpec, ledger []depsv1alpha1.ManagedResource, recordEvent func(string, string, string)) []depsv1alpha1.ManagedResource {
	t.Helper()
	out, _, err := Cleanup(context.Background(), client, "default", "checkout-service", "uid-1", spec, ledger, false, false, recordEvent)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	return out
}

// A table waiting to be emptied can still be drained, and cannot be refilled.
func TestCleanup_ATableWaitingToBeDeletedStopsTakingNewData_ButCanStillBeEmptied(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	table := onlyTable(t, client)

	cleanupAll(t, client, &depsv1alpha1.DynamoDBSpec{}, ledger, nil)

	v := viewPolicy(t, table.policy)
	if len(v.Statement) != 1 || v.Statement[0].Sid != pendingDeletionDenySid || v.Statement[0].Effect != "Deny" {
		t.Fatalf("expected one Deny statement, got %s", table.policy)
	}
	st := v.Statement[0]
	for _, want := range []string{"dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:BatchWriteItem", "dynamodb:PartiQLInsert", "dynamodb:PartiQLUpdate"} {
		if !slices.Contains(st.Action, want) {
			t.Errorf("the write %s is not denied: %v", want, st.Action)
		}
	}
	for _, mustStayOpen := range []string{"dynamodb:DeleteItem", "dynamodb:PartiQLDelete", "dynamodb:Scan", "dynamodb:Query", "dynamodb:GetItem", "dynamodb:DeleteTable"} {
		if slices.Contains(st.Action, mustStayOpen) {
			t.Errorf("%s is denied, but emptying the table needs it: %v", mustStayOpen, st.Action)
		}
	}
	if len(st.Resource) != 2 || st.Resource[0] != table.arn || st.Resource[1] != table.arn+"/index/*" {
		t.Errorf("the deny should cover the table and its indexes, got %v", st.Resource)
	}
}

// retryable mirrors how the controller decides: a ReconcileError says so
// itself, anything else is classified by IsRetryable.
func retryable(err error) bool {
	var reconcileErr *cloudctlaws.ReconcileError
	if errors.As(err, &reconcileErr) {
		return reconcileErr.Retryable
	}
	return cloudctlaws.IsRetryable(err)
}

func TestAddPendingDeletionDeny_WritesOnlyWhenItDiffers_AndKeepsOtherStatements(t *testing.T) {
	client := newFakeDynamoDB()
	setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	table := onlyTable(t, client)
	table.policy = `{"Version":"2012-10-17","Statement":[{"Sid":"theirs","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Action":"dynamodb:GetItem","Resource":"*"}]}`
	table.policyRevision = 7

	for range 3 {
		if err := addPendingDeletionDeny(context.Background(), client, table.arn); err != nil {
			t.Fatalf("addPendingDeletionDeny() error = %v", err)
		}
	}

	if client.policyWrites != 1 {
		t.Errorf("policy writes = %d over three identical calls, want 1 (DynamoDB also refuses a second change for a few seconds)", client.policyWrites)
	}
	if !strings.Contains(table.policy, `"theirs"`) || !strings.Contains(table.policy, pendingDeletionDenySid) {
		t.Errorf("expected their statement kept and ours added, got %s", table.policy)
	}
}

func TestAddPendingDeletionDeny_OnATableWithoutAPolicy_AttachesOneOnlyIfNoneExists(t *testing.T) {
	client := newFakeDynamoDB()
	setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	table := onlyTable(t, client)

	if err := addPendingDeletionDeny(context.Background(), client, table.arn); err != nil {
		t.Fatalf("addPendingDeletionDeny() error = %v", err)
	}
	if table.policy == "" {
		t.Fatal("no policy was attached")
	}
}

func TestRemovePendingDeletionDeny_LeavesOtherStatements_AndDropsAPolicyThatBecomesEmpty(t *testing.T) {
	client := newFakeDynamoDB()
	setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	table := onlyTable(t, client)
	if err := addPendingDeletionDeny(context.Background(), client, table.arn); err != nil {
		t.Fatal(err)
	}

	if err := removePendingDeletionDeny(context.Background(), client, table.arn); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}
	if table.policy != "" {
		t.Errorf("our statement was the only one, so the policy should be gone, got %s", table.policy)
	}

	table.policy = `{"Version":"2012-10-17","Statement":[{"Sid":"theirs","Effect":"Allow","Principal":"*","Action":"dynamodb:GetItem","Resource":"*"}]}`
	table.policyRevision = 3
	if err := addPendingDeletionDeny(context.Background(), client, table.arn); err != nil {
		t.Fatal(err)
	}
	if err := removePendingDeletionDeny(context.Background(), client, table.arn); err != nil {
		t.Fatalf("removePendingDeletionDeny() error = %v", err)
	}
	if v := viewPolicy(t, table.policy); len(v.Statement) != 1 || v.Statement[0].Sid != "theirs" {
		t.Errorf("only their statement should remain, got %s", table.policy)
	}
}

func TestRemovePendingDeletionDeny_ATableThatIsGoneHasNothingToRemove(t *testing.T) {
	client := newFakeDynamoDB()
	if err := removePendingDeletionDeny(context.Background(), client, "arn:aws:dynamodb:us-east-1:123456789012:table/gone"); err != nil {
		t.Errorf("removePendingDeletionDeny() error = %v, want none for a table that no longer exists", err)
	}
}

func TestCleanup_ATableDeclaredAgainGetsItsWritesBack(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	table := onlyTable(t, client)
	ledger = cleanupAll(t, client, &depsv1alpha1.DynamoDBSpec{}, ledger, nil)
	if table.policy == "" {
		t.Fatal("setup: the deny should be in place")
	}

	backInSpec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{{Name: "sessions", PartitionKey: "id", DeletionPolicy: depsv1alpha1.DeletionPolicyDelete}}}
	cleanupAll(t, client, backInSpec, ledger, nil)

	if table.policy != "" {
		t.Errorf("a table back in use must accept writes again, policy: %s", table.policy)
	}
}

func TestCleanup_ATableRetainedWhileWaitingIsNoLongerBlocked(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	table := onlyTable(t, client)
	ledger = cleanupAll(t, client, &depsv1alpha1.DynamoDBSpec{}, ledger, nil)
	for i := range ledger {
		ledger[i].DeletionPolicy = depsv1alpha1.DeletionPolicyRetain // the policy changed while it waited
	}

	cleanupAll(t, client, &depsv1alpha1.DynamoDBSpec{}, ledger, nil)

	if table.policy != "" {
		t.Errorf("a retained table is not being emptied and must accept writes, policy: %s", table.policy)
	}
}

// DynamoDB refuses a second change to a table's policy for a few seconds after
// the first, and a read can be older than the last write: both are tried again.
func TestAddPendingDeletionDeny_FailuresThatClearOnTheirOwnAreRetried(t *testing.T) {
	for name, putErr := range map[string]error{
		"another change just settled":          &fakeAWSError{code: "LimitExceededException", fault: smithy.FaultClient},
		"the policy changed since it was read": &types.PolicyNotFoundException{},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeDynamoDB()
			setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
			client.putPolicyErr = putErr

			err := addPendingDeletionDeny(context.Background(), client, onlyTable(t, client).arn)

			if err == nil || !retryable(err) {
				t.Errorf("addPendingDeletionDeny() error = %v, want a retryable error", err)
			}
		})
	}
}

func TestAddPendingDeletionDeny_AnotherFailureIsNotCalledTransient(t *testing.T) {
	client := newFakeDynamoDB()
	setupTable(t, client, "default", "checkout-service", "sessions", depsv1alpha1.DeletionPolicyDelete, false)
	client.putPolicyErr = &fakeAWSError{code: "ValidationException", fault: smithy.FaultClient}

	err := addPendingDeletionDeny(context.Background(), client, onlyTable(t, client).arn)

	if err == nil || retryable(err) {
		t.Errorf("addPendingDeletionDeny() error = %v, want a non-retryable error", err)
	}
}

// --- deletionProtection ---

func ensureSessions(t *testing.T, client *fakeDynamoDB, protection *bool, adopt bool, ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
	t.Helper()
	spec := &depsv1alpha1.DynamoDBSpec{Resources: []depsv1alpha1.DynamoDBTableSpec{
		{Name: "sessions", PartitionKey: "id", DeletionProtection: protection, Adopt: adopt},
	}}
	out, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	return out
}

func boolPtr(b bool) *bool { return &b }

func TestEnsure_DeletionProtectionOnANewTable(t *testing.T) {
	for name, tc := range map[string]struct {
		setting *bool
		want    bool
	}{
		"unset: protected":   {nil, true},
		"true: protected":    {boolPtr(true), true},
		"false: unprotected": {boolPtr(false), false},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeDynamoDB()
			ensureSessions(t, client, tc.setting, false, nil)
			if got := onlyTable(t, client).deletionProtection; got != tc.want {
				t.Errorf("deletion protection = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnsure_AnExplicitDeletionProtectionIsKeptInLine_AnUnsetOneIsNot(t *testing.T) {
	client := newFakeDynamoDB()
	ledger := ensureSessions(t, client, nil, false, nil)
	ledger = ensureSessions(t, client, nil, false, ledger) // out of Creating
	table := onlyTable(t, client)

	table.deletionProtection = false // someone turned it off by hand
	ledger = ensureSessions(t, client, nil, false, ledger)
	if table.deletionProtection {
		t.Error("unset must leave the table as it is: the setting is not re-asserted")
	}

	ledger = ensureSessions(t, client, boolPtr(true), false, ledger)
	if !table.deletionProtection {
		t.Error("an explicit true must be put back")
	}

	ensureSessions(t, client, boolPtr(false), false, ledger)
	if table.deletionProtection {
		t.Error("an explicit false must be applied, so a table can be made deletable by spec")
	}
}

func TestEnsure_AnAdoptedTableKeepsItsDeletionProtectionUnlessTheSpecSetsIt(t *testing.T) {
	for name, tc := range map[string]struct {
		setting        *bool
		existing, want bool
	}{
		"unset, unprotected stays unprotected": {nil, false, false},
		"unset, protected stays protected":     {nil, true, true},
		"true protects it":                     {boolPtr(true), false, true},
		"false unprotects it":                  {boolPtr(false), true, false},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeDynamoDB()
			created := ensureSessions(t, client, nil, false, nil)
			table := onlyTable(t, client)
			table.tags = map[string]string{"team": "someone-else"} // no longer ours
			table.deletionProtection = tc.existing

			ensureSessions(t, client, tc.setting, true, nil)
			_ = created

			if table.deletionProtection != tc.want {
				t.Errorf("deletion protection = %v, want %v", table.deletionProtection, tc.want)
			}
		})
	}
}

// DynamoDB refuses a resource policy whose statement ID has anything but
// letters and digits, which no fake would notice.
func TestPendingDeletionDenySid_HasOnlyLettersAndDigits(t *testing.T) {
	for _, r := range pendingDeletionDenySid {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			t.Fatalf("statement ID %q has %q, which DynamoDB does not accept", pendingDeletionDenySid, r)
		}
	}
}
