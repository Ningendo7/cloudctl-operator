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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
)

const (
	pendingDeletionDenySid = "cloudctl-pending-deletion-deny"

	// noPolicyRevision is what PutResourcePolicy takes as the expected
	// revision to attach a policy only if the table has none.
	noPolicyRevision = "NO_POLICY"
)

// pendingDeletionDenyActions are the writes that add or change data. A table
// waiting to be emptied has to keep allowing the writes that empty it:
// DeleteItem and a PartiQL delete are left out on purpose, and so is TTL, which
// is not a request. BatchWriteItem carries deletes too, and no condition key
// tells them apart from puts, so it is denied with the rest: leaving it open
// would leave a way to keep filling the table.
var pendingDeletionDenyActions = []string{
	"dynamodb:PutItem",
	"dynamodb:UpdateItem",
	"dynamodb:BatchWriteItem",
	"dynamodb:PartiQLInsert",
	"dynamodb:PartiQLUpdate",
}

type denyStatement struct {
	Sid       string   `json:"Sid"`
	Effect    string   `json:"Effect"`
	Principal string   `json:"Principal"`
	Action    []string `json:"Action"`
	Resource  []string `json:"Resource"`
}

// tablePolicy is a table's resource policy as read, with the revision a write
// has to name.
type tablePolicy struct {
	statements []json.RawMessage
	revisionID string // empty when the table has no policy
}

// addPendingDeletionDeny merges a Deny statement for the data-adding writes
// into the table's resource policy, scoped to the table and its indexes, so
// the table can be drained but not refilled. A resource policy Deny beats any
// Allow from any source. Idempotent: it writes only when the statement is
// missing or differs.
func addPendingDeletionDeny(ctx context.Context, client dynamodbAPI, tableARN string) error {
	policy, err := readTablePolicy(ctx, client, tableARN)
	if err != nil {
		return err
	}
	statements, changed, err := awspolicy.Upsert(policy.statements, pendingDeletionDenySid, denyStatement{
		Sid:       pendingDeletionDenySid,
		Effect:    "Deny",
		Principal: "*",
		Action:    pendingDeletionDenyActions,
		Resource:  []string{tableARN, tableARN + "/index/*"},
	})
	if err != nil || !changed {
		return err
	}
	policy.statements = statements
	return writeTablePolicy(ctx, client, tableARN, policy)
}

// removePendingDeletionDeny removes exactly the statement addPendingDeletionDeny
// added, leaving every other statement alone, and the policy itself if nothing
// else is left. A table that is already gone has nothing to remove.
func removePendingDeletionDeny(ctx context.Context, client dynamodbAPI, tableARN string) error {
	policy, err := readTablePolicy(ctx, client, tableARN)
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return nil
		}
		return err
	}
	statements, changed := awspolicy.Remove(policy.statements, pendingDeletionDenySid)
	if !changed {
		return nil
	}
	policy.statements = statements
	if len(statements) == 0 {
		_, err := client.DeleteResourcePolicy(ctx, &dynamodb.DeleteResourcePolicyInput{
			ResourceArn: &tableARN, ExpectedRevisionId: &policy.revisionID,
		})
		return policyWriteError(err, "removing the table's resource policy")
	}
	return writeTablePolicy(ctx, client, tableARN, policy)
}

func readTablePolicy(ctx context.Context, client dynamodbAPI, tableARN string) (tablePolicy, error) {
	out, err := client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: &tableARN})
	if err != nil {
		var none *types.PolicyNotFoundException
		if errors.As(err, &none) {
			return tablePolicy{}, nil
		}
		return tablePolicy{}, wrapAWSError(err, "reading the table's resource policy")
	}
	var doc struct {
		Statement []json.RawMessage `json:"Statement"`
	}
	if out.Policy != nil && *out.Policy != "" {
		if err := json.Unmarshal([]byte(*out.Policy), &doc); err != nil {
			return tablePolicy{}, fmt.Errorf("parsing the table's resource policy: %w", err)
		}
	}
	revision := ""
	if out.RevisionId != nil {
		revision = *out.RevisionId
	}
	return tablePolicy{statements: doc.Statement, revisionID: revision}, nil
}

func writeTablePolicy(ctx context.Context, client dynamodbAPI, tableARN string, policy tablePolicy) error {
	encoded, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": policy.statements})
	if err != nil {
		return fmt.Errorf("encoding the table's resource policy: %w", err)
	}
	expected := policy.revisionID
	if expected == "" {
		expected = noPolicyRevision
	}
	policyJSON := string(encoded)
	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn: &tableARN, Policy: &policyJSON, ExpectedRevisionId: &expected,
	})
	return policyWriteError(err, "writing the table's resource policy")
}

// policyWriteError makes the ways a policy write fails while another is
// settling, or after the policy changed since it was read, something to try
// again: DynamoDB refuses a second change to a table's policy for a few
// seconds after the first, and reads are eventually consistent.
func policyWriteError(err error, errContext string) error {
	if err == nil {
		return nil
	}
	var none *types.PolicyNotFoundException
	var apiErr smithy.APIError
	if errors.As(err, &none) || (errors.As(err, &apiErr) && apiErr.ErrorCode() == "LimitExceededException") {
		return &cloudctlaws.ReconcileError{Err: fmt.Errorf("%s: %w", errContext, err), Retryable: true}
	}
	return wrapAWSError(err, errContext)
}
