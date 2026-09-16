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

package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const pendingDeletionDenySid = "cloudctl-pending-deletion-deny"

// policyDocument models an SQS resource policy just enough to add/remove
// our own statement without disturbing anything else. Existing statements
// are kept as raw JSON rather than a fully-typed struct, since AWS policy
// JSON has fields (Principal, Action, Resource) that can be either a
// string or an array depending on who wrote it — round-tripping them
// opaquely avoids us mangling a pre-existing statement we don't need to
// understand, we just need to preserve it.
type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []json.RawMessage `json:"Statement"`
}

type denyStatement struct {
	Sid       string `json:"Sid"`
	Effect    string `json:"Effect"`
	Principal string `json:"Principal"`
	Action    string `json:"Action"`
	Resource  string `json:"Resource"`
}

// addPendingDeletionDeny merges a Deny statement for sqs:SendMessage into
// the queue's resource policy, scoped to just that action so existing
// consumers can keep draining the queue while nothing new gets added. A
// resource-policy Deny beats any Allow from any source — our own derived
// IAM, a cross-account grant, hand-managed IAM — which is the point:
// revoking our own IAM grants alone only blocks producers using roles we
// control. Idempotent — safe to call every reconcile while a resource
// stays in PendingDeletion (re-adds under the same Sid rather than
// duplicating).
func addPendingDeletionDeny(ctx context.Context, client sqsAPI, queueURL, queueArn string) error {
	doc, err := readPolicy(ctx, client, queueURL)
	if err != nil {
		return err
	}

	doc.Statement = removeStatementBySid(doc.Statement, pendingDeletionDenySid)

	stmt, err := json.Marshal(denyStatement{
		Sid:       pendingDeletionDenySid,
		Effect:    "Deny",
		Principal: "*",
		Action:    "sqs:SendMessage",
		Resource:  queueArn,
	})
	if err != nil {
		return fmt.Errorf("encoding pending-deletion deny statement: %w", err)
	}
	doc.Statement = append(doc.Statement, stmt)

	return writePolicy(ctx, client, queueURL, doc)
}

// removePendingDeletionDeny removes exactly the Sid addPendingDeletionDeny
// added, leaving any other pre-existing policy statements untouched. If
// nothing else remains, the policy is cleared entirely rather than left as
// an empty (and likely invalid) statement list.
func removePendingDeletionDeny(ctx context.Context, client sqsAPI, queueURL string) error {
	doc, err := readPolicy(ctx, client, queueURL)
	if err != nil {
		return err
	}

	before := len(doc.Statement)
	doc.Statement = removeStatementBySid(doc.Statement, pendingDeletionDenySid)
	if len(doc.Statement) == before {
		return nil // nothing to do
	}

	if len(doc.Statement) == 0 {
		_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl:   &queueURL,
			Attributes: map[string]string{string(types.QueueAttributeNamePolicy): ""},
		})
		return err
	}

	return writePolicy(ctx, client, queueURL, doc)
}

func readPolicy(ctx context.Context, client sqsAPI, queueURL string) (policyDocument, error) {
	out, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
	})
	if err != nil {
		return policyDocument{}, fmt.Errorf("reading queue policy: %w", err)
	}

	raw := out.Attributes[string(types.QueueAttributeNamePolicy)]
	if raw == "" {
		return policyDocument{Version: "2012-10-17"}, nil
	}

	var doc policyDocument
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return policyDocument{}, fmt.Errorf("parsing existing queue policy: %w", err)
	}
	return doc, nil
}

func writePolicy(ctx context.Context, client sqsAPI, queueURL string, doc policyDocument) error {
	encoded, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encoding queue policy: %w", err)
	}
	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   &queueURL,
		Attributes: map[string]string{string(types.QueueAttributeNamePolicy): string(encoded)},
	})
	return err
}

func removeStatementBySid(statements []json.RawMessage, sid string) []json.RawMessage {
	filtered := make([]json.RawMessage, 0, len(statements))
	for _, s := range statements {
		var probe struct {
			Sid string `json:"Sid"`
		}
		if err := json.Unmarshal(s, &probe); err == nil && probe.Sid == sid {
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}
