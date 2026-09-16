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

package sns

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

const pendingDeletionDenySid = "cloudctl-pending-deletion-deny"

type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []json.RawMessage `json:"Statement"`
}

type denyStatement struct {
	Sid       string   `json:"Sid"`
	Effect    string   `json:"Effect"`
	Principal string   `json:"Principal"`
	Action    []string `json:"Action"`
	Resource  string   `json:"Resource"`
}

// addPendingDeletionDeny merges a Deny statement for sns:Publish and
// sns:Subscribe into the topic's policy, scoped to just those two actions so
// it doesn't interfere with anything else the policy grants. Blocking
// Subscribe (not just Publish) matters here: while a topic sits in
// PendingDeletion, nothing new should be able to attach to it either.
// Idempotent - safe every reconcile.
func addPendingDeletionDeny(ctx context.Context, client snsAPI, topicArn string) error {
	doc, err := readPolicy(ctx, client, topicArn)
	if err != nil {
		return err
	}

	doc.Statement = removeStatementBySid(doc.Statement, pendingDeletionDenySid)

	stmt, err := json.Marshal(denyStatement{
		Sid:       pendingDeletionDenySid,
		Effect:    "Deny",
		Principal: "*",
		Action:    []string{"sns:Publish", "sns:Subscribe"},
		Resource:  topicArn,
	})
	if err != nil {
		return fmt.Errorf("encoding pending-deletion deny statement: %w", err)
	}
	doc.Statement = append(doc.Statement, stmt)

	return writePolicy(ctx, client, topicArn, doc)
}

// removePendingDeletionDeny removes exactly the Sid addPendingDeletionDeny
// added, leaving any other pre-existing policy statements untouched.
func removePendingDeletionDeny(ctx context.Context, client snsAPI, topicArn string) error {
	doc, err := readPolicy(ctx, client, topicArn)
	if err != nil {
		return err
	}

	before := len(doc.Statement)
	doc.Statement = removeStatementBySid(doc.Statement, pendingDeletionDenySid)
	if len(doc.Statement) == before {
		return nil
	}

	if len(doc.Statement) == 0 {
		attrName := "Policy"
		attrValue := ""
		_, err := client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
			TopicArn:       &topicArn,
			AttributeName:  &attrName,
			AttributeValue: &attrValue,
		})
		return err
	}

	return writePolicy(ctx, client, topicArn, doc)
}

func readPolicy(ctx context.Context, client snsAPI, topicArn string) (policyDocument, error) {
	out, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topicArn})
	if err != nil {
		return policyDocument{}, fmt.Errorf("reading topic policy: %w", err)
	}

	raw := out.Attributes["Policy"]
	if raw == "" {
		return policyDocument{Version: "2012-10-17"}, nil
	}

	var doc policyDocument
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return policyDocument{}, fmt.Errorf("parsing existing topic policy: %w", err)
	}
	return doc, nil
}

func writePolicy(ctx context.Context, client snsAPI, topicArn string, doc policyDocument) error {
	encoded, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encoding topic policy: %w", err)
	}
	attrName := "Policy"
	attrValue := string(encoded)
	_, err = client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
		TopicArn:       &topicArn,
		AttributeName:  &attrName,
		AttributeValue: &attrValue,
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
