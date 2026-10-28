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

	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const pendingDeletionDenySid = "cloudctl-pending-deletion-deny"

// policyAttrName is the SNS topic attribute name SetTopicAttributes/
// GetTopicAttributes use for the topic policy JSON.
const policyAttrName = "Policy"

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

	statements, changed, err := awspolicy.Upsert(doc.Statement, pendingDeletionDenySid, denyStatement{
		Sid:       pendingDeletionDenySid,
		Effect:    "Deny",
		Principal: "*",
		Action:    []string{"sns:Publish", "sns:Subscribe"},
		Resource:  topicArn,
	})
	if err != nil || !changed {
		return err
	}
	doc.Statement = statements

	return writePolicy(ctx, client, topicArn, doc)
}

// topicActions are the actions a topic policy governs. SNS refuses a wildcard
// in a policy statement ("action out of service scope"), so the transport
// statement names them.
var topicActions = []string{
	"sns:GetTopicAttributes",
	"sns:SetTopicAttributes",
	"sns:AddPermission",
	"sns:RemovePermission",
	"sns:DeleteTopic",
	"sns:Subscribe",
	"sns:ListSubscriptionsByTopic",
	"sns:Publish",
}

// reconcileTransportSecurity makes the topic refuse plain-HTTP requests, by a
// statement the operator owns in its policy. Other statements, AWS's default
// one among them, are kept, and nothing is written when the policy already
// says this.
func reconcileTransportSecurity(ctx context.Context, client snsAPI, topicArn string, recordEvent status.EventRecorder) error {
	doc, err := readPolicy(ctx, client, topicArn)
	if err != nil {
		return wrapAWSError(err, "reading the topic's policy")
	}
	statements, changed, err := awspolicy.Upsert(doc.Statement, awspolicy.InsecureTransportSid, awspolicy.DenyInsecureTransportActions(topicActions, topicArn))
	if err != nil || !changed {
		return err
	}
	doc.Statement = statements
	if err := writePolicy(ctx, client, topicArn, doc); err != nil {
		return wrapAWSError(err, "writing the topic's transport-security policy")
	}
	if recordEvent != nil {
		recordEvent("Normal", "TransportSecurityEnforced", fmt.Sprintf(
			"SNS topic %s now refuses requests that do not use TLS (policy statement %s)", topicArn, awspolicy.InsecureTransportSid))
	}
	return nil
}

// removePendingDeletionDeny removes exactly the Sid addPendingDeletionDeny
// added, leaving any other pre-existing policy statements untouched.
func removePendingDeletionDeny(ctx context.Context, client snsAPI, topicArn string) error {
	doc, err := readPolicy(ctx, client, topicArn)
	if err != nil {
		return err
	}

	statements, changed := awspolicy.Remove(doc.Statement, pendingDeletionDenySid)
	if !changed {
		return nil
	}
	doc.Statement = statements

	if len(doc.Statement) == 0 {
		attrName := policyAttrName
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

	raw := out.Attributes[policyAttrName]
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
	attrName := policyAttrName
	attrValue := string(encoded)
	_, err = client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
		TopicArn:       &topicArn,
		AttributeName:  &attrName,
		AttributeValue: &attrValue,
	})
	return err
}
