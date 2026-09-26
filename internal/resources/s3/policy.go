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

package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const pendingDeletionDenySid = "cloudctl-pending-deletion-deny"

// policyDocument mirrors sqs/policy.go's own type: existing statements are
// kept as raw JSON rather than a fully-typed struct, so a pre-existing
// statement we don't need to understand is preserved untouched rather than
// risked being mangled by a round trip through a struct that doesn't model
// every shape AWS policy JSON can take.
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

// addPendingDeletionDeny merges a Deny statement for s3:PutObject into the
// bucket's policy, scoped to every object in it. A single action is enough
// to block every write path — s3:PutObject also governs all three
// multipart upload calls (CreateMultipartUpload/UploadPart/
// CompleteMultipartUpload), confirmed via AWS's own IAM action reference —
// unlike SQS, which needs two distinct actions (SendMessage and
// SendMessageBatch) to cover both single and batch sends. A bucket policy
// Deny beats any Allow from any source, the same reasoning as SQS/SNS's
// equivalent. Idempotent — safe to call every reconcile while a bucket
// stays in PendingDeletion (re-adds under the same Sid rather than
// duplicating).
func addPendingDeletionDeny(ctx context.Context, client s3API, bucket string) error {
	doc, err := readPolicy(ctx, client, bucket)
	if err != nil {
		return err
	}

	doc.Statement = removeStatementBySid(doc.Statement, pendingDeletionDenySid)

	stmt, err := json.Marshal(denyStatement{
		Sid:       pendingDeletionDenySid,
		Effect:    "Deny",
		Principal: "*",
		Action:    "s3:PutObject",
		Resource:  "arn:aws:s3:::" + bucket + "/*",
	})
	if err != nil {
		return fmt.Errorf("encoding pending-deletion deny statement: %w", err)
	}
	doc.Statement = append(doc.Statement, stmt)

	return writePolicy(ctx, client, bucket, doc)
}

// removePendingDeletionDeny removes exactly the Sid addPendingDeletionDeny
// added, leaving any other pre-existing policy statements untouched. If
// nothing else remains, the policy is deleted entirely via
// DeleteBucketPolicy rather than left as an empty (and invalid) statement
// list — S3 has a dedicated delete call for this, unlike SQS's single
// Policy attribute, which is cleared by writing an empty string instead.
func removePendingDeletionDeny(ctx context.Context, client s3API, bucket string) error {
	doc, err := readPolicy(ctx, client, bucket)
	if err != nil {
		return err
	}

	before := len(doc.Statement)
	doc.Statement = removeStatementBySid(doc.Statement, pendingDeletionDenySid)
	if len(doc.Statement) == before {
		return nil // nothing to do
	}

	if len(doc.Statement) == 0 {
		if _, err := client.DeleteBucketPolicy(ctx, &s3sdk.DeleteBucketPolicyInput{Bucket: &bucket}); err != nil && !isNotFoundError(err) {
			return err
		}
		return nil
	}

	return writePolicy(ctx, client, bucket, doc)
}

func readPolicy(ctx context.Context, client s3API, bucket string) (policyDocument, error) {
	out, err := client.GetBucketPolicy(ctx, &s3sdk.GetBucketPolicyInput{Bucket: &bucket})
	if err != nil {
		if isNoSuchBucketPolicy(err) {
			return policyDocument{Version: "2012-10-17"}, nil
		}
		return policyDocument{}, fmt.Errorf("reading bucket policy: %w", err)
	}

	if out.Policy == nil || *out.Policy == "" {
		return policyDocument{Version: "2012-10-17"}, nil
	}

	var doc policyDocument
	if err := json.Unmarshal([]byte(*out.Policy), &doc); err != nil {
		return policyDocument{}, fmt.Errorf("parsing existing bucket policy: %w", err)
	}
	return doc, nil
}

func writePolicy(ctx context.Context, client s3API, bucket string, doc policyDocument) error {
	encoded, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encoding bucket policy: %w", err)
	}
	policy := string(encoded)
	_, err = client.PutBucketPolicy(ctx, &s3sdk.PutBucketPolicyInput{Bucket: &bucket, Policy: &policy})
	return err
}

// isNoSuchBucketPolicy reports whether err is S3's "this bucket has no
// policy at all" response — a real, expected state for any bucket that's
// never had PutBucketPolicy called on it, not an error condition. No typed
// exception exists for this in the SDK, same as isNoSuchTagSet's own
// string-code check.
func isNoSuchBucketPolicy(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchBucketPolicy"
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
