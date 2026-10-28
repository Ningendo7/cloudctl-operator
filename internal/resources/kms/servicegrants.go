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

package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"

	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const (
	keyPolicyName       = "default"
	sidSNSDelivery      = "cloudctl-sns-delivery"
	sidCloudWatchAlarms = "cloudctl-cloudwatch-alarms"
)

// ServiceGrants says which AWS services may use a key on behalf of which
// resources. AWS services are not IAM identities in the account, so only the
// key policy can let SNS deliver into an encrypted queue, or CloudWatch
// publish to an encrypted topic; without it both fail silently. Each grant
// is limited to the exact resources that need it.
type ServiceGrants struct {
	// SNSTopicARNs are the topics that deliver into queues encrypted with
	// the key.
	SNSTopicARNs []string
	// AlarmARNs are the alarms that publish to topics encrypted with the key.
	AlarmARNs []string
}

// serviceKeyStatements are the key policy statements the operator owns, keyed
// by Sid, for the grants given. A service with nothing to act on behalf of
// has no statement.
func serviceKeyStatements(account string, grants ServiceGrants) map[string]any {
	statements := map[string]any{}
	add := func(sid, service string, sourceARNs []string) {
		if len(sourceARNs) == 0 {
			return
		}
		sorted := slices.Compact(slices.Sorted(slices.Values(sourceARNs)))
		arns := make([]any, len(sorted))
		for i, arn := range sorted {
			arns[i] = arn
		}
		statements[sid] = map[string]any{
			"Sid":       sid,
			"Effect":    "Allow",
			"Principal": map[string]any{"Service": service},
			"Action":    []any{"kms:GenerateDataKey*", "kms:Decrypt"},
			"Resource":  "*",
			"Condition": map[string]any{
				"StringEquals": map[string]any{"aws:SourceAccount": account},
				"ArnEquals":    map[string]any{"aws:SourceArn": arns},
			},
		}
	}
	add(sidSNSDelivery, "sns.amazonaws.com", grants.SNSTopicARNs)
	add(sidCloudWatchAlarms, "cloudwatch.amazonaws.com", grants.AlarmARNs)
	return statements
}

// mergeKeyPolicy brings the operator's statements in policy in line with
// grants, by Sid, and keeps every other statement: PutKeyPolicy replaces the
// whole document. Statements it owns that are no longer needed are removed,
// which also retires the account-wide ones earlier versions wrote. nil grants
// leaves the operator's statements as they are. An empty policy gets the
// account root statement AWS's default policy has, without which IAM policies
// grant nothing and PutKeyPolicy is refused.
func mergeKeyPolicy(policy, account string, grants *ServiceGrants) (merged string, changed bool, err error) {
	doc := map[string]json.RawMessage{}
	if policy != "" {
		if err := json.Unmarshal([]byte(policy), &doc); err != nil {
			return "", false, fmt.Errorf("parsing key policy: %w", err)
		}
	}
	statements, err := statementList(doc["Statement"])
	if err != nil {
		return "", false, err
	}
	if policy == "" {
		doc["Version"] = json.RawMessage(`"2012-10-17"`)
		root, err := json.Marshal(map[string]any{
			"Sid":       "Enable IAM User Permissions",
			"Effect":    "Allow",
			"Principal": map[string]any{"AWS": "arn:aws:iam::" + account + ":root"},
			"Action":    "kms:*",
			"Resource":  "*",
		})
		if err != nil {
			return "", false, fmt.Errorf("encoding key policy: %w", err)
		}
		statements = append(statements, root)
		changed = true
	}

	if grants != nil {
		wanted := serviceKeyStatements(account, *grants)
		for _, sid := range []string{sidSNSDelivery, sidCloudWatchAlarms} {
			var statementChanged bool
			if stmt, ok := wanted[sid]; ok {
				statements, statementChanged, err = awspolicy.Upsert(statements, sid, stmt)
			} else {
				statements, statementChanged = awspolicy.Remove(statements, sid)
			}
			if err != nil {
				return "", false, err
			}
			changed = changed || statementChanged
		}
	}
	if !changed {
		return policy, false, nil
	}

	encodedStatements, err := json.Marshal(statements)
	if err != nil {
		return "", false, fmt.Errorf("encoding key policy: %w", err)
	}
	doc["Statement"] = encodedStatements
	encoded, err := json.Marshal(doc)
	if err != nil {
		return "", false, fmt.Errorf("encoding key policy: %w", err)
	}
	return string(encoded), true, nil
}

// statementList reads a policy's Statement, which is one object or a list.
func statementList(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if raw[0] == '[' {
		var list []json.RawMessage
		if err := json.Unmarshal(raw, &list); err != nil {
			return nil, fmt.Errorf("parsing key policy statements: %w", err)
		}
		return list, nil
	}
	return []json.RawMessage{raw}, nil
}

// ensureKeyPolicy gives a key an account root statement if it has no policy
// at all. Service statements are not touched here: which of them a key needs
// depends on what is encrypted with it, so SyncServiceGrants decides.
func ensureKeyPolicy(ctx context.Context, kmsClient kmsAPI, arn string) error {
	parsed, err := awsarn.Parse(arn)
	if err != nil {
		return fmt.Errorf("parsing key ARN %q: %w", arn, err)
	}
	out, err := kmsClient.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: &arn, PolicyName: aws.String(keyPolicyName)})
	if err != nil {
		return err
	}
	merged, changed, err := mergeKeyPolicy(aws.ToString(out.Policy), parsed.AccountID, nil)
	if err != nil || !changed {
		return err
	}
	_, err = kmsClient.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{
		KeyId: &arn, PolicyName: aws.String(keyPolicyName), Policy: &merged,
	})
	return err
}

// SyncServiceGrants makes the key's policy carry exactly the service
// statements grants call for, adding, narrowing or removing the operator's own
// by Sid and writing only when something differs. A key that is gone or
// scheduled for deletion cannot have its policy changed and is left alone:
// the KMS section already reports it.
func SyncServiceGrants(ctx context.Context, kmsClient kmsAPI, keyARN string, grants ServiceGrants, recordEvent status.EventRecorder) error {
	parsed, err := awsarn.Parse(keyARN)
	if err != nil {
		return fmt.Errorf("parsing key ARN %q: %w", keyARN, err)
	}
	out, err := kmsClient.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: &keyARN, PolicyName: aws.String(keyPolicyName)})
	if err != nil {
		if keyCannotBeChanged(err) {
			return nil
		}
		return wrapAWSError(err, fmt.Sprintf("reading the policy of KMS key %s", keyARN))
	}
	current := aws.ToString(out.Policy)
	if current == "" && len(grants.SNSTopicARNs)+len(grants.AlarmARNs) == 0 {
		return nil // no policy to take statements out of
	}
	merged, changed, err := mergeKeyPolicy(current, parsed.AccountID, &grants)
	if err != nil || !changed {
		return err
	}
	if _, err := kmsClient.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{
		KeyId: &keyARN, PolicyName: aws.String(keyPolicyName), Policy: &merged,
	}); err != nil {
		if keyCannotBeChanged(err) {
			return nil
		}
		return wrapAWSError(err, fmt.Sprintf("updating the policy of KMS key %s", keyARN))
	}
	if recordEvent != nil {
		recordEvent("Normal", "KeyPolicyUpdated", fmt.Sprintf(
			"Updated the policy of KMS key %s: SNS may use it for %d topic(s), CloudWatch for %d alarm(s)",
			keyARN, len(grants.SNSTopicARNs), len(grants.AlarmARNs)))
	}
	return nil
}

func keyCannotBeChanged(err error) bool {
	var invalidState *types.KMSInvalidStateException
	var notFound *types.NotFoundException
	return errors.As(err, &invalidState) || errors.As(err, &notFound)
}
