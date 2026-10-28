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

package aws

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/Ningendo7/cloudctl-operator/internal/policycheck"
)

// iamPrefix is the IAM service prefix of each row of the permissions table.
var iamPrefix = map[string]string{
	"SQS":                       "sqs",
	"SNS":                       "sns",
	"S3":                        "s3",
	"DynamoDB":                  "dynamodb",
	"KMS":                       "kms",
	"IAM":                       "iam",
	"CloudWatch":                "cloudwatch",
	"RDS":                       "rds",
	"EC2 (RDS security groups)": "ec2",
	"Secrets Manager (RDS)":     "secretsmanager",
}

// TestLive_PermissionsTableIsAPolicyAWSRecognises turns docs/threat-model.md's
// permissions table into the identity policy an installer would build from it
// and has Access Analyzer check it. The table is matched against the code
// offline; only AWS can say whether each name is an action that exists, which
// is what makes the policy work at all, since IAM accepts an unknown action
// without complaint and simply never matches it.
func TestLive_PermissionsTableIsAPolicyAWSRecognises(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}

	var actions []string
	for row, names := range documentedActions(t) {
		prefix, known := iamPrefix[row]
		if _, inTable := clientInterfaces[row]; !inTable {
			continue
		}
		if !known {
			t.Fatalf("the permissions table has a row %q this test has no IAM prefix for", row)
		}
		for _, name := range names {
			actions = append(actions, prefix+":"+name)
		}
	}
	slices.Sort(actions)
	actions = slices.Compact(actions)

	document, err := json.Marshal(map[string]any{
		"Version":   "2012-10-17",
		"Statement": []map[string]any{{"Effect": "Allow", "Action": actions, "Resource": "*"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("validating %d actions from the permissions table", len(actions))
	policycheck.ValidatePolicy(t, awssdk.Config(cfg), policycheck.IdentityPolicy, policycheck.NoResourceType, string(document))
}
