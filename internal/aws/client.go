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
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Clients holds one shared client per AWS service, built once at
// controller-manager startup and reused for the process lifetime. Client
// construction resolves credentials (an STS call under IRSA) and builds the
// retry/middleware stack, so building a fresh one per reconcile would add
// latency to every tick and could hammer STS under load. Clients are safe
// for concurrent use.
type Clients struct {
	SQS        SQSClient
	SNS        SNSClient
	S3         *s3.Client
	DynamoDB   *dynamodb.Client
	KMS        *kms.Client
	IAM        IAMClient
	CloudWatch *cloudwatch.Client

	// AccountID and Region identify this controller's own AWS account,
	// resolved once at startup. Needed anywhere a resource's name/ARN must
	// be constructed deterministically ourselves rather than looked up by
	// name: SNS has no "get topic by name" API (CreateTopic is the only
	// name-to-ARN resolution, and it's idempotent in a way that hides
	// whether a topic was just created or already existed, so the sns
	// package constructs the expected ARN itself and checks for it
	// directly), and S3 bucket names need an account-ID-derived suffix
	// since they're unique across every AWS account globally, not just
	// this one.
	AccountID string
	Region    string
}

// NewClients loads the default AWS config (region/credentials resolved from
// the environment/IRSA), resolves this account's own identity via STS, and
// builds one client per service. IAM gets a tighter retry budget than the
// others since its rate limits are considerably stricter. Failing to
// resolve the account ID is treated as fatal, same as a config load
// failure — if we can't determine our own identity, something is
// fundamentally wrong with the credentials setup, and failing fast at
// startup beats failing deep inside a random reconcile later.
func NewClients(ctx context.Context) (*Clients, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	iamCfg := cfg.Copy()
	iamCfg.RetryMaxAttempts = 3

	stsClient := sts.NewFromConfig(cfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("resolving AWS account ID: %w", err)
	}

	return &Clients{
		SQS:        sqs.NewFromConfig(cfg),
		SNS:        sns.NewFromConfig(cfg),
		S3:         s3.NewFromConfig(cfg),
		DynamoDB:   dynamodb.NewFromConfig(cfg),
		KMS:        kms.NewFromConfig(cfg),
		IAM:        iam.NewFromConfig(iamCfg),
		CloudWatch: cloudwatch.NewFromConfig(cfg),
		AccountID:  *identity.Account,
		Region:     cfg.Region,
	}, nil
}
