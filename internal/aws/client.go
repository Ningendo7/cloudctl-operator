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
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"golang.org/x/time/rate"
)

// Clients holds one shared client per AWS service, built once at
// controller-manager startup and reused for the process lifetime. Client
// construction resolves credentials (an STS call under IRSA) and builds the
// retry/middleware stack, so building a fresh one per reconcile would add
// latency to every tick and could hammer STS under load. Clients are safe
// for concurrent use.
type Clients struct {
	SQS            SQSClient
	SNS            SNSClient
	S3             *s3.Client
	DynamoDB       *dynamodb.Client
	KMS            KMSClient
	IAM            IAMClient
	CloudWatch     CloudWatchClient
	RDS            RDSClient
	EC2            EC2Client
	SecretsManager SecretsManagerClient

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

	// RateLimiters paces this operator's own outgoing calls to each AWS
	// service independently (see ratelimit.go), keyed by the same lowercase
	// service name each client uses internally (see rateLimiterDefaults).
	// Exposed so the caller can ramp each one up
	// gradually right after winning leader election.
	RateLimiters map[string]*rate.Limiter
}

// rateLimiterDefault is one service's QPS/burst pair, read from an env var
// pair at startup. Defaults here are deliberately conservative starting
// points, not verified figures for any specific AWS account's actual
// limits — tune via these env vars once real limits are known. IAM's
// default is tighter than the rest since its real limits are considerably
// stricter than SQS/SNS/S3/DynamoDB/CloudWatch's.
type rateLimiterDefault struct {
	name         string
	qpsEnv       string
	burstEnv     string
	defaultQPS   float64
	defaultBurst int
}

const serviceNameSQS = "sqs"
const serviceNameSNS = "sns"
const serviceNameS3 = "s3"
const serviceNameDynamoDB = "dynamodb"
const serviceNameKMS = "kms"
const serviceNameIAM = "iam"
const serviceNameCloudWatch = "cloudwatch"
const serviceNameRDS = "rds"
const serviceNameEC2 = "ec2"
const serviceNameSecretsManager = "secretsmanager"

var rateLimiterDefaults = []rateLimiterDefault{
	{serviceNameSQS, "AWS_SQS_RATE_LIMIT_QPS", "AWS_SQS_RATE_LIMIT_BURST", 20, 40},
	{serviceNameSNS, "AWS_SNS_RATE_LIMIT_QPS", "AWS_SNS_RATE_LIMIT_BURST", 20, 40},
	{serviceNameS3, "AWS_S3_RATE_LIMIT_QPS", "AWS_S3_RATE_LIMIT_BURST", 20, 40},
	{serviceNameDynamoDB, "AWS_DYNAMODB_RATE_LIMIT_QPS", "AWS_DYNAMODB_RATE_LIMIT_BURST", 20, 40},
	{serviceNameKMS, "AWS_KMS_RATE_LIMIT_QPS", "AWS_KMS_RATE_LIMIT_BURST", 10, 20},
	{serviceNameIAM, "AWS_IAM_RATE_LIMIT_QPS", "AWS_IAM_RATE_LIMIT_BURST", 8, 16},
	// CloudWatch's per-account-per-region quotas are far lower than the
	// others: PutMetricAlarm 3/s, DescribeAlarms 9/s, ListTagsForResource
	// 10/s. Alarm writes are rare (unchanged alarms are never rewritten), so
	// this sits at the steady-state describe/tag-read ceiling.
	{serviceNameCloudWatch, "AWS_CLOUDWATCH_RATE_LIMIT_QPS", "AWS_CLOUDWATCH_RATE_LIMIT_BURST", 5, 10},
	{serviceNameRDS, "AWS_RDS_RATE_LIMIT_QPS", "AWS_RDS_RATE_LIMIT_BURST", 20, 40},
	{serviceNameEC2, "AWS_EC2_RATE_LIMIT_QPS", "AWS_EC2_RATE_LIMIT_BURST", 20, 40},
	{serviceNameSecretsManager, "AWS_SECRETSMANAGER_RATE_LIMIT_QPS", "AWS_SECRETSMANAGER_RATE_LIMIT_BURST", 20, 40},
}

func newRateLimiters() map[string]*rate.Limiter {
	limiters := make(map[string]*rate.Limiter, len(rateLimiterDefaults))
	for _, d := range rateLimiterDefaults {
		limiters[d.name] = NewLimiter(envFloat(d.qpsEnv, d.defaultQPS), envInt(d.burstEnv, d.defaultBurst))
	}
	return limiters
}

// NewClients loads the default AWS config (region/credentials resolved from
// the environment/IRSA), resolves this account's own identity via STS, and
// builds one client per service. IAM gets a tighter retry budget than the
// others since its rate limits are considerably stricter. Failing to
// resolve the account ID is treated as fatal, same as a config load
// failure — if we can't determine our own identity, something is
// fundamentally wrong with the credentials setup, and failing fast at
// startup beats failing deep inside a random reconcile later.
//
// Each service's client also gets its own outgoing rate limiter attached
// as SDK middleware (see ratelimit.go) — independent of retry budget,
// this paces requests *before* they're sent, rather than backing off
// after AWS itself starts throttling.
func NewClients(ctx context.Context) (*Clients, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	limiters := newRateLimiters()

	iamCfg := cfg.Copy()
	iamCfg.RetryMaxAttempts = 3

	stsClient := sts.NewFromConfig(cfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("resolving AWS account ID: %w", err)
	}

	return &Clients{
		SQS: sqs.NewFromConfig(cfg, func(o *sqs.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameSQS, limiters[serviceNameSQS])
			o.TracerProvider = tracerProvider()
		}),
		SNS: sns.NewFromConfig(cfg, func(o *sns.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameSNS, limiters[serviceNameSNS])
			o.TracerProvider = tracerProvider()
		}),
		S3: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameS3, limiters[serviceNameS3])
			o.TracerProvider = tracerProvider()
			// A non-nil BaseEndpoint only happens when AWS_ENDPOINT_URL is
			// explicitly set - never true against real AWS, always true
			// against a test double like LocalStack, whose virtual-hosted
			// -style bucket URLs (bucket.s3.amazonaws.com) don't resolve to
			// a non-AWS host.
			if cfg.BaseEndpoint != nil {
				o.UsePathStyle = true
			}
		}),
		DynamoDB: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameDynamoDB, limiters[serviceNameDynamoDB])
			o.TracerProvider = tracerProvider()
		}),
		KMS: kms.NewFromConfig(cfg, func(o *kms.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameKMS, limiters[serviceNameKMS])
			o.TracerProvider = tracerProvider()
		}),
		IAM: iam.NewFromConfig(iamCfg, func(o *iam.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameIAM, limiters[serviceNameIAM])
			o.TracerProvider = tracerProvider()
		}),
		CloudWatch: cloudwatch.NewFromConfig(cfg, func(o *cloudwatch.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameCloudWatch, limiters[serviceNameCloudWatch])
			o.TracerProvider = tracerProvider()
		}),
		RDS: rds.NewFromConfig(cfg, func(o *rds.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameRDS, limiters[serviceNameRDS])
			o.TracerProvider = tracerProvider()
		}),
		EC2: ec2.NewFromConfig(cfg, func(o *ec2.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameEC2, limiters[serviceNameEC2])
			o.TracerProvider = tracerProvider()
		}),
		SecretsManager: secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
			o.APIOptions = withObservability(o.APIOptions, serviceNameSecretsManager, limiters[serviceNameSecretsManager])
			o.TracerProvider = tracerProvider()
		}),
		AccountID:    *identity.Account,
		Region:       cfg.Region,
		RateLimiters: limiters,
	}, nil
}
