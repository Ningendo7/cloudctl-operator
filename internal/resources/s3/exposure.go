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
	"errors"
	"fmt"
	"strings"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const (
	allUsersURI           = "http://acs.amazonaws.com/groups/global/AllUsers"
	authenticatedUsersURI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

	errCodeNoSuchWebsite     = "NoSuchWebsiteConfiguration"
	errCodeNoSuchPolicy      = "NoSuchBucketPolicy"
	errCodeNoSuchPublicBlock = "NoSuchPublicAccessBlockConfiguration"
)

// reconcileTransportSecurity makes the bucket refuse plain-HTTP requests, by a
// statement the operator owns in its policy. A bucket that hosts a website is
// exempt: the website endpoint speaks HTTP only, so the statement would take
// the site down. If the statement is there already it is removed, and either
// way the bucket is reported, since requests to it are then not protected.
// Failing to find out whether it hosts a website fails the pass: guessing
// wrong in one direction breaks a site, in the other leaves HTTP open. The same
// read also drops a pending-deletion deny the bucket should no longer have.
func reconcileTransportSecurity(ctx context.Context, client s3API, bucket string, recordEvent status.EventRecorder) error {
	website, err := hasWebsite(ctx, client, bucket)
	if err != nil {
		return wrapAWSError(err, "checking whether the bucket hosts a website")
	}
	done, err := syncDeclaredPolicy(ctx, client, bucket, !website)
	if err != nil {
		return wrapAWSError(err, "reconciling the bucket's policy")
	}
	if recordEvent == nil {
		return nil
	}
	switch {
	case website:
		recordEvent("Warning", "TransportSecurityNotEnforced", fmt.Sprintf(
			"S3 bucket %s hosts a website, whose endpoint accepts plain HTTP only, so requests to it are not required to use TLS", bucket))
	case done.transport:
		recordEvent("Normal", "TransportSecurityEnforced", fmt.Sprintf(
			"S3 bucket %s now refuses requests that do not use TLS (policy statement %s)", bucket, awspolicy.InsecureTransportSid))
	}
	if done.lifted {
		recordEvent("Normal", "PendingDeletionDenyLifted", fmt.Sprintf(
			"Removed the deletion block (policy statement %s) from S3 bucket %s, which is declared", pendingDeletionDenySid, bucket))
	}
	return nil
}

func hasWebsite(ctx context.Context, client s3API, bucket string) (bool, error) {
	_, err := client.GetBucketWebsite(ctx, &s3sdk.GetBucketWebsiteInput{Bucket: &bucket})
	if hasErrorCode(err, errCodeNoSuchWebsite) {
		return false, nil
	}
	return err == nil, err
}

// warnIfPublic reports a bucket anyone on the internet can reach, as RDS does
// for an instance that is publicly accessible. It changes nothing: making a
// bucket public is a decision that is not the operator's to undo. The check
// is advisory, so a failure to run it is reported and does not fail the pass.
func warnIfPublic(ctx context.Context, client s3API, bucket string, recordEvent status.EventRecorder) {
	if recordEvent == nil {
		return
	}
	via, err := publicExposure(ctx, client, bucket)
	switch {
	case err != nil:
		recordEvent("Warning", "PublicAccessCheckFailed", fmt.Sprintf("Could not tell whether S3 bucket %s is public: %v", bucket, err))
	case len(via) > 0:
		recordEvent("Warning", "BucketPubliclyAccessible", fmt.Sprintf(
			"S3 bucket %s is open to the public through %s. The operator does not change it; serve public content through CloudFront with origin access control, or turn Block Public Access back on",
			bucket, strings.Join(via, " and ")))
	}
}

// publicExposure lists the ways the bucket is open to the public, taking
// Block Public Access into account: a public policy or ACL that the block
// ignores does not make the bucket reachable.
func publicExposure(ctx context.Context, client s3API, bucket string) ([]string, error) {
	block, err := publicAccessBlock(ctx, client, bucket)
	if err != nil {
		return nil, err
	}
	var via []string
	if !boolValue(block.RestrictPublicBuckets) {
		public, err := policyIsPublic(ctx, client, bucket)
		if err != nil {
			return nil, err
		}
		if public {
			via = append(via, "its bucket policy")
		}
	}
	if !boolValue(block.IgnorePublicAcls) {
		public, err := aclIsPublic(ctx, client, bucket)
		if err != nil {
			return nil, err
		}
		if public {
			via = append(via, "its ACL")
		}
	}
	return via, nil
}

func publicAccessBlock(ctx context.Context, client s3API, bucket string) (types.PublicAccessBlockConfiguration, error) {
	out, err := client.GetPublicAccessBlock(ctx, &s3sdk.GetPublicAccessBlockInput{Bucket: &bucket})
	if hasErrorCode(err, errCodeNoSuchPublicBlock) {
		return types.PublicAccessBlockConfiguration{}, nil
	}
	if err != nil || out.PublicAccessBlockConfiguration == nil {
		return types.PublicAccessBlockConfiguration{}, err
	}
	return *out.PublicAccessBlockConfiguration, nil
}

func policyIsPublic(ctx context.Context, client s3API, bucket string) (bool, error) {
	out, err := client.GetBucketPolicyStatus(ctx, &s3sdk.GetBucketPolicyStatusInput{Bucket: &bucket})
	if hasErrorCode(err, errCodeNoSuchPolicy) {
		return false, nil
	}
	if err != nil || out.PolicyStatus == nil {
		return false, err
	}
	return boolValue(out.PolicyStatus.IsPublic), nil
}

func aclIsPublic(ctx context.Context, client s3API, bucket string) (bool, error) {
	out, err := client.GetBucketAcl(ctx, &s3sdk.GetBucketAclInput{Bucket: &bucket})
	if err != nil {
		return false, err
	}
	for _, g := range out.Grants {
		if g.Grantee == nil || g.Grantee.URI == nil {
			continue
		}
		if uri := *g.Grantee.URI; uri == allUsersURI || uri == authenticatedUsersURI {
			return true, nil
		}
	}
	return false, nil
}

func boolValue(b *bool) bool { return b != nil && *b }

func hasErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == code
}
