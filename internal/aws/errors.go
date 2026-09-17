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
	"errors"
	"strings"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
)

// IsPermissionDenied reports whether an AWS error indicates our own IAM
// role lacks permission to perform the call — not something retrying will
// fix, needs a human to fix the controller's IAM policy.
//
// Every service used here shares "NotAuthorized" as a common error type
// (confirmed for SQS, DynamoDB, and IAM's own docs); SNS additionally uses
// its own "AuthorizationError" instead of any of the more common codes.
// Checked here rather than per resource package for the same reason
// ResourceInUseException lives in IsRetryable: nothing about the
// classification is specific to one call site.
func IsPermissionDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "AccessDeniedException", "UnauthorizedException", "UnauthorizedOperation", "NotAuthorized", "AuthorizationError":
			return true
		}
		return false
	}
	// Some calls (S3's HeadBucket is the confirmed case) return a bare
	// HTTP-status error with no error code at all — it doesn't implement
	// smithy.APIError, so the switch above can never match it regardless
	// of what codes it lists. Fall back to the raw HTTP status in that
	// case, mirroring the same "check both the typed exception and the
	// raw status" pattern the s3 package already uses for isNotFoundError.
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == 403 {
		return true
	}
	return false
}

// IsRetryable reports whether an AWS error is likely transient — AWS's own
// fault (throttling, internal errors, service unavailable) rather than a
// problem with our request that retrying won't fix. The SDK's own retryer
// already retries server-fault errors internally before one ever reaches
// us, so seeing one here means those internal retries were exhausted —
// still worth a longer-horizon reconcile-level retry, not a hard failure.
//
// ResourceInUseException (DynamoDB) is a client-fault by HTTP status, but
// semantically the same kind of transient as throttling: it means the
// resource is mid-transition from a previous operation, not that our
// request itself is invalid. Included here rather than special-cased per
// call site, since every DynamoDB control-plane call (CreateTable,
// UpdateTable, DeleteTable, UpdateContinuousBackups) can hit it, and
// nothing about the classification is specific to any one of them.
//
// ConcurrentModification (IAM) is the same idea for role mutations:
// CreateRole/UpdateAssumeRolePolicy/PutRolePolicy/DeleteRole can all return
// it when two requests touch the same role at once, and waiting and
// retrying is AWS's own documented remedy.
func IsRetryable(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorFault() == smithy.FaultServer {
			return true
		}
		code := apiErr.ErrorCode()
		if code == "ThrottlingException" || code == "RequestLimitExceeded" || code == "TooManyRequestsException" || code == "ResourceInUseException" || code == "ConcurrentModification" {
			return true
		}
		return strings.Contains(code, "Throttl")
	}
	// See IsPermissionDenied: a bare HTTP-status error (no smithy.APIError)
	// can't be matched by error code at all. 429 and 5xx are AWS's own
	// generic signals for "retry me" regardless of service.
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		status := respErr.HTTPStatusCode()
		return status == 429 || status >= 500
	}
	return false
}

// ReconcileError wraps an AWS-originated error with a hint about whether
// the controller should retry quickly (transient) or the problem needs a
// human (e.g. a permissions gap that won't resolve on its own). Resource
// packages wrap errors with this so the controller can pick an appropriate
// RequeueAfter and status condition reason without re-deriving the
// classification itself.
type ReconcileError struct {
	Err       error
	Retryable bool
}

func (e *ReconcileError) Error() string { return e.Err.Error() }
func (e *ReconcileError) Unwrap() error { return e.Err }
