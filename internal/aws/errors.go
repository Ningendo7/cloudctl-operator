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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
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
// errCodeAccessDenied and errCodeThrottling are named (rather than inline
// in the switch/if below) purely because each is also referenced by tests
// exercising this exact classification - a constant keeps both sides from
// silently drifting apart on a typo.
const (
	errCodeAccessDenied          = "AccessDenied"
	errCodeAccessDeniedException = "AccessDeniedException"
	errCodeUnauthorizedException = "UnauthorizedException"
	errCodeUnauthorizedOperation = "UnauthorizedOperation"
	errCodeNotAuthorized         = "NotAuthorized"
	errCodeAuthorizationError    = "AuthorizationError"

	errCodeThrottling                            = "ThrottlingException"
	errCodeLimitExceeded                         = "LimitExceededException"
	errCodeRequestLimitExceeded                  = "RequestLimitExceeded"
	errCodeTooManyRequestsException              = "TooManyRequestsException"
	errCodeResourceInUseException                = "ResourceInUseException"
	errCodeConcurrentModification                = "ConcurrentModification"
	errCodeContinuousBackupsUnavailableException = "ContinuousBackupsUnavailableException"
)

func IsPermissionDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case errCodeAccessDenied, errCodeAccessDeniedException, errCodeUnauthorizedException, errCodeUnauthorizedOperation, errCodeNotAuthorized, errCodeAuthorizationError:
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
//
// ContinuousBackupsUnavailableException (DynamoDB) is also a client-fault
// by HTTP status but a transient one: UpdateContinuousBackups/
// DescribeContinuousBackups return it for a brief window right after a
// table reaches ACTIVE, while backups are still being provisioned for it —
// a completely normal sequence for any table created with backup enabled,
// not a request problem.
//
// Failures that never reached AWS, or that the SDK gave up on, are transient
// too: a deadline, the operator's own limiter refusing to wait past one, the
// SDK's retry quota running dry (which drops the original throttling error
// from the chain), and network errors that can pass: a timeout, a connection
// reset or a refused connection. A name that does not resolve or a
// certificate that is not trusted is a setting that is wrong, so it is not.
func IsRetryable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrRateLimitWait) {
		return true
	}
	var quota ratelimit.QuotaExceededError
	if errors.As(err, &quota) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return !IsEndpointUnreachable(err) && isTransientNetworkError(netErr, err)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return isRetryableAPIError(apiErr)
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

// isTransientLimit reports the two DynamoDB limits that clear on their own: a
// change to the table's tags is still in flight, and too many control-plane
// operations are running at once. Other services answer the same code for a
// quota that retrying never fixes (a key, alias or grant limit in KMS), so
// only these messages count.
func isTransientLimit(message string) bool {
	return strings.Contains(message, "Table tags are being updated") ||
		strings.Contains(message, "can be created, updated, or deleted simultaneously")
}

func isRetryableAPIError(apiErr smithy.APIError) bool {
	if apiErr.ErrorFault() == smithy.FaultServer {
		return true
	}
	if apiErr.ErrorCode() == errCodeLimitExceeded {
		return isTransientLimit(apiErr.ErrorMessage())
	}
	switch code := apiErr.ErrorCode(); code {
	case errCodeThrottling, errCodeRequestLimitExceeded, errCodeTooManyRequestsException,
		errCodeResourceInUseException, errCodeConcurrentModification, errCodeContinuousBackupsUnavailableException:
		return true
	default:
		return strings.Contains(code, "Throttl")
	}
}

func isTransientNetworkError(netErr net.Error, err error) bool {
	var dnsErr *net.DNSError
	return netErr.Timeout() ||
		(errors.As(err, &dnsErr) && dnsErr.IsTemporary) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED)
}

// IsEndpointUnreachable reports a failure to reach AWS that waiting will not
// fix: the endpoint's name does not resolve, or the certificate it presents
// is not trusted. Typically a mistyped AWS_ENDPOINT_URL, or a proxy.
func IsEndpointUnreachable(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var invalidCert x509.CertificateInvalidError
	var wrongHost x509.HostnameError
	return errors.As(err, &verifyErr) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &invalidCert) || errors.As(err, &wrongHost)
}

// ReconcileError wraps an AWS-originated error with a hint about whether
// the controller should retry quickly (transient) or the problem needs a
// human (e.g. a permissions gap that won't resolve on its own). Resource
// packages wrap errors with this so the controller can pick an appropriate
// RequeueAfter and status condition reason without re-deriving the
// classification itself.
// Reason, if set, names a distinct, stable condition reason for this
// error - used instead of the generic PermissionDenied/TransientError/
// Error classification, so a caller can alert on this exact Reason
// without it being confused with (or masked by) an ordinary transient
// failure of the same section.
//
// WakesOnChange marks a wait that ends when something this operator watches
// changes (a grant created, a producer's topic published), so polling is only
// a fallback and can be slow.
type ReconcileError struct {
	Err           error
	Retryable     bool
	Reason        string
	WakesOnChange bool
}

func (e *ReconcileError) Error() string { return e.Err.Error() }
func (e *ReconcileError) Unwrap() error { return e.Err }
