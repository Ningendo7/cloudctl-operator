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
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
)

// fakeAPIError is a minimal smithy.APIError implementation for exercising
// the classification helpers without needing a real AWS SDK error type.
type fakeAPIError struct {
	code  string
	fault smithy.ErrorFault
}

func (e *fakeAPIError) Error() string                 { return e.code }
func (e *fakeAPIError) ErrorCode() string             { return e.code }
func (e *fakeAPIError) ErrorMessage() string          { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return e.fault }

func TestIsPermissionDenied(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"AccessDenied", &fakeAPIError{code: "AccessDenied"}, true},
		{"AccessDeniedException", &fakeAPIError{code: "AccessDeniedException"}, true},
		{"UnauthorizedException", &fakeAPIError{code: "UnauthorizedException"}, true},
		{"UnauthorizedOperation", &fakeAPIError{code: "UnauthorizedOperation"}, true},
		{"unrelated code", &fakeAPIError{code: "ThrottlingException"}, false},
		{"not an APIError at all", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermissionDenied(tc.err); got != tc.want {
				t.Errorf("IsPermissionDenied(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"server fault", &fakeAPIError{code: "InternalError", fault: smithy.FaultServer}, true},
		{"client fault, throttling code", &fakeAPIError{code: "ThrottlingException", fault: smithy.FaultClient}, true},
		{"client fault, request limit code", &fakeAPIError{code: "RequestLimitExceeded", fault: smithy.FaultClient}, true},
		{"client fault, contains Throttl", &fakeAPIError{code: "SomeServiceThrottlingError", fault: smithy.FaultClient}, true},
		{"client fault, unrelated code", &fakeAPIError{code: "ValidationException", fault: smithy.FaultClient}, false},
		{"not an APIError at all", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRetryable(tc.err); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestReconcileError_WrapsAndUnwraps(t *testing.T) {
	inner := errors.New("boom")
	wrapped := &ReconcileError{Err: inner, Retryable: true}

	if wrapped.Error() != inner.Error() {
		t.Errorf("Error() = %q, want %q", wrapped.Error(), inner.Error())
	}
	if !errors.Is(wrapped, inner) {
		t.Error("expected errors.Is to see through ReconcileError to the wrapped error")
	}

	var target *ReconcileError
	if !errors.As(fmt.Errorf("context: %w", wrapped), &target) {
		t.Fatal("expected errors.As to find the ReconcileError through additional wrapping")
	}
	if !target.Retryable {
		t.Error("expected Retryable to survive being wrapped further")
	}
}
