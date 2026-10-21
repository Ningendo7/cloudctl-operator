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
	"errors"
	"strings"
	"testing"

	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

func TestIsOwnedBy(t *testing.T) {
	tags := map[string]string{
		OwnerTagKey:    OwnerTagValue("default", "checkout-service"),
		OwnerUIDTagKey: "abc-123",
	}

	if !IsOwnedBy(tags, "default", "checkout-service", "abc-123") {
		t.Error("expected matching namespace/name/uid to be owned")
	}
	if IsOwnedBy(tags, "default", "checkout-service", "different-uid") {
		t.Error("expected mismatched uid to not be owned (stale recreated CR)")
	}
	if IsOwnedBy(tags, "other-namespace", "checkout-service", "abc-123") {
		t.Error("expected mismatched namespace to not be owned")
	}
	if IsOwnedBy(nil, "default", "checkout-service", "abc-123") {
		t.Error("expected untagged resource to not be owned")
	}
}

func TestIsStaleUID(t *testing.T) {
	tags := map[string]string{
		OwnerTagKey:    OwnerTagValue("default", "checkout-service"),
		OwnerUIDTagKey: "old-uid",
	}

	uid, stale := IsStaleUID(tags, "default", "checkout-service", "new-uid")
	if !stale {
		t.Fatal("expected a name match with a different UID to be reported stale")
	}
	if uid != "old-uid" {
		t.Errorf("expected the existing UID %q returned, got %q", "old-uid", uid)
	}

	if _, stale := IsStaleUID(tags, "default", "checkout-service", "old-uid"); stale {
		t.Error("expected a matching UID to not be reported stale")
	}
	if _, stale := IsStaleUID(tags, "other-namespace", "checkout-service", "new-uid"); stale {
		t.Error("expected a different namespace/name to not be reported stale - that's a different-CR collision, not a stale UID")
	}
	if _, stale := IsStaleUID(nil, "default", "checkout-service", "new-uid"); stale {
		t.Error("expected a genuinely untagged resource to not be reported stale")
	}
}

func TestMayTransferFrom(t *testing.T) {
	ctx := WithAdoptFromUID(context.Background(), "old-uid")
	if !MayTransferFrom(ctx, "old-uid") {
		t.Error("the UID the CR was told to take over from must be accepted")
	}
	if MayTransferFrom(ctx, "some-other-uid") {
		t.Error("any other stale UID must still be refused")
	}
	if MayTransferFrom(context.Background(), "old-uid") {
		t.Error("without the annotation nothing is transferable")
	}
	if MayTransferFrom(WithAdoptFromUID(context.Background(), ""), "") {
		t.Error("an empty annotation must not match an empty UID")
	}
}

func TestCheckExistingOwnership(t *testing.T) {
	mine := func(uid string) map[string]string {
		return map[string]string{OwnerTagKey: OwnerTagValue("ns", "app"), OwnerUIDTagKey: uid}
	}
	told := WithAdoptFromUID(context.Background(), "old-uid")
	plain := context.Background()

	tests := []struct {
		name            string
		ctx             context.Context
		tags            map[string]string
		adopt           bool
		wantTransferred bool
		wantErr         string
		wantReason      string
	}{
		{"untagged without adopt", plain, nil, false, false, "set adopt:true", reasons.AdoptRequired},
		{"untagged with adopt", plain, nil, true, false, "", ""},
		{"another CR owns it, even with adopt", plain, map[string]string{OwnerTagKey: "ns/other"}, true, false, "naming collision", reasons.OwnedByAnotherCR},
		{"previous UID without the annotation, even with adopt", plain, mine("old-uid"), true, false, "adopt-from-uid=old-uid", reasons.PreviousIncarnation},
		{"previous UID named by the annotation", told, mine("old-uid"), false, true, "", ""},
		{"a different previous UID than the annotation", told, mine("other-uid"), true, false, "adopt-from-uid=other-uid", reasons.PreviousIncarnation},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transferred, err := CheckExistingOwnership(tc.ctx, "queue", "q", tc.tags, "ns", "app", "new-uid", tc.adopt)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
			var reconcileErr *ReconcileError
			if tc.wantReason != "" && (!errors.As(err, &reconcileErr) || reconcileErr.Reason != tc.wantReason || reconcileErr.Retryable) {
				t.Fatalf("error = %#v, want a non-retryable refusal with reason %q", err, tc.wantReason)
			}
			if transferred != tc.wantTransferred {
				t.Errorf("transferred = %v, want %v", transferred, tc.wantTransferred)
			}
		})
	}
}

func TestOwnershipLostError(t *testing.T) {
	err := OwnershipLostError("queue", "orders", "delete it")
	var reconcileErr *ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.OwnershipLost || reconcileErr.Retryable {
		t.Fatalf("got %#v, want a non-retryable OwnershipLost", err)
	}
	if !strings.Contains(err.Error(), `queue "orders" no longer verified as owned by this CR`) {
		t.Errorf("unexpected message: %v", err)
	}
}
