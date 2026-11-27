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

func TestPreviousOwner(t *testing.T) {
	withCluster(t, "green", "uid-green")
	tags := map[string]string{
		OwnerTagKey:    OwnerTagValue("default", "checkout-service"),
		OwnerUIDTagKey: "old-uid",
	}

	from, stale := PreviousOwner(tags, "default", "checkout-service", "new-uid")
	if !stale || from != "old-uid" {
		t.Fatalf("PreviousOwner = %q, %v; want the earlier UID", from, stale)
	}
	if _, stale := PreviousOwner(tags, "default", "checkout-service", "old-uid"); stale {
		t.Error("expected a matching UID to not be reported stale")
	}
	if _, stale := PreviousOwner(tags, "other-namespace", "checkout-service", "new-uid"); stale {
		t.Error("expected a different namespace/name to not be reported stale - that's a different-CR collision, not a stale UID")
	}
	if _, stale := PreviousOwner(nil, "default", "checkout-service", "new-uid"); stale {
		t.Error("expected a genuinely untagged resource to not be reported stale")
	}

	// Another cluster's resource is a previous owner even with the same UID,
	// as in a cluster restored from another's etcd backup.
	tags[ClusterTagKey] = "blue"
	if from, stale := PreviousOwner(tags, "default", "checkout-service", "old-uid"); !stale || from != "blue/old-uid" {
		t.Errorf("PreviousOwner = %q, %v; want blue/old-uid", from, stale)
	}
	tags[ClusterTagKey] = "uid-green"
	if from, _ := PreviousOwner(tags, "default", "checkout-service", "new-uid"); from != "old-uid" {
		t.Errorf("PreviousOwner = %q; this cluster's other name must not name a cluster", from)
	}
}

// withCluster sets the cluster for one test.
func withCluster(t *testing.T, eksName, uid string) {
	t.Helper()
	before := cluster
	SetCluster(eksName, uid)
	t.Cleanup(func() { cluster = before })
}

func TestCluster_OwnershipFollowsTheClusterTag(t *testing.T) {
	withCluster(t, "green", "uid-green")
	mine := func(clusterTag string) map[string]string {
		tags := map[string]string{OwnerTagKey: OwnerTagValue("ns", "app"), OwnerUIDTagKey: "uid"}
		if clusterTag != "" {
			tags[ClusterTagKey] = clusterTag
		}
		return tags
	}

	tests := []struct {
		name       string
		tags       map[string]string
		owned      bool
		needsClaim bool
		handedOver bool
	}{
		{"tagged before clusters were recorded", mine(""), true, true, false},
		{"tagged with this cluster", mine("green"), true, false, false},
		{"tagged with this cluster's UID, before --eks-cluster-name", mine("uid-green"), true, true, false},
		{"taken over by another cluster", mine("blue"), false, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsOwnedBy(tc.tags, "ns", "app", "uid"); got != tc.owned {
				t.Errorf("IsOwnedBy = %v, want %v", got, tc.owned)
			}
			if got := NeedsClaim(tc.tags); got != tc.needsClaim {
				t.Errorf("NeedsClaim = %v, want %v", got, tc.needsClaim)
			}
			if _, got := HandedOver(tc.tags, "ns", "app"); got != tc.handedOver {
				t.Errorf("HandedOver = %v, want %v", got, tc.handedOver)
			}
		})
	}

	if got := OwnerTags("ns", "app", "uid")[ClusterTagKey]; got != "green" {
		t.Errorf("OwnerTags writes cluster %q, want the EKS name", got)
	}
	SetCluster("", "uid-green")
	if got := OwnerTags("ns", "app", "uid")[ClusterTagKey]; got != "uid-green" {
		t.Errorf("OwnerTags writes cluster %q without an EKS name, want the kube-system UID", got)
	}
	if _, handedOver := HandedOver(mine("blue"), "ns", "other"); handedOver {
		t.Error("another CR's resource is never reported handed over")
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
	withCluster(t, "green", "uid-green")
	mine := func(uid string) map[string]string {
		return map[string]string{OwnerTagKey: OwnerTagValue("ns", "app"), OwnerUIDTagKey: uid}
	}
	blue := func(uid string) map[string]string {
		tags := mine(uid)
		tags[ClusterTagKey] = "blue"
		return tags
	}
	told := WithAdoptFromUID(context.Background(), "old-uid")
	toldBlue := WithAdoptFromUID(context.Background(), "blue/old-uid")
	plain := context.Background()

	tests := []struct {
		name       string
		ctx        context.Context
		tags       map[string]string
		adopt      bool
		want       Takeover
		wantErr    string
		wantReason string
	}{
		{"untagged without adopt", plain, nil, false, Adopted, "set adopt:true", reasons.AdoptRequired},
		{"untagged with adopt", plain, nil, true, Adopted, "", ""},
		{"another CR owns it, even with adopt", plain, map[string]string{OwnerTagKey: "ns/other"}, true, Adopted, "naming collision", reasons.OwnedByAnotherCR},
		{"previous UID without the annotation, even with adopt", plain, mine("old-uid"), true, Adopted, "adopt-from-uid=old-uid", reasons.PreviousIncarnation},
		{"previous UID named by the annotation", told, mine("old-uid"), false, Transferred, "", ""},
		{"a different previous UID than the annotation", told, mine("other-uid"), true, Adopted, "adopt-from-uid=other-uid", reasons.PreviousIncarnation},
		{"its own, without this cluster's tag", plain, mine("new-uid"), false, Claimed, "", ""},
		{"another cluster's, even with adopt", plain, blue("old-uid"), true, Adopted, "adopt-from-uid=blue/old-uid", reasons.OwnedByAnotherCluster},
		{"another cluster's, named without the cluster", told, blue("old-uid"), true, Adopted, "adopt-from-uid=blue/old-uid", reasons.OwnedByAnotherCluster},
		{"another cluster's, handed over", toldBlue, blue("old-uid"), false, Transferred, "", ""},
		{"another cluster's, same UID, handed over", WithAdoptFromUID(context.Background(), "blue/new-uid"), blue("new-uid"), false, Transferred, "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CheckExistingOwnership(tc.ctx, "queue", "q", tc.tags, "ns", "app", "new-uid", tc.adopt)
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
			if err == nil && got != tc.want {
				t.Errorf("CheckExistingOwnership = %v, want %v", got, tc.want)
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
