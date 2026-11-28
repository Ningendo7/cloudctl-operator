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
	"strings"

	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

const (
	// OwnerTagKey identifies which AppDependencies CR owns a resource, as
	// "<namespace>/<name>".
	OwnerTagKey = "cloudctl.io/owner"
	// OwnerUIDTagKey carries the owning CR's UID, so a deleted-and-recreated
	// CR with the same name doesn't get confused with a stale orphan.
	OwnerUIDTagKey = "cloudctl.io/owner-uid"
	// ClusterTagKey names the cluster whose operator manages a resource. Two
	// clusters can hold the same CR with the same UID (one restored from the
	// other's etcd backup); this tag keeps them from both acting on it.
	ClusterTagKey = "cloudctl.io/cluster"
)

// cluster is this process's cluster: --eks-cluster-name and the UID of the
// kube-system namespace. Set once, at startup.
var cluster struct{ eksName, uid string }

// SetCluster sets the cluster this process runs in.
func SetCluster(eksName, kubeSystemUID string) {
	cluster.eksName, cluster.uid = eksName, kubeSystemUID
}

// Cluster is the value written under ClusterTagKey: the EKS cluster name when
// set, else the kube-system namespace UID.
func Cluster() string {
	if cluster.eksName != "" {
		return cluster.eksName
	}
	return cluster.uid
}

// EKSClusterName is --eks-cluster-name, which gives each cluster its own IAM
// roles.
func EKSClusterName() string { return cluster.eksName }

// IsThisCluster reports whether a ClusterTagKey value names this cluster.
// Both names count, so setting --eks-cluster-name later keeps every resource.
func IsThisCluster(value string) bool {
	return value != "" && (value == cluster.eksName || value == cluster.uid)
}

// onThisCluster reports whether tags place a resource on this cluster. One
// with no cluster tag predates it and belongs to whichever cluster claims it.
func onThisCluster(tags map[string]string) bool {
	value, ok := tags[ClusterTagKey]
	return !ok || IsThisCluster(value)
}

// OwnerTagValue formats the value stored under OwnerTagKey.
func OwnerTagValue(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// OwnerTags are the tags that make a resource the CR's, on this cluster.
func OwnerTags(namespace, name, uid string) map[string]string {
	tags := map[string]string{
		OwnerTagKey:    OwnerTagValue(namespace, name),
		OwnerUIDTagKey: uid,
	}
	if c := Cluster(); c != "" {
		tags[ClusterTagKey] = c
	}
	return tags
}

// OwnerTagKeys are the keys removed when a resource is let go.
func OwnerTagKeys() []string {
	return []string{OwnerTagKey, OwnerUIDTagKey, ClusterTagKey}
}

// IsOwnedBy reports whether a resource's tags show it's owned by the given
// CR on this cluster. A resource with no ownership tags, or tags belonging to
// a different CR or cluster, is not owned by us — it must not be adopted,
// mutated, or deleted without an explicit adopt:true or handover.
func IsOwnedBy(tags map[string]string, namespace, name, uid string) bool {
	return tags[OwnerTagKey] == OwnerTagValue(namespace, name) && tags[OwnerUIDTagKey] == uid && onThisCluster(tags)
}

// NeedsClaim reports whether a resource IsOwnedBy accepts still lacks this
// cluster's current name, so the next ownership check writes it.
func NeedsClaim(tags map[string]string) bool {
	c := Cluster()
	return c != "" && tags[ClusterTagKey] != c
}

// IsClaimedBy reports whether a resource is the CR's and already carries this
// cluster's current name, so an ownership check has nothing to write.
func IsClaimedBy(tags map[string]string, namespace, name, uid string) bool {
	return IsOwnedBy(tags, namespace, name, uid) && !NeedsClaim(tags)
}

// OwnedOrTransferable reports whether tags show the CR's ownership, or that of
// an earlier owner it was told to take over from.
func OwnedOrTransferable(ctx context.Context, tags map[string]string, namespace, name, uid string) bool {
	if IsOwnedBy(tags, namespace, name, uid) {
		return true
	}
	from, ok := PreviousOwner(tags, namespace, name, uid)
	return ok && MayTransferFrom(ctx, from)
}

// PreviousOwner reports, for a resource tagged with this CR's name, the earlier
// owner it would be taken over from: the UID of an earlier CR of that name,
// or "<cluster>/<uid>" when another cluster's operator manages it. That value
// in AdoptFromUIDAnnotation hands it over. A name match alone is never
// ownership, even with adopt:true, since the resource may still be in use
// under its old owner.
func PreviousOwner(tags map[string]string, namespace, name, uid string) (from string, ok bool) {
	if tags[OwnerTagKey] != OwnerTagValue(namespace, name) {
		return "", false
	}
	existingUID, hasUID := tags[OwnerUIDTagKey]
	if !hasUID {
		return "", false
	}
	if !onThisCluster(tags) {
		return tags[ClusterTagKey] + "/" + existingUID, true
	}
	return existingUID, existingUID != uid
}

// HandedOver reports whether a resource tagged with this CR's name is now
// managed from another cluster, which took it over. It is that cluster's to
// delete, not this CR's.
func HandedOver(tags map[string]string, namespace, name string) (toCluster string, ok bool) {
	if tags[OwnerTagKey] != OwnerTagValue(namespace, name) || onThisCluster(tags) {
		return "", false
	}
	return tags[ClusterTagKey], true
}

// LeftToOtherCluster reports whether another cluster took a resource over,
// and records that cleanup leaves it alone. The caller drops it from the
// ledger, as it would a resource that is gone.
func LeftToOtherCluster(recordEvent func(eventType, reason, message string), tags map[string]string, namespace, crName, kind, name string) bool {
	toCluster, ok := HandedOver(tags, namespace, crName)
	if ok && recordEvent != nil {
		recordEvent("Normal", "LeftToOtherCluster", fmt.Sprintf("Left %s %q alone: cluster %s took it over", kind, name, toCluster))
	}
	return ok
}

// AdoptFromUIDAnnotation, set on a CR to a previous owner, lets the CR take
// over the resources that owner had. A recreated CR gets a new UID (a
// restore, a re-sync into a new cluster, a blue/green upgrade), and a new
// cluster has its own name, so every resource would otherwise be refused.
// The value is the one PreviousOwner gives, "<uid>" or "<cluster>/<uid>", so
// any other leftover is still refused.
const AdoptFromUIDAnnotation = "cloudctl.io/adopt-from-uid"

// EventOwnershipTransferred is the reason of the event recorded when a
// resource is taken over this way.
const EventOwnershipTransferred = "OwnershipTransferred"

type adoptFromUIDKey struct{}

// WithAdoptFromUID returns ctx carrying the UID the CR was told to take
// resources over from. An empty uid returns ctx unchanged.
func WithAdoptFromUID(ctx context.Context, uid string) context.Context {
	if uid == "" {
		return ctx
	}
	return context.WithValue(ctx, adoptFromUIDKey{}, uid)
}

// TakingOver reports whether the CR was told to take resources over from a
// previous owner.
func TakingOver(ctx context.Context) bool {
	from, _ := ctx.Value(adoptFromUIDKey{}).(string)
	return from != ""
}

// MayTransferFrom reports whether the CR was told to take over from exactly
// this owner, as PreviousOwner gives it.
func MayTransferFrom(ctx context.Context, staleUID string) bool {
	from, _ := ctx.Value(adoptFromUIDKey{}).(string)
	return from != "" && from == staleUID
}

// Takeover is how CheckExistingOwnership lets a resource be tagged as the CR's.
type Takeover int

const (
	// Adopted is an untagged resource the spec says to adopt.
	Adopted Takeover = iota
	// Transferred is one handed over from an earlier owner.
	Transferred
	// Claimed is the CR's own, not yet tagged with this cluster's name.
	Claimed
)

// CheckExistingOwnership decides what to do with a resource that exists but
// isn't tagged for this incarnation of the CR on this cluster. It returns nil
// when the resource may be re-tagged, and how. kind is the noun used in the
// error ("queue", "bucket").
func CheckExistingOwnership(ctx context.Context, kind, name string, tags map[string]string, namespace, crName, crUID string, adopt bool) (Takeover, error) {
	if IsOwnedBy(tags, namespace, crName, crUID) {
		return Claimed, nil
	}
	if owner, ok := tags[OwnerTagKey]; ok && owner != OwnerTagValue(namespace, crName) {
		return Adopted, OwnedByAnotherCRError(kind, name, owner)
	}
	if from, ok := PreviousOwner(tags, namespace, crName, crUID); ok {
		if !MayTransferFrom(ctx, from) {
			return Adopted, PreviousIncarnationError(kind, name, from)
		}
		return Transferred, nil
	}
	if !adopt {
		return Adopted, AdoptRequiredError(kind, name)
	}
	return Adopted, nil
}

// OwnedByAnotherCRError is the refusal for a resource tagged as another CR's.
func OwnedByAnotherCRError(kind, name, owner string) error {
	return &ReconcileError{
		Err:    fmt.Errorf("%s %q is already owned by a different AppDependencies CR (%s) — this looks like a naming collision, not adopting", kind, name, owner),
		Reason: reasons.OwnedByAnotherCR,
	}
}

// PreviousIncarnationError is the refusal for a resource tagged with this
// CR's name and an earlier owner, as PreviousOwner gives it.
func PreviousIncarnationError(kind, name, from string) error {
	if otherCluster, _, found := strings.Cut(from, "/"); found {
		return &ReconcileError{
			Err:    fmt.Errorf("%s %q is managed by the operator in cluster %s, refusing to take it over automatically; if this cluster replaces that one, annotate this CR with %s=%s", kind, name, otherCluster, AdoptFromUIDAnnotation, from),
			Reason: reasons.OwnedByAnotherCluster,
		}
	}
	return &ReconcileError{
		Err:    fmt.Errorf("%s %q is tagged with this CR's name but a different UID (%s) — likely a stale resource from a deleted-and-recreated CR, refusing to adopt automatically; to take it over, annotate this CR with %s=%s", kind, name, from, AdoptFromUIDAnnotation, from),
		Reason: reasons.PreviousIncarnation,
	}
}

// AdoptRequiredError is the refusal for an untagged resource without adopt.
func AdoptRequiredError(kind, name string) error {
	return &ReconcileError{
		Err:    fmt.Errorf("%s %q exists but is not tagged as owned by this CR — set adopt:true to bring it under management", kind, name),
		Reason: reasons.AdoptRequired,
	}
}

// OwnershipLostError is the refusal to act on a resource whose ownership tags
// no longer match; refusal finishes the sentence, such as "delete it".
func OwnershipLostError(kind, name, refusal string) error {
	return &ReconcileError{
		Err:    fmt.Errorf("%s %q no longer verified as owned by this CR — refusing to %s", kind, name, refusal),
		Reason: reasons.OwnershipLost,
	}
}
