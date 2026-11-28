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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/dynamodb"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/s3"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sns"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/sqs"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// ledgerCheck reports whether a ledger entry's ARN is one the operator would
// have written for the CR named.
type ledgerCheck func(id cloudctlaws.Identity, namespace, crName string, entry depsv1alpha1.ManagedResource) error

// ledgerChecks has one check per ledger entry type, each in the package that
// names that type's resources, so a check can't drift from the names it
// creates.
var ledgerChecks = map[string]ledgerCheck{
	resourceTypeSQS:                     sqs.CheckLedgerEntry,
	sqs.SubscriptionResourceType:        sqs.CheckSubscriptionEntry,
	resourceTypeSNS:                     sns.CheckLedgerEntry,
	resourceTypeDynamoDB:                dynamodb.CheckLedgerEntry,
	resourceTypeS3:                      s3.CheckLedgerEntry,
	resourceTypeRDS:                     rds.CheckLedgerEntry,
	rds.SecurityGroupResourceType:       rds.CheckSecurityGroupEntry,
	cloudctlaws.ResourceTypeRDSSnapshot: rds.CheckSnapshotEntry,
	resourceTypeKMS:                     kms.CheckLedgerEntry,
	resourceTypeIAM:                     iam.CheckLedgerEntry,
}

// checkLedger returns the first entry of cr's ledger that fails its ARN check.
func checkLedger(id cloudctlaws.Identity, cr *depsv1alpha1.AppDependencies) error {
	for _, entry := range cr.Status.ManagedResources {
		if err := checkEntry(id, cr, entry); err != nil {
			return err
		}
	}
	return nil
}

// checkEntry checks one entry's ARN against the one the operator would have
// built. An entry of a type no check knows is skipped: no code acts on it
// either.
func checkEntry(id cloudctlaws.Identity, cr *depsv1alpha1.AppDependencies, entry depsv1alpha1.ManagedResource) error {
	check, known := ledgerChecks[entry.Type]
	if !id.Known() || !known || entry.ARN == "" {
		return nil
	}
	return check(id, cr.Namespace, cr.Name, entry)
}

// verifyEntry is the status.EntryVerifier a pass carries: the ARN check, then
// for an ID AWS chose, its tags.
func (r *AppDependenciesReconciler) verifyEntry(ctx context.Context, cr *depsv1alpha1.AppDependencies, entry depsv1alpha1.ManagedResource) error {
	if err := checkEntry(r.identity(), cr, entry); err != nil {
		return err
	}
	return r.verifyChosenID(ctx, cr, entry)
}

func (r *AppDependenciesReconciler) identity() cloudctlaws.Identity {
	return cloudctlaws.NewIdentity(r.AWSClients.Region, r.AWSClients.AccountID)
}

// refuseLedger stops a pass before any section when the ledger can't be acted
// on from here: it is in another region, or holds an entry the operator would
// not have written.
func refuseLedger(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	if refused, err := refuseOtherRegion(ctx, r, cr, original); refused || err != nil {
		return refused, err
	}
	if refused, err := refuseOtherDeployment(ctx, r, cr, original); refused || err != nil {
		return refused, err
	}
	if refused, err := refuseOtherCluster(ctx, r, cr, original); refused || err != nil {
		return refused, err
	}
	return refuseTamperedLedger(ctx, r, cr, original)
}

// refuseOtherCluster stops a CR whose resources were built by the operator in
// another cluster: a cluster restored from another's etcd backup has the same
// CRs, with the same UIDs, and both operators would act on the same
// resources. The handover annotation naming that cluster lets the pass
// through; that cluster keeps its IAM role, which its pods use.
func refuseOtherCluster(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	recorded := cr.Status.Cluster
	if len(cr.Status.ManagedResources) == 0 || recorded == "" || cloudctlaws.IsThisCluster(recorded) {
		return false, nil
	}
	if from, _, found := strings.Cut(cr.Annotations[cloudctlaws.AdoptFromUIDAnnotation], "/"); found && from == recorded {
		// That cluster's operator verified these entries; checking each
		// again now is what takes it over.
		for i := range cr.Status.ManagedResources {
			cr.Status.ManagedResources[i].LastVerifiedAt = nil
		}
		status.RemoveManagedResource(&cr.Status.ManagedResources, resourceTypeIAM, "role")
		return false, nil
	}
	setSectionCondition(ctx, cr, sectionForEntryType(cr.Status.ManagedResources[0].Type), &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("this CR's resources were built by the operator in cluster %s, not this one (%s); nothing is reconciled. "+
			"If this cluster replaces that one, annotate the CR with %s=%s/%s; to delete the CR and leave its resources to that cluster, remove its finalizer",
			recorded, cloudctlaws.Cluster(), cloudctlaws.AdoptFromUIDAnnotation, recorded, cr.UID),
		Reason: reasons.ClusterChanged,
	}, eventRecorderFor(r, cr))
	return true, r.Status().Patch(ctx, cr, statusPatch(original, cr))
}

// refuseOtherDeployment stops a CR whose resources were named under another
// deployment name than this operator's: building names under this one would
// create new resources and lose track of the old. A CR with nothing in its
// ledger yet may take this operator's name.
func refuseOtherDeployment(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	ours := cloudctlaws.Deployment()
	if len(cr.Status.ManagedResources) == 0 || cr.Status.Deployment == ours {
		return false, nil
	}
	setSectionCondition(ctx, cr, sectionForEntryType(cr.Status.ManagedResources[0].Type), &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("this CR's resources are named for deployment %q, but this operator is deployment %q; nothing is reconciled, "+
			"since its names would point at new resources", cr.Status.Deployment, ours),
		Reason: reasons.DeploymentChanged,
	}, eventRecorderFor(r, cr))
	return true, r.Status().Patch(ctx, cr, statusPatch(original, cr))
}

// refuseTamperedLedger stops a pass before it reaches AWS when the ledger
// holds an entry the operator would not have written. Every section acts on
// the ARNs the ledger holds, with the operator's own AWS permissions, and
// anyone who may write the CR's status can write them.
func refuseTamperedLedger(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	// Every ARN first, so a forged one stops the pass before any AWS call.
	err := checkLedger(r.identity(), cr)
	for _, entry := range cr.Status.ManagedResources {
		if err != nil {
			break
		}
		err = r.verifyChosenID(ctx, cr, entry)
	}
	if err == nil {
		return false, nil
	}
	if _, tampered := errors.AsType[*cloudctlaws.LedgerMismatchError](err); !tampered {
		return false, err
	}
	mismatch, _ := errors.AsType[*cloudctlaws.LedgerMismatchError](err)
	section := sectionForEntryType(mismatch.Type)
	setSectionCondition(ctx, cr, section, &cloudctlaws.ReconcileError{
		Err:    fmt.Errorf("%w; nothing is reconciled until it is corrected", err),
		Reason: reasons.LedgerTampered,
	}, eventRecorderFor(r, cr))
	return true, r.Status().Patch(ctx, cr, statusPatch(original, cr))
}

// ownedBy reads the tags of a resource whose ID AWS chose, which its ARN can't
// tie to a CR.
type ownedBy func(ctx context.Context, r *AppDependenciesReconciler, arn, namespace, crName, crUID string) (owned, found bool, err error)

var chosenIDChecks = map[string]ownedBy{
	resourceTypeKMS: func(ctx context.Context, r *AppDependenciesReconciler, arn, namespace, crName, crUID string) (bool, bool, error) {
		return kms.KeyOwnedBy(ctx, r.AWSClients.KMS, arn, namespace, crName, crUID)
	},
	rds.SecurityGroupResourceType: func(ctx context.Context, r *AppDependenciesReconciler, arn, namespace, crName, crUID string) (bool, bool, error) {
		if r.AWSClients.EC2 == nil {
			return false, false, nil
		}
		return rds.SecurityGroupOwnedBy(ctx, r.AWSClients.EC2, arn, namespace, crName, crUID)
	},
}

// verifyChosenID checks, by its tags, that a KMS key or security group in cr's
// ledger is cr's, once per ARN per process. What it has checked is kept in
// memory, where nothing written to status can add to it. A relinquished entry
// is skipped: it no longer carries the owner tags, and nothing acts on it as
// the CR's. A resource that no longer exists is left to its section to report.
func (r *AppDependenciesReconciler) verifyChosenID(ctx context.Context, cr *depsv1alpha1.AppDependencies, entry depsv1alpha1.ManagedResource) error {
	check, ok := chosenIDChecks[entry.Type]
	if !ok || entry.ARN == "" || entry.State == depsv1alpha1.ManagedResourceStateRelinquished {
		return nil
	}
	key := string(cr.UID) + "\x00" + entry.Type + "\x00" + entry.Name
	if seen, ok := r.verifiedIDs.Load(key); ok && seen == entry.ARN {
		return nil
	}
	owned, found, err := check(ctx, r, entry.ARN, cr.Namespace, cr.Name, string(cr.UID))
	switch {
	case err != nil:
		return err
	case !found:
		return nil
	case !owned:
		return &cloudctlaws.LedgerMismatchError{Type: entry.Type, Name: entry.Name, ARN: entry.ARN, Want: "a resource tagged as this CR's"}
	}
	r.verifiedIDs.Store(key, entry.ARN)
	return nil
}
