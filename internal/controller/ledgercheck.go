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

// checkLedger returns the first entry of cr's ledger that fails its check. An
// entry of a type no check knows is skipped: no code acts on it either.
func checkLedger(id cloudctlaws.Identity, cr *depsv1alpha1.AppDependencies) error {
	if !id.Known() {
		return nil
	}
	for _, entry := range cr.Status.ManagedResources {
		check, known := ledgerChecks[entry.Type]
		if !known || entry.ARN == "" {
			continue
		}
		if err := check(id, cr.Namespace, cr.Name, entry); err != nil {
			return err
		}
	}
	return nil
}

// refuseLedger stops a pass before any section when the ledger can't be acted
// on from here: it is in another region, or holds an entry the operator would
// not have written.
func refuseLedger(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	if refused, err := refuseOtherRegion(ctx, r, cr, original); refused || err != nil {
		return refused, err
	}
	return refuseTamperedLedger(ctx, r, cr, original)
}

// refuseTamperedLedger stops a pass before it reaches AWS when the ledger
// holds an entry the operator would not have written. Every section acts on
// the ARNs the ledger holds, with the operator's own AWS permissions, and
// anyone who may write the CR's status can write them.
func refuseTamperedLedger(ctx context.Context, r *AppDependenciesReconciler, cr, original *depsv1alpha1.AppDependencies) (bool, error) {
	err := checkLedger(cloudctlaws.NewIdentity(r.AWSClients.Region, r.AWSClients.AccountID), cr)
	if err == nil {
		err = r.verifyChosenIDs(ctx, cr)
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

// verifyChosenIDs checks, by its tags, that each KMS key and security group in
// cr's ledger is cr's, once per ARN per process. What it has checked is kept
// in memory, where nothing written to status can add to it. A relinquished
// entry is skipped: it no longer carries the owner tags, and nothing acts on
// it as the CR's. A resource that no longer exists is left to its section to
// report.
func (r *AppDependenciesReconciler) verifyChosenIDs(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
	for _, entry := range cr.Status.ManagedResources {
		check, ok := chosenIDChecks[entry.Type]
		if !ok || entry.ARN == "" || entry.State == depsv1alpha1.ManagedResourceStateRelinquished {
			continue
		}
		key := string(cr.UID) + "\x00" + entry.Type + "\x00" + entry.Name
		if seen, ok := r.verifiedIDs.Load(key); ok && seen == entry.ARN {
			continue
		}
		owned, found, err := check(ctx, r, entry.ARN, cr.Namespace, cr.Name, string(cr.UID))
		switch {
		case err != nil:
			return err
		case !found:
			continue
		case !owned:
			return &cloudctlaws.LedgerMismatchError{Type: entry.Type, Name: entry.Name, ARN: entry.ARN, Want: "a resource tagged as this CR's"}
		}
		r.verifiedIDs.Store(key, entry.ARN)
	}
	return nil
}
