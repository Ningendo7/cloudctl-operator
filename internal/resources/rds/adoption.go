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

package rds

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// checkAdoptable refuses to adopt an existing, untagged instance whose
// settings differ from the spec. Adoption must not change an instance, and
// some differences can never be reconciled at all. It runs before anything
// is created or modified for the instance, so a refusal leaves no security
// group, key, tag or modify call behind. Instances that are already ours,
// or owned by someone else, are left to ensureInstance's ownership checks.
// The CRD forbids editing the engine or subnet group of an entry, but a
// retained instance that is removed and declared again is adopted like a
// foreign one, so the same differences are caught.
func checkAdoptable(
	ctx context.Context,
	awsClient rdsAPI,
	kmsClient cloudctlaws.KMSClient,
	namespace, crName string,
	r depsv1alpha1.RDSInstanceSpec,
	ledger []depsv1alpha1.ManagedResource,
) error {
	if !r.Adopt {
		return nil
	}
	// A relinquished instance is no longer ours, so it is compared like any
	// other untagged one.
	if entry, tracked := status.FindManagedResource(ledger, resourceType, r.Name); tracked && entry.State != depsv1alpha1.ManagedResourceStateRelinquished {
		return nil
	}
	instanceID := cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, namespace, crName, resourceType, r.Name, 63)
	out, err := describeDBInstanceCached(ctx, awsClient, instanceID)
	var notFound *types.DBInstanceNotFoundFault
	if errors.As(err, &notFound) {
		return nil
	}
	if err != nil {
		return wrapAWSError(err, "looking up instance")
	}
	if len(out.DBInstances) == 0 {
		return nil
	}
	instance := out.DBInstances[0]

	tagsOut, err := awsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: instance.DBInstanceArn})
	if err != nil {
		return wrapAWSError(err, "reading instance tags")
	}
	if _, tagged := tagsToMap(tagsOut.TagList)[cloudctlaws.OwnerTagKey]; tagged {
		return nil
	}

	conflicts := adoptionConflicts(instance, r)
	keyConflict, err := encryptionConflict(ctx, kmsClient, instance, r, ledger)
	if err != nil {
		return err
	}
	if keyConflict != "" {
		conflicts = append(conflicts, keyConflict)
	}
	if len(conflicts) == 0 {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("instance %q exists but cannot be adopted as declared: %s - adoption never changes an instance, "+
			"so make the spec match what exists, adopt, and then change the spec", instanceID, strings.Join(conflicts, "; ")),
		Reason: reasons.AdoptionConflict,
	}
}

// encryptionConflict says how the instance's storage encryption differs from
// what the spec asks for. RDS can't change either afterwards. An unset
// encryption means AWS's own RDS key.
func encryptionConflict(
	ctx context.Context,
	kmsClient cloudctlaws.KMSClient,
	instance types.DBInstance,
	r depsv1alpha1.RDSInstanceSpec,
	ledger []depsv1alpha1.ManagedResource,
) (string, error) {
	have := aws.ToBool(instance.StorageEncrypted)
	key := aws.ToString(instance.KmsKeyId)
	e := r.Encryption
	switch {
	case e != nil && e.Enabled:
		if have {
			return "storage is encrypted with an existing key, but encryption.enabled asks for a new one, which RDS cannot switch to; point encryption.kmsKeyRef at the instance's key instead", nil
		}
		return "storage is not encrypted (spec: encryption.enabled), and RDS cannot encrypt an existing instance", nil
	case e != nil && e.KMSKeyRef != "":
		if !have {
			return "storage is not encrypted (spec: encryption.kmsKeyRef), and RDS cannot encrypt an existing instance", nil
		}
		if want, ok := kms.SharedKeyARN(ledger, e.KMSKeyRef); ok && key != want {
			return fmt.Sprintf("kms key is %s (spec: %s)", key, want), nil
		}
		return "", nil
	case e != nil:
		if have {
			return "storage is encrypted (spec: encryption.enabled: false)", nil
		}
		return "", nil
	}
	if !have {
		return "storage is not encrypted, and an unset encryption means AWS's RDS key; set encryption: {enabled: false} to adopt it unencrypted", nil
	}
	err := kms.CheckAdoptedKey(ctx, kmsClient, key, "")
	var conflict *cloudctlaws.ReconcileError
	if errors.As(err, &conflict) && conflict.Reason == reasons.AdoptionConflict {
		return conflict.Err.Error(), nil
	}
	return "", err
}

// adoptionConflicts lists the differences between an existing instance and
// the spec that adoption must not paper over: settings that can't be changed
// afterwards (engine, subnet group, encryption), public accessibility, and settings the operator
// would otherwise change on its first reconcile (class, Multi-AZ, a higher
// engine version or storage size than the instance has).
func adoptionConflicts(instance types.DBInstance, r depsv1alpha1.RDSInstanceSpec) []string {
	var conflicts []string
	differs := func(field, actual, want string) {
		conflicts = append(conflicts, fmt.Sprintf("%s is %s (spec: %s)", field, actual, want))
	}

	if actual := aws.ToString(instance.Engine); actual != r.Engine {
		differs("engine", actual, r.Engine)
	}
	actualSubnetGroup := ""
	if instance.DBSubnetGroup != nil {
		actualSubnetGroup = aws.ToString(instance.DBSubnetGroup.DBSubnetGroupName)
	}
	if actualSubnetGroup != r.DBSubnetGroupName {
		differs("dbSubnetGroupName", orNone(actualSubnetGroup), r.DBSubnetGroupName)
	}

	if aws.ToBool(instance.PubliclyAccessible) {
		conflicts = append(conflicts, "instance is publicly accessible (this operator only manages private instances)")
	}

	cur := effectiveAttributes(instance)
	if cur.instanceClass != r.InstanceClass {
		differs("instanceClass", cur.instanceClass, r.InstanceClass)
	}
	wantMultiAZ := r.HighAvailability != nil && r.HighAvailability.Enabled
	if cur.multiAZ != wantMultiAZ {
		differs("multiAZ", fmt.Sprint(cur.multiAZ), fmt.Sprint(wantMultiAZ))
	}
	if r.AllocatedStorage > cur.allocatedStorage {
		differs("allocatedStorage", fmt.Sprintf("%d GiB", cur.allocatedStorage), fmt.Sprintf("%d GiB, which would grow it", r.AllocatedStorage))
	}
	if have, ok := parseVersion(cur.engineVersion); ok {
		if want, ok := parseVersion(r.EngineVersion); ok && compareVersions(have, want) < 0 {
			differs("engineVersion", cur.engineVersion, r.EngineVersion+" or later, which would upgrade it")
		}
	}
	return conflicts
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
