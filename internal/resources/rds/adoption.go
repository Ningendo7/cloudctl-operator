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
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

const reasonAdoptionConflict = "AdoptionConflict"

// checkAdoptable refuses to adopt an existing, untagged instance whose
// settings differ from the spec. Adoption must not change an instance, and
// some differences can never be reconciled at all. It runs before anything
// is created or modified for the instance, so a refusal leaves no security
// group, key, tag or modify call behind. Instances that are already ours,
// or owned by someone else, are left to ensureInstance's ownership checks.
func checkAdoptable(
	ctx context.Context,
	awsClient rdsAPI,
	namespace, crName string,
	r depsv1alpha1.RDSInstanceSpec,
	ledger []depsv1alpha1.ManagedResource,
) error {
	if !r.Adopt || status.HasManagedResource(ledger, resourceType, r.Name) {
		return nil
	}
	instanceID := cloudctlaws.ResourceName(namespace, crName, resourceType, r.Name, 63)
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

	conflicts := adoptionConflicts(instance, r, ledger)
	if len(conflicts) == 0 {
		return nil
	}
	return &cloudctlaws.ReconcileError{
		Err: fmt.Errorf("instance %q exists but cannot be adopted as declared: %s - adoption never changes an instance, "+
			"so make the spec match what exists, adopt, and then change the spec", instanceID, strings.Join(conflicts, "; ")),
		Reason: reasonAdoptionConflict,
	}
}

// adoptionConflicts lists the differences between an existing instance and
// the spec that adoption must not paper over: settings that can't be changed
// afterwards (engine, subnet group, encryption), public accessibility, and settings the operator
// would otherwise change on its first reconcile (class, Multi-AZ, a higher
// engine version or storage size than the instance has).
func adoptionConflicts(instance types.DBInstance, r depsv1alpha1.RDSInstanceSpec, ledger []depsv1alpha1.ManagedResource) []string {
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

	wantEncrypted := r.Encryption != nil && (r.Encryption.Enabled || r.Encryption.KMSKeyRef != "")
	switch haveEncrypted := aws.ToBool(instance.StorageEncrypted); {
	case wantEncrypted && !haveEncrypted:
		conflicts = append(conflicts, "storage is not encrypted (spec: encryption requested, which cannot be enabled on an existing instance)")
	case !wantEncrypted && haveEncrypted:
		conflicts = append(conflicts, "storage is encrypted (spec: no encryption declared)")
	case wantEncrypted && r.Encryption.KMSKeyRef != "":
		// A dedicated key doesn't exist yet at adoption, so only a
		// kmsKeyRef has a key to compare against.
		if want, ok := kms.SharedKeyARN(ledger, r.Encryption.KMSKeyRef); ok && aws.ToString(instance.KmsKeyId) != want {
			differs("kms key", aws.ToString(instance.KmsKeyId), want)
		}
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
