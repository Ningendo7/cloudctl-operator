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

	"github.com/aws/aws-sdk-go-v2/service/ec2"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// instanceIdentifier is the DB instance identifier for a key.
func instanceIdentifier(namespace, crName, key string) string {
	return cloudctlaws.ResourceNameFor(cloudctlaws.NameStyleRDS, namespace, crName, resourceType, key, 63)
}

// CheckLedgerEntry refuses an instance entry whose ARN is not the instance
// this CR would have.
func CheckLedgerEntry(id cloudctlaws.Identity, namespace, crName string, entry depsv1alpha1.ManagedResource) error {
	want := id.RegionalARN("rds", "db:"+instanceIdentifier(namespace, crName, entry.Name))
	return cloudctlaws.CheckLedgerARN(entry.Type, entry.Name, entry.ARN, want)
}

// CheckSecurityGroupEntry refuses a security group entry outside this account
// and region. Its ID is AWS's, so whose group it is is checked by its tags
// where it is used.
func CheckSecurityGroupEntry(id cloudctlaws.Identity, _, _ string, entry depsv1alpha1.ManagedResource) error {
	return cloudctlaws.CheckLedgerARNPrefix(entry.Type, entry.Name, entry.ARN, id.RegionalARN("ec2", "security-group/sg-"))
}

// CheckSnapshotEntry accepts any final-snapshot entry: its ID holds when the
// deletion started, which can't be read back, and the snapshot's tags are
// checked before it is retired.
func CheckSnapshotEntry(cloudctlaws.Identity, string, string, depsv1alpha1.ManagedResource) error {
	return nil
}

// SecurityGroupOwnedBy reads a security group's tags and reports whether it is
// this CR's. found is false when the group no longer exists.
func SecurityGroupOwnedBy(ctx context.Context, client cloudctlaws.EC2Client, groupARN, namespace, crName, crUID string) (owned, found bool, err error) {
	out, err := client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{SecurityGroupIDFromARN(groupARN)}})
	if isGroupNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, true, wrapAWSError(err, "reading a security group's tags to check whose it is")
	}
	if len(out.SecurityGroups) == 0 {
		return false, false, nil
	}
	return cloudctlaws.OwnedOrTransferable(ctx, ec2TagsToMap(out.SecurityGroups[0].Tags), namespace, crName, crUID), true, nil
}
