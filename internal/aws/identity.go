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
	"fmt"
	"strings"
)

// Identity is where this operator works: the partition, region and account
// every ARN it builds is in.
type Identity struct {
	Partition string
	Region    string
	AccountID string
}

// NewIdentity returns the identity for an account and region, taking the
// partition from the region.
func NewIdentity(region, accountID string) Identity {
	return Identity{Partition: PartitionOf(region), Region: region, AccountID: accountID}
}

// Known reports whether the identity is filled in. Without it no ARN can be
// checked.
func (id Identity) Known() bool { return id.Region != "" && id.AccountID != "" }

// PartitionOf returns the AWS partition a region belongs to.
func PartitionOf(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	default:
		return "aws"
	}
}

// RegionalARN is the ARN of resource in service, in this region and account.
func (id Identity) RegionalARN(service, resource string) string {
	return fmt.Sprintf("arn:%s:%s:%s:%s:%s", id.Partition, service, id.Region, id.AccountID, resource)
}

// GlobalARN is RegionalARN for a service without regions, such as IAM.
func (id Identity) GlobalARN(service, resource string) string {
	return fmt.Sprintf("arn:%s:%s::%s:%s", id.Partition, service, id.AccountID, resource)
}

// S3BucketARN is the ARN of a bucket, which names neither region nor account.
func (id Identity) S3BucketARN(bucket string) string {
	return fmt.Sprintf("arn:%s:s3:::%s", id.Partition, bucket)
}

// LedgerMismatchError is a ledger entry whose ARN is not one this operator
// would have built for it. Status can be written by anyone with that RBAC
// right, so an entry is never acted on before it passes this check.
type LedgerMismatchError struct {
	Type, Name, ARN, Want string
}

func (e *LedgerMismatchError) Error() string {
	return fmt.Sprintf("ledger entry %s %q has ARN %s, but this CR's would be %s", e.Type, e.Name, e.ARN, e.Want)
}

// CheckLedgerARN returns a LedgerMismatchError unless arn is want.
func CheckLedgerARN(entryType, name, arn, want string) error {
	if arn == want {
		return nil
	}
	return &LedgerMismatchError{Type: entryType, Name: name, ARN: arn, Want: want}
}

// CheckLedgerARNPrefix is CheckLedgerARN for an ID AWS chose, such as a KMS
// key's, of which only the part before it can be checked.
func CheckLedgerARNPrefix(entryType, name, arn, prefix string) error {
	if strings.HasPrefix(arn, prefix) && len(arn) > len(prefix) {
		return nil
	}
	return &LedgerMismatchError{Type: entryType, Name: name, ARN: arn, Want: prefix + "..."}
}

// SplitDerivedKey undoes DerivedKey: a user's key gives one part, a derived
// one its key and role.
func SplitDerivedKey(key string) []string {
	if base, role, ok := strings.Cut(key, derivedKeySeparator); ok {
		return []string{base, role}
	}
	return []string{key}
}
