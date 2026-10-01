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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// identityHashLen is how many hex characters of the identity hash appear
// in every derived AWS name.
const identityHashLen = 12

// identityHash fingerprints an ordered tuple of fields, joined with a NUL
// byte before hashing — a byte no Kubernetes namespace/name or CRD
// resourceKey pattern can legally contain, so two different tuples can
// never produce the same hash input.
func identityHash(fields ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(fields, "\x00")))
	return hex.EncodeToString(sum[:])[:identityHashLen]
}

// derivedKeySeparator marks a resourceKey as one this operator computed
// itself rather than one a user typed — '#' appears in none of this CRD's
// resourceKey patterns. Only used for the ledger key string, never sent to
// AWS as-is: '#' isn't legal in any of SQS/SNS/S3/DynamoDB/KMS's real
// naming rules, so the AWS-facing name is built separately by
// DerivedResourceName instead.
const derivedKeySeparator = "#"

// DerivedKey builds the resourceKey for a resource this operator derives
// from another one it owns (e.g. an SQS queue's DLQ), named by role so it
// can never collide with a user-typed key.
func DerivedKey(resourceKey, role string) string {
	return resourceKey + derivedKeySeparator + role
}

// ResourceName derives a deterministic AWS resource name from an
// AppDependencies CR's namespace/name, a resource type (sqs, sns,
// dynamodb, s3, kms, iam), and a key within that type (see DerivedKey for
// a derived one). Safe to recompute at any time.
//
// The result is a truncated, human-readable prefix followed by a
// 12-hex-character hash of the full (namespace, crName, resourceType,
// key) tuple — hashing the whole tuple, rather than joining the parts with
// '-', keeps two different identities from ever landing on the same name
// even when '-' appears inside one of the fields. maxLen is the target
// service's own name limit; only the prefix is ever truncated, never the
// hash.
func ResourceName(namespace, crName, resourceType, key string, maxLen int) string {
	hash := identityHash(namespace, crName, resourceType, key)
	return truncateAndAppendHash(fmt.Sprintf("%s-%s-%s", namespace, crName, key), hash, maxLen)
}

// DerivedResourceName is ResourceName's counterpart for a resource this
// operator derives from another one it owns (an SQS queue's DLQ, a
// dedicated KMS key for some other resource's encryption). Each of
// keyParts is hashed as its own tuple element via identityHash rather than
// pre-joined into one string first — a user-declared resource named e.g.
// "orders-dlq" hashes over the single element "orders-dlq", never the two
// elements "orders" and "dlq", so the two can never collide on the same
// AWS name even though the visible, human-readable prefix looks the same
// either way (DerivedKey's ledger key uses '#' for exactly this reason,
// but that character isn't legal in the real AWS name built here).
func DerivedResourceName(namespace, crName, resourceType string, maxLen int, keyParts ...string) string {
	hash := identityHash(append([]string{namespace, crName, resourceType}, keyParts...)...)
	prefix := strings.Join(append([]string{namespace, crName}, keyParts...), "-")
	return truncateAndAppendHash(prefix, hash, maxLen)
}

func truncateAndAppendHash(prefix, hash string, maxLen int) string {
	budget := max(maxLen-len(hash)-1, 0)
	if len(prefix) > budget {
		prefix = prefix[:budget]
	}
	return prefix + "-" + hash
}

// DedicatedKeyLedgerName derives the ledger entry name for a dedicated KMS
// key belonging to a resource in another section, keyed by that section's
// own resource type as well as its resource name — an SQS queue and an S3
// bucket that happen to share a name (legal, since they're declared in
// different sections) must never derive the same dedicated key.
func DedicatedKeyLedgerName(ownerType, resourceName string) string {
	return ownerType + derivedKeySeparator + resourceName + derivedKeySeparator + "key"
}

// TopicARN constructs the deterministic ARN for an SNS topic. SNS has no
// "get topic by name" API — CreateTopic is the only name-to-ARN resolution
// and is idempotent in a way that hides whether a topic was just created or
// already existed — so unlike SQS/DynamoDB, the sns package needs to
// construct the expected ARN itself and check for it directly via
// GetTopicAttributes, mirroring SQS's GetQueueUrl-first flow.
func TopicARN(region, accountID, topicName string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, topicName)
}
