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
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
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

// NameStyle says what a service accepts in a name. Kubernetes allows a CR
// to be called orders.v2, and the derived name starts with it, so a service
// that rejects the dot would reject the name on every pass.
type NameStyle int

const (
	// NameStyleAny leaves the readable prefix as it is, for services that
	// take any of what a Kubernetes name can contain (DynamoDB, IAM,
	// CloudWatch alarms, EC2 security groups).
	NameStyleAny NameStyle = iota
	// NameStyleWordChars is for SQS queues, SNS topics and KMS aliases:
	// letters, digits, '-' and '_'.
	NameStyleWordChars
	// NameStyleRDS is for RDS identifiers and snapshot IDs: letters, digits
	// and single hyphens, starting with a letter and not ending in a hyphen.
	NameStyleRDS
	// NameStyleS3 is for S3 buckets: a '.' is fine, but not next to another
	// '.' or a '-', and not before the hash.
	NameStyleS3
)

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
	return ResourceNameFor(NameStyleAny, namespace, crName, resourceType, key, maxLen)
}

// ResourceNameFor is ResourceName for a service with naming rules of its
// own. Only the prefix is ever changed, and only where the service would
// have rejected it; the hash is over the identity as typed. A name the
// service accepts keeps exactly the form ResourceName gives it, since a
// different one would orphan the resource under the old name.
func ResourceNameFor(style NameStyle, namespace, crName, resourceType, key string, maxLen int) string {
	hash := identityHash(namespace, crName, resourceType, key)
	return buildName(style, fmt.Sprintf("%s-%s-%s", namespace, crName, key), hash, maxLen)
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
	return DerivedResourceNameFor(NameStyleAny, namespace, crName, resourceType, maxLen, keyParts...)
}

// DerivedResourceNameFor is DerivedResourceName for a service with naming
// rules of its own, see ResourceNameFor.
func DerivedResourceNameFor(style NameStyle, namespace, crName, resourceType string, maxLen int, keyParts ...string) string {
	hash := identityHash(append([]string{namespace, crName, resourceType}, keyParts...)...)
	prefix := strings.Join(append([]string{namespace, crName}, keyParts...), "-")
	return buildName(style, prefix, hash, maxLen)
}

// QueueOrTopicName names an SQS queue or SNS topic. A FIFO one must end in
// ".fifo", so the budget leaves room for it. keyParts is the resource key,
// or the key and role of a derived queue such as a DLQ. The alarms that
// watch a queue or topic by name use this too, so the two can't disagree.
func QueueOrTopicName(namespace, crName, resourceType string, fifo bool, maxLen int, keyParts ...string) string {
	const fifoSuffix = ".fifo"
	if fifo {
		maxLen -= len(fifoSuffix)
	}
	name := DerivedResourceNameFor(NameStyleWordChars, namespace, crName, resourceType, maxLen, keyParts...)
	if fifo {
		name += fifoSuffix
	}
	return name
}

// CheckFIFOUnchanged refuses an entry whose queue or topic, named by arn, is
// not the type the spec now asks for. fifo can't be edited on an entry, but
// removing it and adding it back passes that rule while its old entry is
// still in the ledger, and the old queue would then be reconciled as if it
// were the new one. kind is "queue" or "topic".
func CheckFIFOUnchanged(kind, arn string, fifo bool) error {
	isFIFO := strings.HasSuffix(arn, ".fifo")
	if isFIFO == fifo {
		return nil
	}
	have, want := "standard", "FIFO"
	if isFIFO {
		have, want = "FIFO", "standard"
	}
	return &ReconcileError{
		Err: fmt.Errorf("%s %q is a %s %s but the spec now asks for a %s one; the type can't change on an existing %s, so set fifo back, or remove the entry, wait for it to be deleted and add it again",
			kind, arn[strings.LastIndex(arn, ":")+1:], have, kind, want, kind),
		Reason: reasons.FIFOChanged,
	}
}

// buildName cleans the prefix for the service, fits it to maxLen and puts
// the hash after it. With NameStyleAny it only truncates.
func buildName(style NameStyle, prefix, hash string, maxLen int) string {
	prefix = style.clean(prefix)
	budget := max(maxLen-len(hash)-1, 0)
	if style == NameStyleRDS && (prefix == "" || !isLetter(prefix[0])) {
		prefix = "db-" + prefix
	}
	if len(prefix) > budget {
		prefix = prefix[:budget]
	}
	prefix = style.trimEnd(prefix)
	if prefix == "" && style == NameStyleRDS {
		prefix = "db"
	}
	return prefix + "-" + hash
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// clean rewrites what the service rejects as '-'.
func (s NameStyle) clean(prefix string) string {
	switch s {
	case NameStyleWordChars:
		return mapBytes(prefix, func(c byte) bool { return isLetter(c) || isDigit(c) || c == '-' || c == '_' })
	case NameStyleRDS:
		prefix = mapBytes(prefix, func(c byte) bool { return isLetter(c) || isDigit(c) || c == '-' })
		for strings.Contains(prefix, "--") {
			prefix = strings.ReplaceAll(prefix, "--", "-")
		}
		return prefix
	case NameStyleS3:
		prefix = mapBytes(prefix, func(c byte) bool { return c >= 'a' && c <= 'z' || isDigit(c) || c == '-' || c == '.' })
		for _, adjacent := range []string{"..", ".-", "-."} {
			for strings.Contains(prefix, adjacent) {
				prefix = strings.ReplaceAll(prefix, adjacent, "-")
			}
		}
		return prefix
	default:
		return prefix
	}
}

// trimEnd drops what cannot sit right before the "-" and hash that follow.
func (s NameStyle) trimEnd(prefix string) string {
	switch s {
	case NameStyleRDS:
		return strings.TrimRight(prefix, "-")
	case NameStyleS3:
		return strings.TrimRight(prefix, ".")
	default:
		return prefix
	}
}

func mapBytes(s string, keep func(byte) bool) string {
	out := []byte(s)
	for i, c := range out {
		if !keep(c) {
			out[i] = '-'
		}
	}
	return string(out)
}

// DedicatedKeyRole identifies the "dedicated encryption key" derivation
// role, shared between DedicatedKeyLedgerName below and the kms package's
// own DerivedResourceName call for the same key's real alias — both must
// use the identical literal, since kms.EnsureDedicatedKey relies on the
// two staying in lockstep to find the same key by ledger name that it
// names in AWS.
const DedicatedKeyRole = "key"

// DedicatedKeyLedgerName derives the ledger entry name for a dedicated KMS
// key belonging to a resource in another section, keyed by that section's
// own resource type as well as its resource name — an SQS queue and an S3
// bucket that happen to share a name (legal, since they're declared in
// different sections) must never derive the same dedicated key.
func DedicatedKeyLedgerName(ownerType, resourceName string) string {
	return ownerType + derivedKeySeparator + resourceName + derivedKeySeparator + DedicatedKeyRole
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
