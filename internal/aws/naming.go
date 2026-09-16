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

import "fmt"

// ResourceName derives a deterministic AWS resource name from an
// AppDependencies CR's namespace/name and a resource's spec-level key. Safe
// to recompute at any time — nothing about ownership tracking depends on
// persisted random state.
//
// S3 needs its own variant (in the s3 resource package, not here) since
// bucket names are unique across every AWS account globally, not just this
// one, and need an account-id-derived suffix to avoid colliding with an
// unrelated AWS customer.
func ResourceName(namespace, crName, resourceKey string) string {
	return fmt.Sprintf("%s-%s-%s", namespace, crName, resourceKey)
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

// ValidateNameLength checks a fully-derived resource name against a
// service's maximum length, returning a clear, actionable error before an
// AWS call would otherwise reject it with a much less helpful message.
// Combined length of namespace + CR name + resource key easily exceeds a
// tighter service limit (SQS's 80 characters, IAM's 64 once that's built)
// even with entirely reasonable, non-adversarial names — this isn't a
// theoretical edge case.
func ValidateNameLength(name string, maxLength int, service string) error {
	if len(name) > maxLength {
		return fmt.Errorf("computed %s name %q is %d characters, exceeding the %d-character limit — shorten the namespace, CR name, or resource name", service, name, len(name), maxLength)
	}
	return nil
}
