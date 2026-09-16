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

// MergeTags merges desired tags into existing tags, read-merge-write style.
// Some AWS tagging calls are additive already, but at least one (S3's
// PutBucketTagging) replaces the entire tag set, so writing our ownership
// tag would silently wipe anything a human or another tool set. Applying
// this uniformly everywhere avoids having to track which services are safe
// per their own documented semantics. desired wins on key conflicts.
func MergeTags(existing, desired map[string]string) map[string]string {
	merged := make(map[string]string, len(existing)+len(desired))
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range desired {
		merged[k] = v
	}
	return merged
}
