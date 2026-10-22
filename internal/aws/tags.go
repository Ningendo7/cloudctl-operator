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
	"context"
	"fmt"
	"maps"
	"time"
)

// MergeTags merges desired tags into existing tags, read-merge-write style.
// Some AWS tagging calls are additive already, but at least one (S3's
// PutBucketTagging) replaces the entire tag set, so writing our ownership
// tag would silently wipe anything a human or another tool set. Applying
// this uniformly everywhere avoids having to track which services are safe
// per their own documented semantics. desired wins on key conflicts.
func MergeTags(existing, desired map[string]string) map[string]string {
	merged := make(map[string]string, len(existing)+len(desired))
	maps.Copy(merged, existing)
	maps.Copy(merged, desired)
	return merged
}

// The tags left on a resource the operator lets go of, saying where it came
// from. They are not ownership: IsOwnedBy never reads them.
const (
	ReleasedFromTagKey = "cloudctl.io/released-from"
	ReleasedAtTagKey   = "cloudctl.io/released-at"
)

type releaseTagsOffKey struct{}

// WithReleaseTags turns the released-from tags on or off for ctx. They are on
// unless a context says otherwise.
func WithReleaseTags(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, releaseTagsOffKey{}, !enabled)
}

// ReleaseTags returns the tags to leave on a resource being let go: the CR it
// came from and the UTC date. It returns nil when they are turned off.
func ReleaseTags(ctx context.Context, namespace, crName string, now time.Time) map[string]string {
	if off, _ := ctx.Value(releaseTagsOffKey{}).(bool); off {
		return nil
	}
	return map[string]string{
		ReleasedFromTagKey: OwnerTagValue(namespace, crName),
		ReleasedAtTagKey:   now.UTC().Format(time.DateOnly),
	}
}

// ReleasedTagKeys are the keys ReleaseTags writes.
func ReleasedTagKeys() []string {
	return []string{ReleasedFromTagKey, ReleasedAtTagKey}
}

// HasReleaseTags reports whether tags carry either released tag.
func HasReleaseTags(tags map[string]string) bool {
	_, from := tags[ReleasedFromTagKey]
	_, at := tags[ReleasedAtTagKey]
	return from || at
}

// WithoutReleaseTags returns a copy of tags without the released tags, for the
// services that replace the whole tag set on every write.
func WithoutReleaseTags(tags map[string]string) map[string]string {
	out := maps.Clone(tags)
	for _, key := range ReleasedTagKeys() {
		delete(out, key)
	}
	return out
}

// ReleaseTagFailed records that the released tags could not be written or
// removed. The resource's ownership is not affected either way, so this never
// fails a pass.
func ReleaseTagFailed(recordEvent func(eventType, reason, message string), kind, name string, err error) {
	if recordEvent == nil {
		return
	}
	recordEvent("Warning", "ReleaseTagFailed", fmt.Sprintf("Could not update the released-from tags on %s %q: %v. Ownership was not affected", kind, name, err))
}
