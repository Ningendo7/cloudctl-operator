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

import "testing"

func TestIsOwnedBy(t *testing.T) {
	tags := map[string]string{
		OwnerTagKey:    OwnerTagValue("default", "checkout-service"),
		OwnerUIDTagKey: "abc-123",
	}

	if !IsOwnedBy(tags, "default", "checkout-service", "abc-123") {
		t.Error("expected matching namespace/name/uid to be owned")
	}
	if IsOwnedBy(tags, "default", "checkout-service", "different-uid") {
		t.Error("expected mismatched uid to not be owned (stale recreated CR)")
	}
	if IsOwnedBy(tags, "other-namespace", "checkout-service", "abc-123") {
		t.Error("expected mismatched namespace to not be owned")
	}
	if IsOwnedBy(nil, "default", "checkout-service", "abc-123") {
		t.Error("expected untagged resource to not be owned")
	}
}
