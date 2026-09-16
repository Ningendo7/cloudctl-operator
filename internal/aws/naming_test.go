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
	"strings"
	"testing"
)

func TestResourceName(t *testing.T) {
	got := ResourceName("default", "checkout-service", "orders")
	want := "default-checkout-service-orders"
	if got != want {
		t.Errorf("ResourceName() = %q, want %q", got, want)
	}
}

func TestTopicARN(t *testing.T) {
	got := TopicARN("us-east-1", "123456789012", "default-checkout-service-orders")
	want := "arn:aws:sns:us-east-1:123456789012:default-checkout-service-orders"
	if got != want {
		t.Errorf("TopicARN() = %q, want %q", got, want)
	}
}

func TestValidateNameLength(t *testing.T) {
	if err := ValidateNameLength("orders", 80, "SQS queue"); err != nil {
		t.Errorf("expected a well-within-limit name to pass, got error: %v", err)
	}

	exactly80 := strings.Repeat("a", 80)
	if err := ValidateNameLength(exactly80, 80, "SQS queue"); err != nil {
		t.Errorf("expected a name exactly at the limit to pass, got error: %v", err)
	}

	tooLong := strings.Repeat("a", 81)
	err := ValidateNameLength(tooLong, 80, "SQS queue")
	if err == nil {
		t.Fatal("expected an error for a name exceeding the limit")
	}
	if !strings.Contains(err.Error(), "81 characters") || !strings.Contains(err.Error(), "80-character limit") {
		t.Errorf("expected the error to explain the actual and allowed lengths, got: %v", err)
	}
}
