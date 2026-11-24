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

package sqs

import (
	"strings"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// CheckLedgerEntry refuses a queue entry whose ARN is not the queue this CR
// would have, such as another CR's queue or one in another account.
func CheckLedgerEntry(id cloudctlaws.Identity, namespace, crName string, entry depsv1alpha1.ManagedResource) error {
	fifo := strings.HasSuffix(entry.ARN, ".fifo")
	want := id.RegionalARN("sqs", cloudctlaws.QueueName(namespace, crName, fifo, cloudctlaws.SplitDerivedKey(entry.Name)...))
	return cloudctlaws.CheckLedgerARN(entry.Type, entry.Name, entry.ARN, want)
}

// CheckSubscriptionEntry refuses a subscription entry that isn't on the topic
// its ledger name refers to. A subscription's ARN is its topic's ARN and an
// ID SNS chose, so only the topic part can be checked.
func CheckSubscriptionEntry(id cloudctlaws.Identity, _, _ string, entry depsv1alpha1.ManagedResource) error {
	_, ref, _ := strings.Cut(entry.Name, subscriptionLedgerSeparator)
	parts := strings.Split(ref, "/")
	if len(parts) != 3 {
		return &cloudctlaws.LedgerMismatchError{Type: entry.Type, Name: entry.Name, ARN: entry.ARN,
			Want: "a name of the form <queue>" + subscriptionLedgerSeparator + "<namespace>/<cr>/<topic>"}
	}
	var err error
	for _, fifo := range []bool{false, true} {
		topicARN := id.RegionalARN("sns", cloudctlaws.TopicName(parts[0], parts[1], fifo, parts[2]))
		if err = cloudctlaws.CheckLedgerARNPrefix(entry.Type, entry.Name, entry.ARN, topicARN+":"); err == nil {
			return nil
		}
	}
	return err
}
