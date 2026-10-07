//go:build e2e

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

package e2e

import (
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// subscriptionLifecycleSpec registers the SNS->SQS subscription scenario
// into the shared Describe block defined in lifecycle_test.go. Builds on
// the same e2e-orders CR's existing sqs.orders queue and sns.order-events
// topic from the earlier sqs/sns stages, rather than standing up a second
// CR - sharedWith/subscribesTo pointed at the CR's own namespace/name is a
// legitimate (if unusual) case the authorization model never special-cases,
// and it avoids this stage needing its own finalizer teardown bookkeeping
// in AfterAll.
//
// This is the one stage that proves the feature actually works end to
// end, not just that the API objects it creates look right: the unit and
// integration tiers already prove Subscribe and the queue policy grant
// are each called correctly in isolation, but only a real reconcile loop
// against a real SNS topic and a real SQS queue can prove a message
// published to the topic is actually delivered into the queue - exactly
// the class of bug (see lifecycle_watch_test.go's own history) that only
// shows up once a real controller, not envtest or a fake client, is
// driving reconciliation.
func subscriptionLifecycleSpec() {
	It("subscribes the real queue to the real topic, delivers a real message, and tears the subscription down again", func() {
		By("applying the updated sample CR with subscribesTo/sharedWith added")
		cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sns-subscription-sample.yaml", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the updated sample CR")

		By("waiting for the CR to report Ready again")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("reading the real queue URL from the connection ConfigMap")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.SQS_ORDERS_URL}")
		queueURL, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(queueURL).NotTo(BeEmpty())

		By("reading the real topic ARN from the connection ConfigMap")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.SNS_ORDER_EVENTS_ARN}")
		topicARN, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(topicARN).NotTo(BeEmpty())

		By("verifying a real SNS subscription exists on the topic")
		subLogs := runAWSCLIPod("verify-subscription", "sns", "list-subscriptions")
		Expect(subLogs).To(ContainSubstring("order-events"), "expected a real subscription on the order-events topic")
		Expect(subLogs).To(ContainSubstring("orders"), "expected the subscription's endpoint to point at the orders queue")

		By("verifying the real queue policy grants the topic send access")
		policyLogs := runAWSCLIPod("verify-queue-policy", "sqs", "get-queue-attributes",
			"--queue-url", queueURL, "--attribute-names", "Policy")
		Expect(policyLogs).To(ContainSubstring(topicARN), "expected the queue policy to scope its grant to the real topic ARN")

		By("publishing a real message to the topic")
		const testMessage = "cloudctl-e2e-subscription-test"
		runAWSCLIPod("publish-event", "sns", "publish", "--topic-arn", topicARN, "--message", testMessage)

		By("verifying the real message actually arrived in the queue")
		deliveryLogs := runAWSCLIPod("verify-delivery", "sqs", "receive-message",
			"--queue-url", queueURL, "--wait-time-seconds", "15", "--max-number-of-messages", "1")
		Expect(deliveryLogs).To(ContainSubstring(testMessage), "expected the published message to actually be delivered into the queue")

		By("reverting the sample CR to drop subscribesTo/sharedWith")
		cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sns-sample.yaml", "-n", namespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to revert the sample CR")

		By("waiting for the CR to report Ready again after the revert")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("verifying the real SNS subscription was torn down")
		removedSubLogs := runAWSCLIPod("verify-subscription-removed", "sns", "list-subscriptions-by-topic", "--topic-arn", topicARN)
		Expect(removedSubLogs).NotTo(ContainSubstring("orders"), "expected the subscription to be gone after subscribesTo was removed")

		By("verifying the real queue policy grant was revoked")
		revokedPolicyLogs := runAWSCLIPod("verify-queue-policy-removed", "sqs", "get-queue-attributes",
			"--queue-url", queueURL, "--attribute-names", "Policy")
		Expect(revokedPolicyLogs).NotTo(ContainSubstring(topicARN), "expected the queue policy grant for the topic to be revoked")
	})
}
