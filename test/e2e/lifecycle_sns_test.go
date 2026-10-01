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

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// snsLifecycleSpec registers the SNS scenario into the shared Describe
// block defined in lifecycle_test.go. Builds on the SQS spec rather than
// standing alone - the same CR gaining a second resource type, same as a
// real team's manifest growing over time, not a fresh scenario from
// scratch.
//
// Unlike every other resource type's lifecycle spec, the topic is
// pre-created and pre-tagged directly via the AWS CLI before the CR's sns
// section is ever applied, instead of letting Ensure's own CreateTopic
// path run. This LocalStack version's ListTagsForResource returns a false
// empty-tags success for a topic ARN that doesn't exist yet, instead of
// the ResourceNotFoundException real AWS raises (confirmed directly
// against this project's pinned LocalStack image via a throwaway
// container - still present in every LocalStack release up to the last
// one offered without an auth token). Ensure can therefore never observe
// "doesn't exist yet" against LocalStack and always takes the
// adoption-required branch - production code is correct here, already
// proven against real AWS separately. Pre-creating sidesteps exactly the
// one call this environment can't be trusted to test (CreateTopic itself);
// everything downstream of it - attribute reconciliation against an
// already-existing topic, the connection ConfigMap, and the
// deletion/cleanup path - still runs for real.
func snsLifecycleSpec() {
	It("adds a real SNS topic to the same CR and reports Ready", func() {
		By("reading the existing CR's UID")
		cmd := exec.Command("kubectl", "get", "appdependencies", "e2e-orders", "-n", namespace,
			"-o", "jsonpath={.metadata.uid}")
		uid, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("pre-creating and pre-tagging the topic directly, working around a LocalStack limitation")
		topicName := cloudctlaws.ResourceName(namespace, "e2e-orders", "sns", "order-events", 256)
		topicArn := cloudctlaws.TopicARN("us-east-1", "000000000000", topicName)
		runAWSCLIPod("seed-topic", "sns", "create-topic", "--name", topicName)
		runAWSCLIPod("tag-topic", "sns", "tag-resource", "--resource-arn", topicArn,
			"--tags",
			"Key="+cloudctlaws.OwnerTagKey+",Value="+cloudctlaws.OwnerTagValue(namespace, "e2e-orders"),
			"Key="+cloudctlaws.OwnerUIDTagKey+",Value="+uid,
		)

		By("applying the updated sample CR with an sns section added")
		cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sns-sample.yaml", "-n", namespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the updated sample CR")

		By("waiting for the CR to report Ready again")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("verifying the connection ConfigMap was populated with the derived topic ARN")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.SNS_ORDER_EVENTS_ARN}")
		output, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(output).To(ContainSubstring("order-events"))

		By("verifying the real topic still exists in LocalStack, independent of the operator's own state")
		logs := runAWSCLIPod("verify-topic", "sns", "list-topics")
		Expect(logs).To(ContainSubstring("order-events"), "expected the real topic to show up in LocalStack's own list-topics output")
	})
}
