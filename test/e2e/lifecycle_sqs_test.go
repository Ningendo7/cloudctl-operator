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

// sqsLifecycleSpec registers the SQS scenario into the shared Describe
// block defined in lifecycle_test.go.
func sqsLifecycleSpec() {
	It("creates a real SQS queue in LocalStack and reports Ready", func() {
		By("applying the sample AppDependencies CR")
		cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sample.yaml", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the sample CR")

		By("waiting for the CR to report Ready")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("verifying the connection ConfigMap was populated with the derived queue URL")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.SQS_ORDERS_URL}")
		output, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(output).To(ContainSubstring("orders"))

		By("verifying the real queue exists in LocalStack, independent of the operator's own state")
		logs := runAWSCLIPod("verify-queue", "sqs", "list-queues")
		Expect(logs).To(ContainSubstring("orders"), "expected the real queue to show up in LocalStack's own list-queues output")
	})
}
