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

// dynamodbLifecycleSpec registers the DynamoDB scenario into the shared
// Describe block defined in lifecycle_test.go. Unlike SNS, DynamoDB's
// existence check uses DescribeTable's own ResourceNotFoundException
// (confirmed directly against this project's pinned LocalStack image),
// not a shared tagging-family API, so no pre-create workaround is needed
// here - CreateTable's own path runs for real.
func dynamodbLifecycleSpec() {
	It("adds a real DynamoDB table to the same CR and reports Ready", func() {
		By("applying the updated sample CR with a dynamodb section added")
		cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sns-dynamodb-sample.yaml", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the updated sample CR")

		By("waiting for the CR to report Ready again")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("verifying the connection ConfigMap was populated with the derived table name")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.DYNAMODB_SESSIONS_TABLE_NAME}")
		output, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(output).To(ContainSubstring("sessions"))

		By("verifying the real table exists in LocalStack, independent of the operator's own state")
		logs := runAWSCLIPod("verify-table", "dynamodb", "list-tables")
		Expect(logs).To(ContainSubstring("sessions"), "expected the real table to show up in LocalStack's own list-tables output")
	})
}
