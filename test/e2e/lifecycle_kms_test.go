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

// kmsLifecycleSpec registers the KMS scenario into the shared Describe
// block defined in lifecycle_test.go. Unlike every other resource type,
// KMS keys aren't exposed via the connection ConfigMap at all (apps reach
// them through their IAM role's permissions, never by reading a key ARN
// out of config) and existence is checked via DescribeKey by alias (a
// primary KMS API, confirmed directly against this project's pinned
// LocalStack image), so - also unlike SNS/S3 - no pre-create workaround is
// needed here either.
//
// deletionPolicy is Retain, same as the design's own recommended default
// for KMS (no force field exists for it by design - deleting a key while
// anything still uses it makes that data permanently unrecoverable
// elsewhere), so the real key is left behind in this run's throwaway
// LocalStack container rather than exercised through deletion here.
func kmsLifecycleSpec() {
	It("adds a real KMS key to the same CR and reports Ready", func() {
		By("applying the updated sample CR with a kms section added")
		cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sns-dynamodb-s3-kms-sample.yaml", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the updated sample CR")

		By("waiting for the CR to report Ready again")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("reading the derived key ARN from the ownership ledger")
		cmd = exec.Command("kubectl", "get", "appdependencies", "e2e-orders", "-n", namespace,
			"-o", `jsonpath={.status.managedResources[?(@.type=="kms")].arn}`)
		keyArn, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(keyArn).To(ContainSubstring("arn:aws:kms:"))

		By("verifying the real key exists in LocalStack by its deterministic alias, independent of the operator's own state")
		alias := "alias/" + cloudctlaws.ResourceName(namespace, "e2e-orders", "kms", "app-key", 256-len("alias/"))
		logs := runAWSCLIPod("verify-key", "kms", "describe-key", "--key-id", alias)
		Expect(logs).To(ContainSubstring(keyArn), "expected the real key's ARN to match the one recorded in the ownership ledger")
	})
}
