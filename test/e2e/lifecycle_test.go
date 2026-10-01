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
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// localstackNamespace is fixed by testdata/localstack.yaml, kept separate
// from the manager's own namespace (which enforces the restricted Pod
// Security Standard) since LocalStack's image isn't built to run under it.
const localstackNamespace = "localstack-system"

// This runs the manager against a real, in-cluster LocalStack instead of
// the fakes every unit test uses or the mocked reconcile loop - the first
// E2E scenario that actually exercises reconciliation, not just whether the
// manager pod comes up. Deploys its own instance of the manager (same fixed
// namespace as the "Manager" Describe block, reused sequentially rather
// than concurrently) patched with AWS_ENDPOINT_URL and a fake OIDC provider,
// since declaring an owned resource always triggers IAM role derivation,
// which hard-errors without one configured.
var _ = Describe("AppDependencies reconciliation against LocalStack", Ordered, func() {
	BeforeAll(func() {
		By("deploying LocalStack")
		cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/localstack.yaml")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy LocalStack")

		By("waiting for LocalStack to become available")
		cmd = exec.Command("kubectl", "rollout", "status", "deployment/localstack",
			"-n", localstackNamespace, "--timeout=2m")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "LocalStack did not become ready")

		By("creating manager namespace")
		cmd = exec.Command("kubectl", "create", "ns", namespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("pointing the manager at LocalStack instead of real AWS")
		cmd = exec.Command("kubectl", "set", "env", "deployment/controller-manager", "-n", namespace,
			"AWS_ENDPOINT_URL=http://localstack."+localstackNamespace+".svc.cluster.local:4566",
			"AWS_ACCESS_KEY_ID=test",
			"AWS_SECRET_ACCESS_KEY=test",
			"AWS_REGION=us-east-1",
		)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to set AWS env vars on the controller-manager")

		By("configuring a fake OIDC provider so IAM role derivation can proceed")
		cmd = exec.Command("kubectl", "patch", "deployment", "controller-manager", "-n", namespace,
			"--type=json", "-p", `[
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--oidc-provider-arn=arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/E2ETEST"},
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--oidc-provider-url=https://oidc.eks.us-east-1.amazonaws.com/id/E2ETEST"}
			]`)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to patch OIDC provider args onto the controller-manager")

		By("waiting for the patched controller-manager rollout to complete")
		cmd = exec.Command("kubectl", "rollout", "status", "deployment/controller-manager",
			"-n", namespace, "--timeout=2m")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "controller-manager rollout did not complete")
	})

	AfterAll(func() {
		By("deleting the sample CR so finalizer-driven cleanup actually runs")
		cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/sqs-sample.yaml",
			"-n", namespace, "--ignore-not-found", "--timeout=90s")
		_, _ = utils.Run(cmd)

		By("deleting the queue-verification pod")
		cmd = exec.Command("kubectl", "delete", "pod", "verify-queue", "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("deleting LocalStack")
		cmd = exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/localstack.yaml", "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	It("creates a real SQS queue in LocalStack and reports Ready", func() {
		By("applying the sample AppDependencies CR")
		cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sample.yaml", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the sample CR")

		By("waiting for the CR to report Ready")
		verifyReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "appdependencies", "e2e-orders", "-n", namespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(Equal("True"), "expected the CR to report Ready")
		}
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("verifying the connection ConfigMap was populated with the derived queue URL")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.SQS_ORDERS_URL}")
		output, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(output).To(ContainSubstring("orders"))

		By("verifying the real queue exists in LocalStack, independent of the operator's own state")
		cmd = exec.Command("kubectl", "run", "verify-queue", "--restart=Never",
			"-n", namespace,
			"--image=amazon/aws-cli",
			"--env=AWS_ACCESS_KEY_ID=test",
			"--env=AWS_SECRET_ACCESS_KEY=test",
			"--env=AWS_DEFAULT_REGION=us-east-1",
			"--command", "--",
			"aws", "--endpoint-url=http://localstack."+localstackNamespace+".svc.cluster.local:4566",
			"sqs", "list-queues",
		)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create the queue-verification pod")

		verifyQueueVerificationSucceeded := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pod", "verify-queue", "-n", namespace,
				"-o", "jsonpath={.status.phase}")
			phase, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Succeeded"))
		}
		Eventually(verifyQueueVerificationSucceeded, 2*time.Minute, 2*time.Second).Should(Succeed())

		cmd = exec.Command("kubectl", "logs", "verify-queue", "-n", namespace)
		logs, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(logs).To(ContainSubstring("orders"), "expected the real queue to show up in LocalStack's own list-queues output")
	})
})
