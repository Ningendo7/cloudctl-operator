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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// localstackNamespace is fixed by testdata/localstack.yaml, kept separate
// from the manager's own namespace (which enforces the restricted Pod
// Security Standard) since LocalStack's image isn't built to run under it.
// LocalStack itself is deployed once for the whole suite in BeforeSuite
// (e2e_suite_test.go), not per Describe block.
const localstackNamespace = "localstack-system"

// This runs the manager against a real, in-cluster LocalStack instead of
// the fakes every unit test uses or the mocked reconcile loop - the first
// E2E scenario that actually exercises reconciliation, not just whether the
// manager pod comes up. Deploys its own instance of the manager (same fixed
// namespace as the "Manager" Describe block, reused sequentially rather
// than concurrently) patched with AWS_ENDPOINT_URL (every Describe block
// needs this, not just this one - see awsEnvArgs' own doc comment) and a
// fake OIDC provider, since declaring an owned resource always triggers IAM
// role derivation, which hard-errors without one configured.
//
// Each resource type's own It block lives in its own lifecycle_<type>_test.go
// file, registered here as a function call rather than inline, so the suite
// stays easy to navigate as more resource types are added without paying
// the setup cost (install CRDs, deploy manager, patch OIDC, wait for
// rollout) more than once for the whole Describe.
var _ = Describe("AppDependencies reconciliation against LocalStack", Ordered, func() {
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
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
		cmd = exec.Command("kubectl", "set", "env", "deployment/"+deploymentName, "-n", namespace)
		cmd.Args = append(cmd.Args, awsEnvArgs()...)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to set AWS env vars on the controller-manager")

		By("configuring a fake OIDC provider so IAM role derivation can proceed")
		cmd = exec.Command("kubectl", "patch", "deployment", deploymentName, "-n", namespace,
			"--type=json", "-p", `[
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--oidc-provider-arn=arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/E2ETEST"},
				{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--oidc-provider-url=https://oidc.eks.us-east-1.amazonaws.com/id/E2ETEST"}
			]`)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to patch OIDC provider args onto the controller-manager")

		By("waiting for the patched controller-manager rollout to complete")
		cmd = exec.Command("kubectl", "rollout", "status", "deployment/"+deploymentName,
			"-n", namespace, "--timeout=2m")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "controller-manager rollout did not complete")
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		By("Fetching the CR's full status for debugging")
		cmd := exec.Command("kubectl", "get", "appdependencies", "e2e-orders", "-n", namespace, "-o", "yaml")
		if out, err := utils.Run(cmd); err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "e2e-orders status:\n%s", out)
		}

		By("Fetching controller manager logs for debugging")
		cmd = exec.Command("kubectl", "logs", "-l", "control-plane=controller-manager",
			"-n", namespace, "--all-containers", "--tail=300")
		if out, err := utils.Run(cmd); err == nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "controller-manager logs:\n%s", out)
		}
	})

	AfterAll(func() {
		// kubectl delete on a resource whose finalizer never clears would
		// otherwise block here indefinitely; cap it and force the
		// finalizer off rather than let one broken reconcile hang the
		// entire suite (make undeploy below deletes the whole namespace
		// next, which would then also block forever on the same stuck
		// object).
		// Deletes by name/kind rather than -f <file>, so this doesn't need
		// updating every time a new resource-type stage's sample file is
		// added - whatever the CR's current cumulative spec is, this still
		// finds and deletes the one object.
		By("deleting the sample CR so finalizer-driven cleanup actually runs")
		cmd := exec.Command("kubectl", "delete", "appdependencies", "e2e-orders",
			"-n", namespace, "--ignore-not-found", "--timeout=60s")
		_, delErr := utils.Run(cmd)
		if delErr != nil {
			By("CR deletion didn't complete in time - forcing its finalizer off")
			cmd = exec.Command("kubectl", "patch", "appdependencies", "e2e-orders", "-n", namespace,
				"--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
			_, _ = utils.Run(cmd)
		}

		By("deleting the verification pods")
		cmd = exec.Command("kubectl", "delete", "pod",
			"verify-queue", "seed-topic", "tag-topic", "verify-topic", "verify-table",
			"seed-bucket", "tag-bucket", "verify-bucket", "verify-key",
			"-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found", "--timeout=60s")
		_, _ = utils.Run(cmd)
	})

	sqsLifecycleSpec()
	watchLifecycleSpec()
	snsLifecycleSpec()
	dynamodbLifecycleSpec()
	s3LifecycleSpec()
	kmsLifecycleSpec()
})

// verifyReady polls the given CR's aggregate Ready condition - shared
// across every resource type's lifecycle spec since the check itself never
// varies, only which CR/timing triggers it.
func verifyReady(g Gomega) {
	// Every spec past the first applies a change to a CR that's already
	// Ready from the previous stage - Eventually's first check runs
	// immediately, which can read that stale, still-True condition before
	// this generation's own reconcile has even started. Checking
	// observedGeneration alongside status closes that race: a Ready
	// condition left over from an earlier generation never satisfies it.
	cmd := exec.Command("kubectl", "get", "appdependencies", "e2e-orders", "-n", namespace,
		"-o", `jsonpath={.metadata.generation} {.status.conditions[?(@.type=="Ready")].observedGeneration} {.status.conditions[?(@.type=="Ready")].status}`)
	output, err := utils.Run(cmd)
	g.Expect(err).NotTo(HaveOccurred())
	fields := strings.Fields(output)
	g.Expect(fields).To(HaveLen(3), "expected \"<generation> <observedGeneration> <status>\", got %q", output)
	g.Expect(fields[1]).To(Equal(fields[0]), "Ready condition is stale (observedGeneration %s != current generation %s) - this spec change hasn't been reconciled yet", fields[1], fields[0])
	g.Expect(fields[2]).To(Equal("True"), "expected the CR to report Ready")
}
