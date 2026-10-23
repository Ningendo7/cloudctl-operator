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
	"encoding/json"
	"os/exec"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// cliVersion is stamped into the CLI built for this scenario.
const cliVersion = "v0.0.0-e2e"

type cliLedgerItem struct {
	Type     string `json:"type"`
	Name     string `json:"name"`
	OnDelete string `json:"onDelete"`
}

// cliLifecycleSpec runs the CLI, built the way a release builds it, against
// the CR the SQS scenario left Ready. It is registered right after that
// scenario in lifecycle_test.go.
func cliLifecycleSpec() {
	It("reports the Ready CR through kubectl appdeps", func() {
		By("building the CLI")
		_, err := utils.Run(exec.Command("make", "cli-build", "VERSION="+cliVersion))
		Expect(err).NotTo(HaveOccurred(), "Failed to build the CLI")

		By("running status, which exits 0 only when the CR is Ready")
		out, err := utils.Run(exec.Command("bin/kubectl-appdeps", "status", "e2e-orders", "-n", namespace))
		Expect(err).NotTo(HaveOccurred(), "status should exit 0 for a Ready CR")
		Expect(out).To(ContainSubstring("Namespace: " + namespace + "   CR: e2e-orders"))
		Expect(out).To(ContainSubstring("Ready: True"))
		Expect(out).To(MatchRegexp(`SQSReady\s+True\s+Reconciled`))

		By("listing the ledger as JSON")
		out, err = utils.Run(exec.Command("bin/kubectl-appdeps", "resources", "e2e-orders", "-n", namespace, "-o", "json"))
		Expect(err).NotTo(HaveOccurred())
		var doc struct {
			Kind  string          `json:"kind"`
			Items []cliLedgerItem `json:"items"`
		}
		Expect(json.Unmarshal([]byte(out), &doc)).To(Succeed(), "resources -o json should print JSON only: %s", out)
		Expect(doc.Kind).To(Equal("Resources"))
		var queue *cliLedgerItem
		for i := range doc.Items {
			if doc.Items[i].Type == "sqs" && doc.Items[i].Name == "orders" {
				queue = &doc.Items[i]
			}
		}
		Expect(queue).NotTo(BeNil(), "the queue should be in the ledger: %s", out)
		Expect(queue.OnDelete).To(Equal("DeleteEvenIfNotEmpty"), "the sample declares Delete with force")

		By("running retained, which lists nothing for a Delete-policy CR but still says where to look")
		out, err = utils.Run(exec.Command("bin/kubectl-appdeps", "retained", "-n", namespace))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("Key=cloudctl.io/released-from"))

		By("running it as a kubectl plugin, found on PATH")
		out, err = utils.Run(exec.Command("sh", "-c", `PATH="$PWD/bin:$PATH" kubectl appdeps version`))
		Expect(err).NotTo(HaveOccurred(), "kubectl should find kubectl-appdeps on PATH")
		Expect(out).To(ContainSubstring("Client: " + cliVersion))
		Expect(out).To(ContainSubstring("Server API: deps.cloudctl.io/v1alpha1"))
	})
}
