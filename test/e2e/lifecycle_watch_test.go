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

	"github.com/Ningendo7/cloudctl-operator/internal/resources/serviceaccount"
	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// watchLifecycleSpec registers specs proving the generated ConfigMap's
// Owns() watch (SetupWithManager) self-heals against a real, in-cluster
// manager - not just envtest's fake control plane. Registered after
// sqsLifecycleSpec so e2e-orders-connection already exists with real data.
//
// Also includes the equivalent ServiceAccount specs, deliberately: a
// matching Owns(&corev1.ServiceAccount{}) watch was investigated at length
// against envtest (see internal/controller/consumers_watch_test.go history)
// and never shipped - Update/Delete events on it silently never reached
// Reconcile(), for a reason never root-caused, despite the identical
// mechanism working correctly for ConfigMap. These specs exist to answer
// one specific question: does the same gap reproduce against a real
// cluster (real etcd/apiserver/kubelet, not envtest's fake control plane),
// or was it an envtest-specific artifact? That answer determines whether
// Owns(&corev1.ServiceAccount{}) belongs back in SetupWithManager for
// real. See appdependencies_controller.go for whether that watch is
// currently present - these specs are only meaningful while it is.
func watchLifecycleSpec() {
	It("recreates the connection ConfigMap after it's deleted out-of-band", func() {
		By("deleting the connection ConfigMap directly")
		cmd := exec.Command("kubectl", "delete", "configmap", "e2e-orders-connection", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to delete the connection ConfigMap")

		By("waiting for the manager's watch to recreate it, with no manual reconcile trigger")
		Eventually(func() error {
			cmd := exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace)
			_, err := utils.Run(cmd)
			return err
		}, 30*time.Second, 2*time.Second).Should(Succeed())
	})

	It("repairs the connection ConfigMap's data after it's corrupted in place", func() {
		By("corrupting the queue URL value in place, not deleting the object")
		cmd := exec.Command("kubectl", "patch", "configmap", "e2e-orders-connection", "-n", namespace,
			"--type=merge", "-p", `{"data":{"SQS_ORDERS_URL":"corrupted"}}`)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to corrupt the connection ConfigMap")

		By("waiting for the manager's watch to repair the value")
		Eventually(func() (string, error) {
			cmd := exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
				"-o", "jsonpath={.data.SQS_ORDERS_URL}")
			return utils.Run(cmd)
		}, 30*time.Second, 2*time.Second).Should(ContainSubstring("orders"))
	})

	It("recreates the ServiceAccount after it's deleted out-of-band", func() {
		By("confirming the ServiceAccount already carries the IRSA annotation")
		// Eventually, not a single-shot check: the preceding spec's own
		// Owns()-triggered reconciles can still be settling (ConfigMap and
		// ServiceAccount re-applies on the same CR can cascade a few
		// rounds), so a one-shot read can catch this object mid-rewrite.
		Eventually(func() (string, error) {
			cmd := exec.Command("kubectl", "get", "serviceaccount", "e2e-orders", "-n", namespace,
				"-o", "jsonpath={.metadata.annotations"+jsonPathEscape(serviceaccount.RoleARNAnnotation)+"}")
			return utils.Run(cmd)
		}, 30*time.Second, 2*time.Second).ShouldNot(BeEmpty())

		By("deleting the ServiceAccount directly")
		cmd := exec.Command("kubectl", "delete", "serviceaccount", "e2e-orders", "-n", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to delete the ServiceAccount")

		By("waiting for the manager's watch to recreate it, with no manual reconcile trigger")
		Eventually(func() error {
			cmd := exec.Command("kubectl", "get", "serviceaccount", "e2e-orders", "-n", namespace)
			_, err := utils.Run(cmd)
			return err
		}, 30*time.Second, 2*time.Second).Should(Succeed())
	})

	It("restores the ServiceAccount's IRSA annotation after it's stripped out-of-band", func() {
		By("stripping the IRSA role-arn annotation in place, not deleting the object")
		cmd := exec.Command("kubectl", "annotate", "serviceaccount", "e2e-orders", "-n", namespace,
			serviceaccount.RoleARNAnnotation+"-")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to strip the IRSA annotation")

		By("waiting for the manager's watch to restore the annotation")
		Eventually(func() (string, error) {
			cmd := exec.Command("kubectl", "get", "serviceaccount", "e2e-orders", "-n", namespace,
				"-o", "jsonpath={.metadata.annotations"+jsonPathEscape(serviceaccount.RoleARNAnnotation)+"}")
			return utils.Run(cmd)
		}, 30*time.Second, 2*time.Second).ShouldNot(BeEmpty())
	})
}

// jsonPathEscape wraps an annotation/label key containing dots in the
// quoting kubectl's jsonpath parser needs to treat it as one literal key
// rather than a nested-field path - e.g. eks.amazonaws.com/role-arn would
// otherwise be read as field "eks" of field "amazonaws" of field "com/role-arn".
func jsonPathEscape(key string) string {
	return `['` + key + `']`
}
