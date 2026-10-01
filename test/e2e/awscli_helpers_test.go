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
	"time"

	. "github.com/onsi/gomega"

	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// runAWSCLIPod applies a one-off, restricted-PodSecurity-compliant pod
// running the aws CLI against the in-cluster LocalStack with awsArgs,
// waits for it to succeed, and returns its logs. name must be unique
// within the namespace for the lifetime of the pod. Shared by every
// resource type's lifecycle spec, both to independently verify a resource
// exists outside the operator's own state, and (see lifecycle_sns_test.go)
// to seed state the reconcile loop will then pick up, where LocalStack's
// own API can't be trusted to report existence correctly.
func runAWSCLIPod(name string, awsArgs ...string) string {
	fullCommand := append([]string{
		"aws", "--endpoint-url=http://localstack." + localstackNamespace + ".svc.cluster.local:4566",
	}, awsArgs...)

	podYAML := `
apiVersion: v1
kind: Pod
metadata:
  name: ` + name + `
  namespace: ` + namespace + `
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: ` + name + `
      image: amazon/aws-cli
      env:
        - {name: AWS_ACCESS_KEY_ID, value: "test"}
        - {name: AWS_SECRET_ACCESS_KEY, value: "test"}
        - {name: AWS_DEFAULT_REGION, value: "us-east-1"}
        - {name: HOME, value: "/tmp"}
      command: ` + yamlFlowList(fullCommand) + `
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
`
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(podYAML)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create pod "+name)

	verifySucceeded := func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pod", name, "-n", namespace, "-o", "jsonpath={.status.phase}")
		phase, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(phase).To(Equal("Succeeded"))
	}
	EventuallyWithOffset(1, verifySucceeded, 2*time.Minute, 2*time.Second).Should(Succeed())

	cmd = exec.Command("kubectl", "logs", name, "-n", namespace)
	logs, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return logs
}

// yamlFlowList renders items as a YAML flow-sequence ("[\"a\", \"b\"]"),
// avoiding hand-rolled indentation for a block sequence embedded in a
// larger hand-built YAML string.
func yamlFlowList(items []string) string {
	quoted := make([]string, len(items))
	for i, it := range items {
		quoted[i] = fmt.Sprintf("%q", it)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
