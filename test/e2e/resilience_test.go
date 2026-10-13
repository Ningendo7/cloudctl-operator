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
	"fmt"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/controller/predicates"
	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

const (
	finalizer          = "deps.cloudctl.io/finalizer"
	localstackDeployNS = localstackNamespace
)

var awsPodSeq atomic.Int64

// nextAWSPod returns a unique name for a one-off aws-cli verification pod.
func nextAWSPod(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, awsPodSeq.Add(1))
}

func queueNameFor(crName string) string {
	return cloudctlaws.ResourceName(resilienceNamespace, crName, "sqs", "orders", 80)
}

func mustKubectl(args ...string) string {
	GinkgoHelper()
	out, err := utils.Run(exec.Command("kubectl", args...))
	Expect(err).NotTo(HaveOccurred())
	return out
}

func tryKubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

func waitNoManagerPods() {
	GinkgoHelper()
	Eventually(func() string {
		out, _ := tryKubectl("get", "pods", "-n", namespace, "-l", "control-plane=controller-manager", "-o", "name")
		return strings.TrimSpace(out)
	}, 2*time.Minute, 2*time.Second).Should(BeEmpty(), "manager pods did not terminate")
}

// restoreManager returns the manager to a healthy state pointed at
// LocalStack, with its normal two replicas. Safe to call when it already is.
func restoreManager() {
	GinkgoHelper()
	mustKubectl("scale", "deployment/"+deploymentName, "-n", namespace, "--replicas=0")
	waitNoManagerPods()
	args := append([]string{"set", "env", "deployment/" + deploymentName, "-n", namespace}, awsEnvArgs()...)
	mustKubectl(args...)
	mustKubectl("scale", "deployment/"+deploymentName, "-n", namespace, "--replicas=2")
	mustKubectl("rollout", "status", "deployment/"+deploymentName, "-n", namespace, "--timeout=4m")
}

func rdsCRManifest(name, subnetGroup string, storage int) string {
	return fmt.Sprintf(`apiVersion: deps.cloudctl.io/v1alpha1
kind: AppDependencies
metadata:
  name: %s
  namespace: %s
spec:
  rds:
    resources:
      - name: orders-db
        dbSubnetGroupName: %s
        engine: postgres
        engineVersion: "16.3"
        instanceClass: db.t4g.micro
        allocatedStorage: %d
`, name, resilienceNamespace, subnetGroup, storage)
}

// The checks here are the ones the earlier lifecycle stages can't make: the
// operator's real RBAC, behavior that needs a real garbage collector, a
// manager that is restarted, failed over, starved of AWS, and started with
// no AWS at all. None of it depends on AWS being faithful, which is why it
// is worth spending the e2e budget here rather than on RDS or KMS.
var _ = Describe("Operator resilience against LocalStack", Ordered, ContinueOnFailure, func() {
	BeforeAll(func() {
		By("waiting for any earlier suite to finish removing the manager namespace")
		Eventually(func() error {
			_, err := tryKubectl("get", "ns", namespace)
			return err
		}, 3*time.Minute, 3*time.Second).Should(HaveOccurred())

		By("deploying the manager against LocalStack")
		mustKubectl("create", "ns", namespace)
		mustKubectl("label", "--overwrite", "ns", namespace, "pod-security.kubernetes.io/enforce=restricted")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred())
		mustKubectl(append([]string{"set", "env", "deployment/" + deploymentName, "-n", namespace}, awsEnvArgs()...)...)
		mustKubectl("patch", "deployment", deploymentName, "-n", namespace, "--type=json", "-p", `[
			{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--oidc-provider-arn=arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/E2ETEST"},
			{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--oidc-provider-url=https://oidc.eks.us-east-1.amazonaws.com/id/E2ETEST"}
		]`)
		mustKubectl("rollout", "status", "deployment/"+deploymentName, "-n", namespace, "--timeout=4m")

		By("creating the namespace the specs' CRs live in, and one long-lived CR")
		mustKubectl("create", "ns", resilienceNamespace)
		_, err = kubectlApplyStdin(queueCRManifest(resilienceNamespace, "persist"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) { crReady(g, resilienceNamespace, "persist") }, 3*time.Minute, 2*time.Second).Should(Succeed())
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "appdependencies", "-n", resilienceNamespace, "-o", "yaml"},
			{"get", "pods", "-n", namespace, "-o", "wide"},
			{"logs", "-n", namespace, "-l", "control-plane=controller-manager", "--all-containers", "--tail=200", "--prefix"},
			{"get", "events", "-n", resilienceNamespace, "--sort-by=.lastTimestamp"},
			{"get", "pods", "-n", localstackDeployNS, "-o", "wide"},
		} {
			if out, err := tryKubectl(args...); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n", strings.Join(args, " "), out)
			}
		}
	})

	AfterAll(func() {
		By("making sure LocalStack and the manager are back before tearing down")
		_, _ = tryKubectl("scale", "deployment/localstack", "-n", localstackDeployNS, "--replicas=1")
		_, _ = tryKubectl("rollout", "status", "deployment/localstack", "-n", localstackDeployNS, "--timeout=3m")
		_, _ = tryKubectl("scale", "deployment/"+deploymentName, "-n", namespace, "--replicas=2")

		By("deleting the CRs so finalizers run, forcing any that don't finish")
		_, _ = tryKubectl("delete", "appdependencies", "--all", "-n", resilienceNamespace, "--wait=true", "--timeout=90s")
		if out, err := tryKubectl("get", "appdependencies", "-n", resilienceNamespace, "-o", "name"); err == nil {
			for _, name := range strings.Fields(out) {
				_, _ = tryKubectl("patch", name, "-n", resilienceNamespace, "--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
			}
		}
		_, _ = tryKubectl("delete", "rdssubnetgroupgrant", "e2e-grant", "--ignore-not-found")
		_, _ = tryKubectl("delete", "ns", resilienceNamespace, "--ignore-not-found", "--timeout=90s")

		_, _ = utils.Run(exec.Command("make", "undeploy"))
		_, _ = utils.Run(exec.Command("make", "uninstall"))
		_, _ = tryKubectl("delete", "ns", namespace, "--ignore-not-found", "--timeout=90s")
	})

	It("runs with the narrow permissions it was given, and no more", func() {
		asSA := fmt.Sprintf("--as=system:serviceaccount:%s:%s", namespace, serviceAccountName)
		can := func(verb, resource string, extra ...string) bool {
			args := append([]string{"auth", "can-i", verb, resource, asSA, "-n", resilienceNamespace}, extra...)
			out, _ := tryKubectl(args...)
			return lastLine(out) == "yes"
		}

		type check struct {
			verb, resource string
			extra          []string
		}
		for _, c := range []check{
			{"create", "configmaps", nil},
			{"create", "secrets", nil},
			{"create", "serviceaccounts", nil},
			{"patch", "serviceaccounts", nil},
			{"patch", "appdependencies.deps.cloudctl.io", []string{"--subresource=status"}},
			{"update", "appdependencies.deps.cloudctl.io", []string{"--subresource=finalizers"}},
			{"watch", "rdssubnetgroupgrants.deps.cloudctl.io", nil},
			{"create", "events.events.k8s.io", nil},
			{"create", "securitygrouppolicies.vpcresources.k8s.aws", nil},
		} {
			Expect(can(c.verb, c.resource, c.extra...)).To(BeTrue(), "the manager needs to %s %s", c.verb, c.resource)
		}
		for _, c := range []check{
			{"delete", "serviceaccounts", nil},
			{"create", "rdssubnetgroupgrants.deps.cloudctl.io", nil},
			{"update", "rdssubnetgroupgrants.deps.cloudctl.io", nil},
			{"delete", "rdssubnetgroupgrants.deps.cloudctl.io", nil},
			{"get", "pods", nil},
			{"create", "pods", []string{"--subresource=exec"}},
			{"create", "deployments.apps", nil},
			{"list", "nodes", nil},
			{"delete", "namespaces", nil},
			{"create", "clusterroles.rbac.authorization.k8s.io", nil},
			{"create", "rolebindings.rbac.authorization.k8s.io", nil},
			{"patch", "customresourcedefinitions.apiextensions.k8s.io", nil},
		} {
			Expect(can(c.verb, c.resource, c.extra...)).To(BeFalse(), "the manager must not be able to %s %s", c.verb, c.resource)
		}
	})

	It("rejects invalid and immutable-field edits at the API server, before any controller sees them", func() {
		for name, tc := range map[string]struct {
			manifest string
			want     string
		}{
			"unsupported engine": {strings.Replace(rdsCRManifest("bad-engine", "sg", 20), "postgres", "oracle", 1), "Unsupported value"},
			"storage too small":  {rdsCRManifest("bad-storage", "sg", 10), "greater than or equal to 20"},
			"key and dedicated key together": {strings.Replace(rdsCRManifest("bad-key", "sg", 20), "        allocatedStorage: 20\n",
				"        allocatedStorage: 20\n        encryption:\n          enabled: true\n          kmsKeyRef: shared\n", 1), "mutually exclusive"},
		} {
			_, err := kubectlApplyStdin(tc.manifest, "--dry-run=server")
			Expect(err).To(HaveOccurred(), name)
			Expect(err.Error()).To(ContainSubstring(tc.want), name)
		}

		By("creating a valid CR and then trying to change what can never change")
		_, err := kubectlApplyStdin(rdsCRManifest("immutable", "sg", 40))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, _ = tryKubectl("delete", "appdependencies", "immutable", "-n", resilienceNamespace, "--wait=true", "--timeout=2m")
		})

		_, err = kubectlApplyStdin(strings.Replace(rdsCRManifest("immutable", "sg", 40), "postgres", "mysql", 1))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("engine is immutable"))

		_, err = kubectlApplyStdin(rdsCRManifest("immutable", "other-sg", 40))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dbSubnetGroupName is immutable"))

		_, err = kubectlApplyStdin(rdsCRManifest("immutable", "sg", 20))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot be decreased"))
	})

	It("keeps a database out of an unauthorized subnet group, and lets it in the moment a grant appears", func() {
		_, err := kubectlApplyStdin(rdsCRManifest("rds-unauth", "e2e-subnet-group", 20))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, _ = tryKubectl("delete", "appdependencies", "rds-unauth", "-n", resilienceNamespace, "--wait=true", "--timeout=2m")
			_, _ = tryKubectl("delete", "rdssubnetgroupgrant", "e2e-grant", "--ignore-not-found")
		})

		const notAuthorized = "is not authorized to use DB subnet group"
		Eventually(func(g Gomega) {
			cr, err := getCR(resilienceNamespace, "rds-unauth")
			g.Expect(err).NotTo(HaveOccurred())
			status, _, message := cr.condition("RDSReady")
			g.Expect(status).To(Equal("False"))
			g.Expect(message).To(ContainSubstring(notAuthorized))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		Consistently(func(g Gomega) {
			cr, err := getCR(resilienceNamespace, "rds-unauth")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cr.Status.ManagedResources).To(BeEmpty(), "nothing may be created for a CR that isn't authorized")
		}, 10*time.Second, 2*time.Second).Should(Succeed())

		By("a platform admin grants the namespace")
		_, err = kubectlApplyStdin(fmt.Sprintf(`apiVersion: deps.cloudctl.io/v1alpha1
kind: RDSSubnetGroupGrant
metadata:
  name: e2e-grant
spec:
  dbSubnetGroupName: e2e-subnet-group
  allowedNamespaces: [%s]
`, resilienceNamespace))
		Expect(err).NotTo(HaveOccurred())

		// LocalStack has no RDS, so the CR then fails for a different
		// reason; what matters is that authorization stopped being it.
		Eventually(func(g Gomega) {
			cr, err := getCR(resilienceNamespace, "rds-unauth")
			g.Expect(err).NotTo(HaveOccurred())
			_, _, message := cr.condition("RDSReady")
			g.Expect(message).NotTo(ContainSubstring(notAuthorized))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())
	})

	It("deleting a CR garbage-collects what it owns but leaves the retained queue, with ownership handed back", func() {
		_, err := kubectlApplyStdin(queueCRManifest(resilienceNamespace, "cascade"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) { crReady(g, resilienceNamespace, "cascade") }, 3*time.Minute, 2*time.Second).Should(Succeed())

		queueURL := strings.TrimSpace(mustKubectl("get", "configmap", "cascade-connection", "-n", resilienceNamespace,
			"-o", "jsonpath={.data.SQS_ORDERS_URL}"))
		Expect(queueURL).NotTo(BeEmpty())
		Expect(runAWSCLIPod(nextAWSPod("tags-before"), "sqs", "list-queue-tags", "--queue-url", queueURL)).
			To(ContainSubstring(cloudctlaws.OwnerTagKey), "the queue should carry the operator's ownership tag while managed")

		mustKubectl("delete", "appdependencies", "cascade", "-n", resilienceNamespace, "--wait=true", "--timeout=3m")

		for _, owned := range []struct{ kind, name string }{
			{"configmap", "cascade-connection"},
			{"serviceaccount", "cascade"},
		} {
			Eventually(func() error {
				_, err := tryKubectl("get", owned.kind, owned.name, "-n", resilienceNamespace)
				return err
			}, 2*time.Minute, 2*time.Second).Should(HaveOccurred(), "the owned %s should be garbage-collected with the CR", owned.kind)
		}

		Expect(queueCount(nextAWSPod("queue-after"), queueNameFor("cascade"))).To(Equal(1),
			"a Retain queue must survive its CR")
		Expect(runAWSCLIPod(nextAWSPod("tags-after"), "sqs", "list-queue-tags", "--queue-url", queueURL)).
			NotTo(ContainSubstring(cloudctlaws.OwnerTagKey), "a retained queue must be released, not left claiming an owner that no longer exists")
	})

	It("handles a burst of CRs without duplicates, leaks, or the manager running out of memory", func() {
		const burst = 8
		var manifests []string
		for i := 0; i < burst; i++ {
			manifests = append(manifests, queueCRManifest(resilienceNamespace, fmt.Sprintf("burst-%d", i)))
		}
		_, err := kubectlApplyStdin(strings.Join(manifests, "---\n"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			for i := 0; i < burst; i++ {
				crReady(g, resilienceNamespace, fmt.Sprintf("burst-%d", i))
			}
		}, 5*time.Minute, 3*time.Second).Should(Succeed())

		Expect(queueCount(nextAWSPod("burst-queues"), "e2e-resilience-burst-")).To(Equal(burst),
			"each CR should have exactly one queue")

		var names []string
		for i := 0; i < burst; i++ {
			names = append(names, fmt.Sprintf("burst-%d", i))
		}
		mustKubectl(append([]string{"delete", "appdependencies", "-n", resilienceNamespace, "--wait=true", "--timeout=4m"}, names...)...)
		for i := 0; i < burst; i++ {
			name := fmt.Sprintf("burst-%d", i)
			Eventually(func() error {
				_, err := tryKubectl("get", "configmap", name+"-connection", "-n", resilienceNamespace)
				return err
			}, 2*time.Minute, 2*time.Second).Should(HaveOccurred(), "ConfigMap for %s leaked", name)
		}

		pods, err := managerPods()
		Expect(err).NotTo(HaveOccurred())
		Expect(pods).NotTo(BeEmpty())
		for _, p := range pods {
			Expect(p.restarts()).To(BeZero(), "pod %s restarted under load", p.Metadata.Name)
			Expect(p.oomKilled()).To(BeFalse(), "pod %s was OOM-killed", p.Metadata.Name)
		}
	})

	It("fails over to the standby when the leader is killed, and the new leader really reconciles", func() {
		old, err := leaderPodName()
		Expect(err).NotTo(HaveOccurred())

		By("killing the leader without letting it release the lease")
		mustKubectl("delete", "pod", old, "-n", namespace, "--grace-period=0", "--force")

		Eventually(func() (string, error) { return leaderPodName() }, 2*time.Minute, 2*time.Second).
			ShouldNot(Equal(old), "the standby should take over once the lease expires")

		cmUID, err := objectUID("configmap", resilienceNamespace, "persist-connection")
		Expect(err).NotTo(HaveOccurred())
		mustKubectl("delete", "configmap", "persist-connection", "-n", resilienceNamespace)
		Eventually(func() (string, error) { return objectUID("configmap", resilienceNamespace, "persist-connection") },
			2*time.Minute, 2*time.Second).Should(And(Not(BeEmpty()), Not(Equal(cmUID))),
			"the new leader must repair out-of-band damage, not just hold the lease")

		Eventually(func(g Gomega) {
			pods, err := managerPods()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				g.Expect(p.ready()).To(BeTrue())
			}
		}, 4*time.Minute, 3*time.Second).Should(Succeed(), "the killed pod should be replaced")
	})

	It("keeps its state across a full manager restart: nothing is recreated, duplicated, or forgotten", func() {
		before, err := getCR(resilienceNamespace, "persist")
		Expect(err).NotTo(HaveOccurred())
		createdAt := before.ledgerCreatedAt("sqs", "orders")
		Expect(createdAt).NotTo(BeEmpty())
		saUID, err := objectUID("serviceaccount", resilienceNamespace, "persist")
		Expect(err).NotTo(HaveOccurred())

		mustKubectl("rollout", "restart", "deployment/"+deploymentName, "-n", namespace)
		mustKubectl("rollout", "status", "deployment/"+deploymentName, "-n", namespace, "--timeout=4m")

		By("proving the restarted manager is reconciling by damaging something it must repair")
		cmUID, err := objectUID("configmap", resilienceNamespace, "persist-connection")
		Expect(err).NotTo(HaveOccurred())
		mustKubectl("delete", "configmap", "persist-connection", "-n", resilienceNamespace)
		Eventually(func() (string, error) { return objectUID("configmap", resilienceNamespace, "persist-connection") },
			3*time.Minute, 2*time.Second).Should(And(Not(BeEmpty()), Not(Equal(cmUID))))

		forceReconcile(resilienceNamespace, "persist")
		Eventually(func(g Gomega) { crReady(g, resilienceNamespace, "persist") }, 3*time.Minute, 2*time.Second).Should(Succeed())

		after, err := getCR(resilienceNamespace, "persist")
		Expect(err).NotTo(HaveOccurred())
		Expect(after.ledgerCreatedAt("sqs", "orders")).To(Equal(createdAt), "the ledger entry must be the original, not a re-created one")
		Expect(after.Metadata.UID).To(Equal(before.Metadata.UID))
		Expect(objectUID("serviceaccount", resilienceNamespace, "persist")).To(Equal(saUID), "the ServiceAccount must not be recreated")
		Expect(queueCount(nextAWSPod("persist-queue"), queueNameFor("persist"))).To(Equal(1), "a restart must not create a second queue")
	})

	It("never destroys in-cluster state during an AWS outage, holds deletions, and recovers by itself", func() {
		_, err := kubectlApplyStdin(queueCRManifest(resilienceNamespace, "outage"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) { crReady(g, resilienceNamespace, "outage") }, 3*time.Minute, 2*time.Second).Should(Succeed())
		cmUID, err := objectUID("configmap", resilienceNamespace, "outage-connection")
		Expect(err).NotTo(HaveOccurred())
		saUID, err := objectUID("serviceaccount", resilienceNamespace, "outage")
		Expect(err).NotTo(HaveOccurred())

		By("taking AWS away")
		DeferCleanup(func() {
			_, _ = tryKubectl("scale", "deployment/localstack", "-n", localstackDeployNS, "--replicas=1")
		})
		mustKubectl("scale", "deployment/localstack", "-n", localstackDeployNS, "--replicas=0")
		Eventually(func() string {
			out, _ := tryKubectl("get", "pods", "-n", localstackDeployNS, "-l", "app=localstack", "-o", "name")
			return strings.TrimSpace(out)
		}, 2*time.Minute, 2*time.Second).Should(BeEmpty())

		forceReconcile(resilienceNamespace, "outage")
		Eventually(func(g Gomega) { crNotReady(g, resilienceNamespace, "outage") }, 5*time.Minute, 3*time.Second).Should(Succeed(),
			"a reconcile that can't reach AWS must say so")

		Consistently(func(g Gomega) {
			uid, err := objectUID("configmap", resilienceNamespace, "outage-connection")
			g.Expect(err).NotTo(HaveOccurred(), "the ConfigMap must not be deleted because AWS is down")
			g.Expect(uid).To(Equal(cmUID))
			uid, err = objectUID("serviceaccount", resilienceNamespace, "outage")
			g.Expect(err).NotTo(HaveOccurred(), "the ServiceAccount must not be deleted because AWS is down")
			g.Expect(uid).To(Equal(saUID))
			pods, err := managerPods()
			g.Expect(err).NotTo(HaveOccurred())
			for _, p := range pods {
				g.Expect(p.restarts()).To(BeZero(), "the manager must survive an AWS outage, not crash")
			}
		}, 20*time.Second, 2*time.Second).Should(Succeed())

		By("deleting one CR and creating another while AWS is down")
		mustKubectl("delete", "appdependencies", "persist", "-n", resilienceNamespace, "--wait=false")
		_, err = kubectlApplyStdin(queueCRManifest(resilienceNamespace, "during-outage"))
		Expect(err).NotTo(HaveOccurred())

		Consistently(func(g Gomega) {
			persist, err := getCR(resilienceNamespace, "persist")
			g.Expect(err).NotTo(HaveOccurred(), "a CR whose cleanup can't run must not vanish")
			g.Expect(persist.Metadata.Finalizers).To(ContainElement(finalizer))
			fresh, err := getCR(resilienceNamespace, "during-outage")
			g.Expect(err).NotTo(HaveOccurred())
			status, _, _ := fresh.condition("Ready")
			g.Expect(status).NotTo(Equal("True"), "nothing can be Ready without AWS")
		}, 20*time.Second, 2*time.Second).Should(Succeed())

		By("bringing AWS back, with all of its state gone")
		mustKubectl("scale", "deployment/localstack", "-n", localstackDeployNS, "--replicas=1")
		mustKubectl("rollout", "status", "deployment/localstack", "-n", localstackDeployNS, "--timeout=4m")

		// A reconcile that failed repeatedly is backing off, possibly for
		// minutes; nudge each CR the way an operator would until it
		// converges, so the test measures recovery, not the backoff timer.
		Eventually(func(g Gomega) {
			for _, name := range []string{"outage", "during-outage", "persist"} {
				_ = tryForceReconcile(resilienceNamespace, name)
			}
			crReady(g, resilienceNamespace, "outage")
			crReady(g, resilienceNamespace, "during-outage")
			crGone(g, resilienceNamespace, "persist")
		}, 8*time.Minute, 20*time.Second).Should(Succeed())

		Expect(queueCount(nextAWSPod("outage-queue"), queueNameFor("outage"))).To(Equal(1),
			"the queue LocalStack lost should be recreated exactly once")
		Expect(queueCount(nextAWSPod("during-queue"), queueNameFor("during-outage"))).To(Equal(1))
	})

	It("stays up but unready when it can't reach AWS at startup, instead of crash-looping, and recovers once it can", func() {
		DeferCleanup(func() {
			if CurrentSpecReport().Failed() {
				restoreManager()
			}
		})

		mustKubectl("scale", "deployment/"+deploymentName, "-n", namespace, "--replicas=0")
		waitNoManagerPods()
		mustKubectl("set", "env", "deployment/"+deploymentName, "-n", namespace, "AWS_ENDPOINT_URL=http://127.0.0.1:1")
		mustKubectl("scale", "deployment/"+deploymentName, "-n", namespace, "--replicas=2")

		Eventually(func(g Gomega) {
			pods, err := managerPods()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				g.Expect(p.Status.Phase).To(Equal("Running"))
			}
		}, 3*time.Minute, 3*time.Second).Should(Succeed())

		// Long enough for the liveness probe to run several times: a
		// process that gave up or wedged would be restarted by it.
		Consistently(func(g Gomega) {
			pods, err := managerPods()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				g.Expect(p.Status.Phase).To(Equal("Running"))
				g.Expect(p.restarts()).To(BeZero(), "pod %s must not crash-loop without AWS", p.Metadata.Name)
				g.Expect(p.ready()).To(BeFalse(), "pod %s must not report ready without AWS", p.Metadata.Name)
			}
		}, 75*time.Second, 5*time.Second).Should(Succeed())

		By("pointing it back at AWS")
		restoreManager()
		Eventually(func(g Gomega) {
			pods, err := managerPods()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				g.Expect(p.ready()).To(BeTrue())
			}
		}, 4*time.Minute, 3*time.Second).Should(Succeed())

		forceReconcile(resilienceNamespace, "outage")
		Eventually(func(g Gomega) { crReady(g, resilienceNamespace, "outage") }, 4*time.Minute, 3*time.Second).Should(Succeed())
	})
})

// resilienceNamespace holds every CR the resilience specs create, kept apart
// from the manager's own namespace.
const resilienceNamespace = "e2e-resilience"

// leaderElectionLease is the Lease the manager's leader election uses
// (LeaderElectionID in cmd/main.go).
const leaderElectionLease = "687fa3e1.cloudctl.io"

type crView struct {
	Metadata struct {
		UID               string            `json:"uid"`
		Generation        int64             `json:"generation"`
		DeletionTimestamp string            `json:"deletionTimestamp"`
		Finalizers        []string          `json:"finalizers"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
	Status struct {
		Conditions []struct {
			Type               string `json:"type"`
			Status             string `json:"status"`
			Reason             string `json:"reason"`
			Message            string `json:"message"`
			ObservedGeneration int64  `json:"observedGeneration"`
		} `json:"conditions"`
		ManagedResources []struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			ARN       string `json:"arn"`
			State     string `json:"state"`
			CreatedAt string `json:"createdAt"`
		} `json:"managedResources"`
		IAMRoleARN string `json:"iamRoleARN"`
	} `json:"status"`
}

func getCR(ns, name string) (crView, error) {
	var cr crView
	out, err := utils.Run(exec.Command("kubectl", "get", "appdependencies", name, "-n", ns, "-o", "json"))
	if err != nil {
		return cr, err
	}
	return cr, json.Unmarshal([]byte(out), &cr)
}

// condition returns a condition's status, reason and message ("" if absent).
func (c crView) condition(condType string) (status, reason, message string) {
	for _, cond := range c.Status.Conditions {
		if cond.Type == condType {
			return cond.Status, cond.Reason, cond.Message
		}
	}
	return "", "", ""
}

func (c crView) ledgerCreatedAt(resourceType, name string) string {
	for _, r := range c.Status.ManagedResources {
		if r.Type == resourceType && r.Name == name {
			return r.CreatedAt
		}
	}
	return ""
}

// crReady asserts the CR's aggregate Ready condition is True for its
// current generation.
func crReady(g Gomega, ns, name string) {
	cr, err := getCR(ns, name)
	g.Expect(err).NotTo(HaveOccurred())
	status, _, message := cr.condition("Ready")
	g.Expect(status).To(Equal("True"), "CR %s/%s is not Ready: %s", ns, name, message)
	for _, cond := range cr.Status.Conditions {
		if cond.Type == "Ready" {
			g.Expect(cond.ObservedGeneration).To(Equal(cr.Metadata.Generation), "Ready is stale for generation %d", cr.Metadata.Generation)
		}
	}
}

// crNotReady asserts the CR's aggregate Ready condition is explicitly False.
func crNotReady(g Gomega, ns, name string) {
	cr, err := getCR(ns, name)
	g.Expect(err).NotTo(HaveOccurred())
	status, _, _ := cr.condition("Ready")
	g.Expect(status).To(Equal("False"))
}

// crGone asserts the CR no longer exists.
func crGone(g Gomega, ns, name string) {
	_, err := utils.Run(exec.Command("kubectl", "get", "appdependencies", name, "-n", ns))
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("NotFound"))
}

// forceReconcile triggers a reconcile the way a human would, via the
// force-reconcile annotation. Needed whenever a test changes something
// outside the cluster (AWS), since nothing there raises a watch event.
func forceReconcile(ns, name string) {
	GinkgoHelper()
	Expect(tryForceReconcile(ns, name)).To(Succeed(), "failed to force a reconcile of %s/%s", ns, name)
}

// tryForceReconcile is forceReconcile for callers inside a polling loop,
// where the CR may legitimately be gone by now.
func tryForceReconcile(ns, name string) error {
	_, err := utils.Run(exec.Command("kubectl", "annotate", "appdependencies", name, "-n", ns, "--overwrite",
		fmt.Sprintf("%s=%d", predicates.ForceReconcileAnnotation, time.Now().UnixNano())))
	return err
}

func kubectlApplyStdin(manifest string, extraArgs ...string) (string, error) {
	args := append([]string{"apply", "-f", "-"}, extraArgs...)
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(manifest)
	return utils.Run(cmd)
}

// objectUID returns the UID of a namespaced object, so a test can tell
// "still the same object" from "deleted and recreated".
func objectUID(kind, ns, name string) (string, error) {
	out, err := utils.Run(exec.Command("kubectl", "get", kind, name, "-n", ns, "-o", "jsonpath={.metadata.uid}"))
	return strings.TrimSpace(out), err
}

// queueCRManifest is a minimal CR owning one SQS queue named "orders".
func queueCRManifest(ns, name string) string {
	return fmt.Sprintf(`apiVersion: deps.cloudctl.io/v1alpha1
kind: AppDependencies
metadata:
  name: %s
  namespace: %s
spec:
  sqs:
    resources:
      - name: orders
`, name, ns)
}

type podView struct {
	Metadata struct {
		Name              string `json:"name"`
		DeletionTimestamp string `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		Phase             string `json:"phase"`
		ContainerStatuses []struct {
			Ready        bool `json:"ready"`
			RestartCount int  `json:"restartCount"`
			LastState    struct {
				Terminated struct {
					Reason string `json:"reason"`
				} `json:"terminated"`
			} `json:"lastState"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (p podView) restarts() int {
	total := 0
	for _, c := range p.Status.ContainerStatuses {
		total += c.RestartCount
	}
	return total
}

func (p podView) ready() bool {
	if len(p.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, c := range p.Status.ContainerStatuses {
		if !c.Ready {
			return false
		}
	}
	return true
}

func (p podView) oomKilled() bool {
	for _, c := range p.Status.ContainerStatuses {
		if c.LastState.Terminated.Reason == "OOMKilled" {
			return true
		}
	}
	return false
}

// managerPods returns the manager's pods that are not being deleted.
func managerPods() ([]podView, error) {
	out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", namespace,
		"-l", "control-plane=controller-manager", "-o", "json"))
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []podView `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, err
	}
	var live []podView
	for _, p := range list.Items {
		if p.Metadata.DeletionTimestamp == "" {
			live = append(live, p)
		}
	}
	return live, nil
}

// leaderPodName returns the name of the pod currently holding the manager's
// leader-election lease.
func leaderPodName() (string, error) {
	out, err := utils.Run(exec.Command("kubectl", "get", "lease", leaderElectionLease, "-n", namespace,
		"-o", "jsonpath={.spec.holderIdentity}"))
	if err != nil {
		return "", err
	}
	holder := strings.TrimSpace(out)
	if holder == "" {
		return "", fmt.Errorf("lease %s has no holder yet", leaderElectionLease)
	}
	// controller-runtime appends "_<uuid>" to the hostname.
	return strings.SplitN(holder, "_", 2)[0], nil
}

// queueCount returns how many queues in LocalStack have name containing substr.
func queueCount(podName, substr string) int {
	logs := runAWSCLIPod(podName, "sqs", "list-queues")
	return strings.Count(logs, substr)
}
