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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/test/utils"
)

// s3BucketName mirrors the unexported bucketName function in
// internal/resources/s3/s3.go exactly (base name truncated to leave room
// for an 8-char account-ID hash suffix, for S3's globally-unique naming
// requirement) - duplicated here since that function isn't exported and
// this is the only place outside that package that needs to predict a
// bucket's name before the operator creates it.
func s3BucketName(namespace, crName, resourceKey, accountID string) string {
	const s3NameMaxLen = 63
	const accountHashLen = 8
	base := cloudctlaws.ResourceName(namespace, crName, "s3", resourceKey, s3NameMaxLen-accountHashLen-1)
	sum := sha256.Sum256([]byte(accountID))
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(sum[:])[:accountHashLen])
}

// s3LifecycleSpec registers the S3 scenario into the shared Describe block
// defined in lifecycle_test.go.
//
// Like SNS, the bucket is pre-created and pre-tagged directly via the AWS
// CLI before the CR's s3 section is applied, instead of letting Ensure's
// own CreateBucket path run. Ensure calls CreateBucket with tags set
// inline via CreateBucketConfiguration.Tags (a newer S3 feature), and this
// LocalStack version rejects that request outright with a MalformedXML
// error whenever CreateBucketConfiguration carries Tags without a
// LocationConstraint - confirmed directly against this project's pinned
// LocalStack image using the same aws-sdk-go-v2 types Ensure itself uses,
// not just the CLI. Production code is correct: this exact call shape
// (us-east-1, Tags, no LocationConstraint) was already exercised
// successfully against real AWS earlier in this project's live-AWS test
// campaign. Pre-creating with a plain CreateBucket + separate
// PutBucketTagging (confirmed working against this LocalStack version)
// sidesteps the one call this environment can't be trusted to test;
// everything downstream - ownership verification, attribute
// reconciliation, the connection ConfigMap, and the deletion/cleanup path
// - still runs for real.
func s3LifecycleSpec() {
	It("adds a real S3 bucket to the same CR and reports Ready", func() {
		By("reading the existing CR's UID")
		cmd := exec.Command("kubectl", "get", "appdependencies", "e2e-orders", "-n", namespace,
			"-o", "jsonpath={.metadata.uid}")
		uid, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("pre-creating and pre-tagging the bucket directly, working around a LocalStack limitation")
		bucket := s3BucketName(namespace, "e2e-orders", "receipts", "000000000000")
		runAWSCLIPod("seed-bucket", "s3api", "create-bucket", "--bucket", bucket)
		runAWSCLIPod("tag-bucket", "s3api", "put-bucket-tagging", "--bucket", bucket,
			"--tagging", fmt.Sprintf(
				`{"TagSet":[{"Key":"%s","Value":"%s"},{"Key":"%s","Value":"%s"}]}`,
				cloudctlaws.OwnerTagKey, cloudctlaws.OwnerTagValue(namespace, "e2e-orders"),
				cloudctlaws.OwnerUIDTagKey, uid,
			),
		)

		By("applying the updated sample CR with an s3 section added")
		cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/sqs-sns-dynamodb-s3-sample.yaml", "-n", namespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply the updated sample CR")

		By("waiting for the CR to report Ready again")
		Eventually(verifyReady, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("verifying the connection ConfigMap was populated with the derived bucket name")
		cmd = exec.Command("kubectl", "get", "configmap", "e2e-orders-connection", "-n", namespace,
			"-o", "jsonpath={.data.S3_RECEIPTS_BUCKET}")
		output, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(output).To(Equal(bucket))

		By("verifying the real bucket still exists in LocalStack, independent of the operator's own state")
		logs := runAWSCLIPod("verify-bucket", "s3api", "list-buckets")
		Expect(logs).To(ContainSubstring(bucket), "expected the real bucket to show up in LocalStack's own list-buckets output")
	})
}
