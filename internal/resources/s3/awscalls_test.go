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

package s3

import (
	"context"
	"strings"
	"testing"
	"time"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/kmstest"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// awsCalls counts the calls Ensure makes, by operation name.
type awsCalls map[string]int

// writes are the calls that change something in AWS: everything but reads.
func (c awsCalls) writes() map[string]int {
	out := map[string]int{}
	for op, n := range c {
		if !strings.HasPrefix(op, "Get") && !strings.HasPrefix(op, "List") &&
			!strings.HasPrefix(op, "Describe") && !strings.HasPrefix(op, "Head") {
			out[op] = n
		}
	}
	return out
}

// countingS3API holds the client in a named field rather than embedding it: a method
// added to s3API then stops this file compiling until it is counted too.
type countingS3API struct {
	client s3API
	calls  awsCalls
}

func (w countingS3API) HeadBucket(ctx context.Context, in *s3sdk.HeadBucketInput, optFns ...func(*s3sdk.Options)) (*s3sdk.HeadBucketOutput, error) {
	w.calls["HeadBucket"]++
	return w.client.HeadBucket(ctx, in, optFns...)
}

func (w countingS3API) CreateBucket(ctx context.Context, in *s3sdk.CreateBucketInput, optFns ...func(*s3sdk.Options)) (*s3sdk.CreateBucketOutput, error) {
	w.calls["CreateBucket"]++
	return w.client.CreateBucket(ctx, in, optFns...)
}

func (w countingS3API) DeleteBucket(ctx context.Context, in *s3sdk.DeleteBucketInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketOutput, error) {
	w.calls["DeleteBucket"]++
	return w.client.DeleteBucket(ctx, in, optFns...)
}

func (w countingS3API) GetBucketTagging(ctx context.Context, in *s3sdk.GetBucketTaggingInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketTaggingOutput, error) {
	w.calls["GetBucketTagging"]++
	return w.client.GetBucketTagging(ctx, in, optFns...)
}

func (w countingS3API) PutBucketTagging(ctx context.Context, in *s3sdk.PutBucketTaggingInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketTaggingOutput, error) {
	w.calls["PutBucketTagging"]++
	return w.client.PutBucketTagging(ctx, in, optFns...)
}

func (w countingS3API) PutBucketVersioning(ctx context.Context, in *s3sdk.PutBucketVersioningInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketVersioningOutput, error) {
	w.calls["PutBucketVersioning"]++
	return w.client.PutBucketVersioning(ctx, in, optFns...)
}

func (w countingS3API) GetBucketVersioning(ctx context.Context, in *s3sdk.GetBucketVersioningInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketVersioningOutput, error) {
	w.calls["GetBucketVersioning"]++
	return w.client.GetBucketVersioning(ctx, in, optFns...)
}

func (w countingS3API) GetBucketLifecycleConfiguration(ctx context.Context, in *s3sdk.GetBucketLifecycleConfigurationInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketLifecycleConfigurationOutput, error) {
	w.calls["GetBucketLifecycleConfiguration"]++
	return w.client.GetBucketLifecycleConfiguration(ctx, in, optFns...)
}

func (w countingS3API) PutBucketLifecycleConfiguration(ctx context.Context, in *s3sdk.PutBucketLifecycleConfigurationInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketLifecycleConfigurationOutput, error) {
	w.calls["PutBucketLifecycleConfiguration"]++
	return w.client.PutBucketLifecycleConfiguration(ctx, in, optFns...)
}

func (w countingS3API) DeleteBucketLifecycle(ctx context.Context, in *s3sdk.DeleteBucketLifecycleInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketLifecycleOutput, error) {
	w.calls["DeleteBucketLifecycle"]++
	return w.client.DeleteBucketLifecycle(ctx, in, optFns...)
}

func (w countingS3API) ListObjectVersions(ctx context.Context, in *s3sdk.ListObjectVersionsInput, optFns ...func(*s3sdk.Options)) (*s3sdk.ListObjectVersionsOutput, error) {
	w.calls["ListObjectVersions"]++
	return w.client.ListObjectVersions(ctx, in, optFns...)
}

func (w countingS3API) DeleteObjects(ctx context.Context, in *s3sdk.DeleteObjectsInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectsOutput, error) {
	w.calls["DeleteObjects"]++
	return w.client.DeleteObjects(ctx, in, optFns...)
}

func (w countingS3API) ListMultipartUploads(ctx context.Context, in *s3sdk.ListMultipartUploadsInput, optFns ...func(*s3sdk.Options)) (*s3sdk.ListMultipartUploadsOutput, error) {
	w.calls["ListMultipartUploads"]++
	return w.client.ListMultipartUploads(ctx, in, optFns...)
}

func (w countingS3API) AbortMultipartUpload(ctx context.Context, in *s3sdk.AbortMultipartUploadInput, optFns ...func(*s3sdk.Options)) (*s3sdk.AbortMultipartUploadOutput, error) {
	w.calls["AbortMultipartUpload"]++
	return w.client.AbortMultipartUpload(ctx, in, optFns...)
}

func (w countingS3API) GetBucketEncryption(ctx context.Context, in *s3sdk.GetBucketEncryptionInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketEncryptionOutput, error) {
	w.calls["GetBucketEncryption"]++
	return w.client.GetBucketEncryption(ctx, in, optFns...)
}

func (w countingS3API) PutBucketEncryption(ctx context.Context, in *s3sdk.PutBucketEncryptionInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketEncryptionOutput, error) {
	w.calls["PutBucketEncryption"]++
	return w.client.PutBucketEncryption(ctx, in, optFns...)
}

func (w countingS3API) GetBucketPolicy(ctx context.Context, in *s3sdk.GetBucketPolicyInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketPolicyOutput, error) {
	w.calls["GetBucketPolicy"]++
	return w.client.GetBucketPolicy(ctx, in, optFns...)
}

func (w countingS3API) PutBucketPolicy(ctx context.Context, in *s3sdk.PutBucketPolicyInput, optFns ...func(*s3sdk.Options)) (*s3sdk.PutBucketPolicyOutput, error) {
	w.calls["PutBucketPolicy"]++
	return w.client.PutBucketPolicy(ctx, in, optFns...)
}

func (w countingS3API) DeleteBucketPolicy(ctx context.Context, in *s3sdk.DeleteBucketPolicyInput, optFns ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketPolicyOutput, error) {
	w.calls["DeleteBucketPolicy"]++
	return w.client.DeleteBucketPolicy(ctx, in, optFns...)
}

func (w countingS3API) GetBucketWebsite(ctx context.Context, in *s3sdk.GetBucketWebsiteInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketWebsiteOutput, error) {
	w.calls["GetBucketWebsite"]++
	return w.client.GetBucketWebsite(ctx, in, optFns...)
}

func (w countingS3API) GetBucketPolicyStatus(ctx context.Context, in *s3sdk.GetBucketPolicyStatusInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketPolicyStatusOutput, error) {
	w.calls["GetBucketPolicyStatus"]++
	return w.client.GetBucketPolicyStatus(ctx, in, optFns...)
}

func (w countingS3API) GetPublicAccessBlock(ctx context.Context, in *s3sdk.GetPublicAccessBlockInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetPublicAccessBlockOutput, error) {
	w.calls["GetPublicAccessBlock"]++
	return w.client.GetPublicAccessBlock(ctx, in, optFns...)
}

func (w countingS3API) GetBucketAcl(ctx context.Context, in *s3sdk.GetBucketAclInput, optFns ...func(*s3sdk.Options)) (*s3sdk.GetBucketAclOutput, error) {
	w.calls["GetBucketAcl"]++
	return w.client.GetBucketAcl(ctx, in, optFns...)
}

// A pass over a bucket with nothing to change must change nothing in AWS,
// inside the trust window and at the recheck after it.
func TestEnsure_AnIdleBucketIsNotWrittenTo(t *testing.T) {
	calls := awsCalls{}
	client := countingS3API{client: newFakeS3(), calls: calls}
	kmsClient := kmstest.NewFakeKMSClient()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{
		Name:       "receipts",
		Backup:     &depsv1alpha1.S3BackupSpec{Enabled: true},
		Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true},
	}}}
	ensure := func(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
		t.Helper()
		clear(calls)
		updated, err := Ensure(context.Background(), client, kmsClient, nil, "default", "checkout-service", "uid-1",
			testRegion, testAccountID, spec, ledger, nil, nil)
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
		return updated
	}
	ledger := ensure(ensure(nil))

	ledger = ensure(ledger)
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("a pass inside the trust window wrote to S3: %v", writes)
	}

	stale := metav1.NewTime(time.Now().Add(-2 * status.TrustWindow))
	for i := range ledger {
		ledger[i].LastVerifiedAt = &stale
	}
	ensure(ledger)
	if writes := calls.writes(); len(writes) != 0 {
		t.Errorf("the recheck after the trust window wrote to S3: %v", writes)
	}
	if len(calls) == 0 {
		t.Error("the recheck made no calls at all, so it checked nothing")
	}
	t.Logf("calls at the recheck: %v", calls)
}
