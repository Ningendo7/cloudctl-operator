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
	"errors"
	"fmt"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// fakeHTTPStatusError builds a generic, un-typed error carrying only an
// HTTP status code — the shape HeadBucket's own docs confirm it returns for
// a missing/forbidden/relocated bucket instead of a typed exception with a
// message body.
func fakeHTTPStatusError(status int) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      errors.New("generic HTTP status error"),
		},
	}
}

// fakeAWSError is a minimal smithy.APIError implementation for injecting
// AWS-style errors (NoSuchTagSet, throttling, permission-denied, ...) into
// fakeS3 calls, so error-classification behavior is actually exercisable
// in tests.
type fakeAWSError struct {
	code  string
	fault smithy.ErrorFault
}

func (e *fakeAWSError) Error() string                 { return e.code }
func (e *fakeAWSError) ErrorCode() string             { return e.code }
func (e *fakeAWSError) ErrorMessage() string          { return e.code }
func (e *fakeAWSError) ErrorFault() smithy.ErrorFault { return e.fault }

// fakeObjectVersion and fakeUpload model just enough of a bucket's contents
// to exercise the emptiness check and the deletion sequence (object
// versions + delete markers + in-progress multipart uploads).
type fakeObjectVersion struct {
	key            string
	versionID      string
	isDeleteMarker bool
}

type fakeUpload struct {
	key      string
	uploadID string
}

type fakeBucket struct {
	tags             map[string]string
	versioningStatus types.BucketVersioningStatus
	hasLifecycle     bool
	lifecycleRules   []types.LifecycleRule
	versions         []fakeObjectVersion
	uploads          []fakeUpload
}

type fakeS3 struct {
	buckets map[string]*fakeBucket // keyed by bucket name

	createBucketErr         error
	headBucketErr           error
	getBucketTaggingErr     error
	putBucketTaggingErr     error
	abortMultipartUploadErr error
	deleteObjectsErr        error
	// failToDeleteKey, if set, makes DeleteObjects report that one key
	// failed (via Errors) while leaving it in place, simulating S3's
	// documented partial-failure behavior within an otherwise-successful call.
	failToDeleteKey string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{buckets: map[string]*fakeBucket{}}
}

func (f *fakeS3) HeadBucket(_ context.Context, in *s3sdk.HeadBucketInput, _ ...func(*s3sdk.Options)) (*s3sdk.HeadBucketOutput, error) {
	if f.headBucketErr != nil {
		return nil, f.headBucketErr
	}
	if _, ok := f.buckets[*in.Bucket]; !ok {
		return nil, &types.NotFound{}
	}
	return &s3sdk.HeadBucketOutput{}, nil
}

func (f *fakeS3) CreateBucket(_ context.Context, in *s3sdk.CreateBucketInput, _ ...func(*s3sdk.Options)) (*s3sdk.CreateBucketOutput, error) {
	if f.createBucketErr != nil {
		return nil, f.createBucketErr
	}
	// No tags here - CreateBucket doesn't accept them, matching real S3.
	f.buckets[*in.Bucket] = &fakeBucket{}
	return &s3sdk.CreateBucketOutput{}, nil
}

func (f *fakeS3) DeleteBucket(_ context.Context, in *s3sdk.DeleteBucketInput, _ ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	if len(b.versions) > 0 || len(b.uploads) > 0 {
		return nil, &fakeAWSError{code: "BucketNotEmpty"}
	}
	delete(f.buckets, *in.Bucket)
	return &s3sdk.DeleteBucketOutput{}, nil
}

func (f *fakeS3) GetBucketTagging(_ context.Context, in *s3sdk.GetBucketTaggingInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetBucketTaggingOutput, error) {
	if f.getBucketTaggingErr != nil {
		return nil, f.getBucketTaggingErr
	}
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	if len(b.tags) == 0 {
		return nil, &fakeAWSError{code: "NoSuchTagSet"}
	}
	return &s3sdk.GetBucketTaggingOutput{TagSet: mapToTags(b.tags)}, nil
}

func (f *fakeS3) PutBucketTagging(_ context.Context, in *s3sdk.PutBucketTaggingInput, _ ...func(*s3sdk.Options)) (*s3sdk.PutBucketTaggingOutput, error) {
	if f.putBucketTaggingErr != nil {
		return nil, f.putBucketTaggingErr
	}
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	b.tags = tagsToMap(in.Tagging.TagSet)
	return &s3sdk.PutBucketTaggingOutput{}, nil
}

func (f *fakeS3) PutBucketVersioning(_ context.Context, in *s3sdk.PutBucketVersioningInput, _ ...func(*s3sdk.Options)) (*s3sdk.PutBucketVersioningOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	b.versioningStatus = in.VersioningConfiguration.Status
	return &s3sdk.PutBucketVersioningOutput{}, nil
}

func (f *fakeS3) GetBucketVersioning(_ context.Context, in *s3sdk.GetBucketVersioningInput, _ ...func(*s3sdk.Options)) (*s3sdk.GetBucketVersioningOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	return &s3sdk.GetBucketVersioningOutput{Status: b.versioningStatus}, nil
}

func (f *fakeS3) PutBucketLifecycleConfiguration(_ context.Context, in *s3sdk.PutBucketLifecycleConfigurationInput, _ ...func(*s3sdk.Options)) (*s3sdk.PutBucketLifecycleConfigurationOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	b.hasLifecycle = true
	b.lifecycleRules = in.LifecycleConfiguration.Rules
	return &s3sdk.PutBucketLifecycleConfigurationOutput{}, nil
}

func (f *fakeS3) DeleteBucketLifecycle(_ context.Context, in *s3sdk.DeleteBucketLifecycleInput, _ ...func(*s3sdk.Options)) (*s3sdk.DeleteBucketLifecycleOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	b.hasLifecycle = false
	b.lifecycleRules = nil
	return &s3sdk.DeleteBucketLifecycleOutput{}, nil
}

func (f *fakeS3) ListObjectVersions(_ context.Context, in *s3sdk.ListObjectVersionsInput, _ ...func(*s3sdk.Options)) (*s3sdk.ListObjectVersionsOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}

	var versions []types.ObjectVersion
	var markers []types.DeleteMarkerEntry
	for _, v := range b.versions {
		key, versionID := v.key, v.versionID
		if v.isDeleteMarker {
			markers = append(markers, types.DeleteMarkerEntry{Key: &key, VersionId: &versionID})
		} else {
			versions = append(versions, types.ObjectVersion{Key: &key, VersionId: &versionID})
		}
	}
	return &s3sdk.ListObjectVersionsOutput{Versions: versions, DeleteMarkers: markers}, nil
}

func (f *fakeS3) DeleteObjects(_ context.Context, in *s3sdk.DeleteObjectsInput, _ ...func(*s3sdk.Options)) (*s3sdk.DeleteObjectsOutput, error) {
	if f.deleteObjectsErr != nil {
		return nil, f.deleteObjectsErr
	}
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	toDelete := map[string]bool{}
	for _, o := range in.Delete.Objects {
		toDelete[fmt.Sprintf("%s/%s", *o.Key, versionOf(o.VersionId))] = true
	}
	var remaining []fakeObjectVersion
	var partialErrors []types.Error
	for _, v := range b.versions {
		if !toDelete[fmt.Sprintf("%s/%s", v.key, v.versionID)] {
			remaining = append(remaining, v)
			continue
		}
		if v.key == f.failToDeleteKey {
			// Simulate a real, documented S3 behavior: DeleteObjects can
			// return 200 OK overall while individual objects within the
			// batch failed - the object stays present, and its failure is
			// reported in Errors instead of Deleted.
			remaining = append(remaining, v)
			msg := "simulated per-object failure"
			partialErrors = append(partialErrors, types.Error{Key: &v.key, Message: &msg})
		}
	}
	b.versions = remaining
	return &s3sdk.DeleteObjectsOutput{Errors: partialErrors}, nil
}

func (f *fakeS3) ListMultipartUploads(_ context.Context, in *s3sdk.ListMultipartUploadsInput, _ ...func(*s3sdk.Options)) (*s3sdk.ListMultipartUploadsOutput, error) {
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	var uploads []types.MultipartUpload
	for _, u := range b.uploads {
		key, uploadID := u.key, u.uploadID
		uploads = append(uploads, types.MultipartUpload{Key: &key, UploadId: &uploadID})
	}
	return &s3sdk.ListMultipartUploadsOutput{Uploads: uploads}, nil
}

func (f *fakeS3) AbortMultipartUpload(_ context.Context, in *s3sdk.AbortMultipartUploadInput, _ ...func(*s3sdk.Options)) (*s3sdk.AbortMultipartUploadOutput, error) {
	if f.abortMultipartUploadErr != nil {
		return nil, f.abortMultipartUploadErr
	}
	b, ok := f.buckets[*in.Bucket]
	if !ok {
		return nil, &types.NoSuchBucket{}
	}
	var remaining []fakeUpload
	found := false
	for _, u := range b.uploads {
		if u.key == *in.Key && u.uploadID == *in.UploadId {
			found = true
			continue
		}
		remaining = append(remaining, u)
	}
	if !found {
		return nil, &types.NoSuchUpload{}
	}
	b.uploads = remaining
	return &s3sdk.AbortMultipartUploadOutput{}, nil
}

func versionOf(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
