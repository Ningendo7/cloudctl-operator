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
	"encoding/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/awspolicy"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func ensureReceipts(t *testing.T, client *fakeS3, adopt bool, recordEvent status.EventRecorder) error {
	t.Helper()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts", Adopt: adopt}}}
	_, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, nil, nil, recordEvent)
	return err
}

func receiptsBucket() string {
	return bucketName("default", "checkout-service", "receipts", testAccountID)
}

// statementsOf returns the Sids in a bucket policy.
func statementsOf(t *testing.T, policy string) []string {
	t.Helper()
	var doc struct{ Statement []struct{ Sid string } }
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		t.Fatalf("policy is not JSON: %v: %s", err, policy)
	}
	sids := make([]string, 0, len(doc.Statement))
	for _, s := range doc.Statement {
		sids = append(sids, s.Sid)
	}
	return sids
}

func TestEnsure_ABucketRefusesPlainHTTP(t *testing.T) {
	client := newFakeS3()
	recordEvent, events := newEventCollector()

	if err := ensureReceipts(t, client, false, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	policy := client.buckets[receiptsBucket()].policy
	for _, want := range []string{`"Effect":"Deny"`, `"Action":"s3:*"`, `"aws:SecureTransport":"false"`,
		`arn:aws:s3:::` + receiptsBucket() + `"`, `arn:aws:s3:::` + receiptsBucket() + `/*"`} {
		if !strings.Contains(policy, want) {
			t.Errorf("the bucket policy should contain %s, got %s", want, policy)
		}
	}
	if got := countEvents(*events, "TransportSecurityEnforced"); got != 1 {
		t.Errorf("expected one TransportSecurityEnforced event, got %+v", *events)
	}
}

func TestEnsure_TheTransportStatementIsWrittenOnlyOnce(t *testing.T) {
	client := newFakeS3()
	recordEvent, events := newEventCollector()
	if err := ensureReceipts(t, client, false, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	writes := client.policyWrites

	if err := ensureReceipts(t, client, false, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if client.policyWrites != writes {
		t.Errorf("a pass that changed nothing wrote the policy %d more times", client.policyWrites-writes)
	}
	if got := countEvents(*events, "TransportSecurityEnforced"); got != 1 {
		t.Errorf("the event should be recorded when the statement is added, not on every pass, got %d", got)
	}
}

func TestEnsure_ARemovedTransportStatementIsPutBack(t *testing.T) {
	client := newFakeS3()
	if err := ensureReceipts(t, client, false, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	client.buckets[receiptsBucket()].policy = ""

	if err := ensureReceipts(t, client, false, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if !strings.Contains(client.buckets[receiptsBucket()].policy, awspolicy.InsecureTransportSid) {
		t.Errorf("the statement was not restored: %q", client.buckets[receiptsBucket()].policy)
	}
}

func ownerTagsFor() map[string]string { return ownerTags("default", "checkout-service", "uid-1") }

func TestEnsure_AnAdoptedBucketGetsTheStatementToo_AndOthersAreKept(t *testing.T) {
	client := newFakeS3()
	foreign := `{"Version":"2012-10-17","Statement":[{"Sid":"cdn","Effect":"Allow","Principal":{"Service":"cloudfront.amazonaws.com"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::x/*"}]}`
	client.buckets[receiptsBucket()] = &fakeBucket{tags: map[string]string{"team": "someone-else"}, policy: foreign}
	recordEvent, events := newEventCollector()

	if err := ensureReceipts(t, client, true, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	got := statementsOf(t, client.buckets[receiptsBucket()].policy)
	if len(got) != 2 || got[0] != "cdn" || got[1] != awspolicy.InsecureTransportSid {
		t.Errorf("statements = %v, want their CloudFront statement kept and ours added", got)
	}
	if countEvents(*events, "TransportSecurityEnforced") != 1 {
		t.Errorf("adopting should say the policy was edited, got %+v", *events)
	}
}

// A website endpoint speaks HTTP only: the statement would take the site down.
func TestEnsure_ABucketHostingAWebsiteIsNotMadeTLSOnly(t *testing.T) {
	client := newFakeS3()
	client.buckets[receiptsBucket()] = &fakeBucket{tags: ownerTagsFor(), website: true}
	recordEvent, events := newEventCollector()

	if err := ensureReceipts(t, client, false, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if policy := client.buckets[receiptsBucket()].policy; strings.Contains(policy, awspolicy.InsecureTransportSid) {
		t.Errorf("a website bucket got the TLS-only statement: %s", policy)
	}
	if countEvents(*events, "TransportSecurityNotEnforced") != 1 {
		t.Errorf("expected a TransportSecurityNotEnforced warning, got %+v", *events)
	}
}

func TestEnsure_OurStatementIsRemovedOnceABucketHostsAWebsite(t *testing.T) {
	client := newFakeS3()
	if err := ensureReceipts(t, client, false, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	client.buckets[receiptsBucket()].website = true

	if err := ensureReceipts(t, client, false, nil); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if policy := client.buckets[receiptsBucket()].policy; policy != "" {
		t.Errorf("the only statement was ours, so the policy should be gone, got %s", policy)
	}
}

// Guessing wrong either breaks a site or leaves HTTP open, so a failed lookup
// fails the pass.
func TestEnsure_FailingToTellWhetherABucketHostsAWebsiteFailsThePass(t *testing.T) {
	client := newFakeS3()
	client.getBucketWebsiteErr = &fakeAWSError{code: "AccessDenied", fault: 1}

	err := ensureReceipts(t, client, false, nil)

	if err == nil || !strings.Contains(err.Error(), "website") {
		t.Fatalf("Ensure() error = %v, want the failed website lookup", err)
	}
	if client.buckets[receiptsBucket()].policy != "" {
		t.Error("no policy should be written when the website check could not run")
	}
}

func publicRead() []types.Grant {
	return []types.Grant{{Grantee: &types.Grantee{Type: types.TypeGroup, URI: aws.String(allUsersURI)}, Permission: types.PermissionRead}}
}

func TestEnsure_APublicBucketIsReportedAndLeftAlone(t *testing.T) {
	blocksEverything := &types.PublicAccessBlockConfiguration{RestrictPublicBuckets: aws.Bool(true), IgnorePublicAcls: aws.Bool(true)}
	for name, tc := range map[string]struct {
		bucket     fakeBucket
		wantReport string
	}{
		"public policy": {fakeBucket{policyPublic: true, policy: `{"Version":"2012-10-17","Statement":[]}`}, "its bucket policy"},
		"public ACL":    {fakeBucket{aclGrants: publicRead()}, "its ACL"},
		"both":          {fakeBucket{policyPublic: true, policy: `{"Version":"2012-10-17","Statement":[]}`, aclGrants: publicRead()}, "its bucket policy and its ACL"},
		"public policy that Block Public Access ignores": {fakeBucket{policyPublic: true, policy: `{"Version":"2012-10-17","Statement":[]}`, publicBlock: blocksEverything}, ""},
		"public ACL that Block Public Access ignores":    {fakeBucket{aclGrants: publicRead(), publicBlock: blocksEverything}, ""},
		"private": {fakeBucket{}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeS3()
			bucket := tc.bucket
			bucket.tags = ownerTagsFor()
			client.buckets[receiptsBucket()] = &bucket
			recordEvent, events := newEventCollector()

			if err := ensureReceipts(t, client, false, recordEvent); err != nil {
				t.Fatalf("Ensure() error = %v", err)
			}

			warnings := 0
			for _, e := range *events {
				if e.reason == "BucketPubliclyAccessible" {
					warnings++
					if !strings.Contains(e.message, tc.wantReport) {
						t.Errorf("the warning should name %q, got %q", tc.wantReport, e.message)
					}
				}
			}
			if (tc.wantReport != "") != (warnings == 1) || warnings > 1 {
				t.Errorf("BucketPubliclyAccessible warnings = %d, want report %q; events %+v", warnings, tc.wantReport, *events)
			}
			if bucket.aclGrants != nil && len(client.buckets[receiptsBucket()].aclGrants) != len(bucket.aclGrants) {
				t.Error("the operator must not change a bucket's ACL")
			}
		})
	}
}

// The check is advisory: a missing permission must not fail the bucket.
func TestEnsure_AFailedPublicAccessCheckIsReportedNotFatal(t *testing.T) {
	client := newFakeS3()
	client.getPublicAccessBlockErr = &fakeAWSError{code: "AccessDenied", fault: 1}
	recordEvent, events := newEventCollector()

	if err := ensureReceipts(t, client, false, recordEvent); err != nil {
		t.Fatalf("Ensure() error = %v, an advisory check must not fail the pass", err)
	}
	if countEvents(*events, "PublicAccessCheckFailed") != 1 {
		t.Errorf("expected a PublicAccessCheckFailed warning, got %+v", *events)
	}
}

// The website lookup, the policy read and the public-access reads happen when
// the bucket is verified, once per trust window, not on every pass.
func TestEnsure_TheTransportAndExposureChecksRunOncePerTrustWindow(t *testing.T) {
	client := newFakeS3()
	spec := &depsv1alpha1.S3Spec{Resources: []depsv1alpha1.S3BucketSpec{{Name: "receipts"}}}
	ensure := func(ledger []depsv1alpha1.ManagedResource) []depsv1alpha1.ManagedResource {
		t.Helper()
		out, err := Ensure(context.Background(), client, nil, nil, "default", "checkout-service", "uid-1", testRegion, testAccountID, spec, ledger, nil, nil)
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
		return out
	}
	ledger := ensure(nil)
	client.buckets[receiptsBucket()].policy = "" // someone removes the statement

	ledger = ensure(ledger)
	if client.buckets[receiptsBucket()].policy != "" {
		t.Fatal("a pass trusted from the ledger checked the policy")
	}

	old := metav1.NewTime(time.Now().Add(-status.TrustWindow - time.Minute))
	for i := range ledger {
		ledger[i].LastVerifiedAt = &old
	}
	ensure(ledger)
	if !strings.Contains(client.buckets[receiptsBucket()].policy, awspolicy.InsecureTransportSid) {
		t.Errorf("the statement was not restored when the bucket was verified again: %q", client.buckets[receiptsBucket()].policy)
	}
}
