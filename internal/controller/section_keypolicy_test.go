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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/alarms"
)

const (
	checkoutKeyARN = "arn:aws:kms:us-east-1:123456789012:key/checkout"
	platformKeyARN = "arn:aws:kms:us-east-1:123456789012:key/platform"
	eventsTopicARN = "arn:aws:sns:us-east-1:123456789012:platform-events"
)

var (
	shopGrant      = []depsv1alpha1.SharedWithEntry{{Namespace: "shop", Name: "checkout"}}
	eventsFromShop = depsv1alpha1.ConsumeRef{Namespace: "data", Name: "platform", ResourceName: "events"}
	keyEntry       = func(name, arn string) depsv1alpha1.ManagedResource {
		return depsv1alpha1.ManagedResource{Type: "kms", Name: name, ARN: arn}
	}
	alarmsOnEvents   = &depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: &eventsFromShop}
	checkoutQueueSQS = &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders"}}}
)

// platform owns an encrypted topic shared with shop/checkout.
func platformCR(sharedWith []depsv1alpha1.SharedWithEntry) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "data", Name: "platform"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}},
			SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{
				Name: "events", SharedWith: sharedWith, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "primary"},
			}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			keyEntry("primary", platformKeyARN),
			{Type: "sns", Name: "events", ARN: eventsTopicARN},
		}},
	}
}

func keyPolicyScheme(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).WithObjects(objs...).Build()
}

// checkoutCR consumes the platform's topic into an encrypted queue.
func checkoutCR(subscribesTo ...depsv1alpha1.ConsumeRef) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "checkout"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{{Name: "primary"}}},
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{
				Name: "inbox", SubscribesTo: subscribesTo, Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: "primary"},
			}}},
		},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{keyEntry("primary", checkoutKeyARN)}},
	}
}

func TestKeyServiceGrants_ASubscribedTopicIsGrantedToTheQueuesKey(t *testing.T) {
	k8s := keyPolicyScheme(t, platformCR(shopGrant))

	grants, err := keyServiceGrants(context.Background(), k8s, checkoutCR(eventsFromShop), "us-east-1", "123456789012")

	if err != nil {
		t.Fatalf("keyServiceGrants() error = %v", err)
	}
	if got := grants[checkoutKeyARN].SNSTopicARNs; len(got) != 1 || got[0] != eventsTopicARN {
		t.Errorf("SNS grants for the queue's key = %v, want exactly %s", got, eventsTopicARN)
	}
	if len(grants[checkoutKeyARN].AlarmARNs) != 0 {
		t.Errorf("no alarm publishes to a topic encrypted with this key, got %v", grants[checkoutKeyARN].AlarmARNs)
	}
}

func TestKeyServiceGrants_ATopicThatIsNotAuthorizedOrNotThereYetGrantsNothing(t *testing.T) {
	for name, k8s := range map[string]client.Client{
		"the owner never shared it": keyPolicyScheme(t, platformCR(nil)),
		"the owner does not exist":  keyPolicyScheme(t),
	} {
		t.Run(name, func(t *testing.T) {
			grants, err := keyServiceGrants(context.Background(), k8s, checkoutCR(eventsFromShop), "us-east-1", "123456789012")

			if err != nil {
				t.Fatalf("keyServiceGrants() error = %v", err)
			}
			if got := grants[checkoutKeyARN].SNSTopicARNs; len(got) != 0 {
				t.Errorf("no subscription can exist for this reference, yet SNS was granted %v", got)
			}
		})
	}
}

func TestKeyServiceGrants_AlarmsThatPublishToATopicAreGrantedToTheTopicsKey(t *testing.T) {
	checkout := checkoutCR()
	checkout.Spec.SQS = checkoutQueueSQS
	checkout.Spec.Alarms = alarmsOnEvents
	elsewhere := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "reports"},
		Spec:       depsv1alpha1.AppDependenciesSpec{SQS: checkoutQueueSQS, Alarms: &depsv1alpha1.AlarmsSpec{Enabled: true}}, // alarms, but no topic
	}
	platform := platformCR([]depsv1alpha1.SharedWithEntry{
		{Namespace: "shop", Name: "checkout"}, {Namespace: "shop", Name: "reports"}, {Namespace: "shop", Name: "ghost"},
	})
	k8s := keyPolicyScheme(t, checkout, elsewhere)

	grants, err := keyServiceGrants(context.Background(), k8s, platform, "us-east-1", "123456789012")

	if err != nil {
		t.Fatalf("keyServiceGrants() error = %v", err)
	}
	want := alarms.AlarmARNs("us-east-1", "123456789012", "shop", "checkout", alarmsOnEvents, checkoutQueueSQS, nil, nil)
	got := grants[platformKeyARN].AlarmARNs
	if len(want) == 0 || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("alarm grants = %v, want exactly shop/checkout's alarms %v (not reports', which publish nowhere, and not a CR that is gone)", got, want)
	}
	if len(grants[platformKeyARN].SNSTopicARNs) != 0 {
		t.Errorf("nothing delivers into a queue encrypted with the topic's key, got %v", grants[platformKeyARN].SNSTopicARNs)
	}
}

func TestKeyServiceGrants_ADeletingConsumersAlarmsAreNotGranted(t *testing.T) {
	checkout := checkoutCR()
	checkout.Spec.SQS = checkoutQueueSQS
	checkout.Spec.Alarms = alarmsOnEvents
	now := metav1.Now()
	checkout.DeletionTimestamp = &now
	checkout.Finalizers = []string{finalizerName}

	grants, err := keyServiceGrants(context.Background(), keyPolicyScheme(t, checkout), platformCR(shopGrant), "us-east-1", "123456789012")

	if err != nil {
		t.Fatalf("keyServiceGrants() error = %v", err)
	}
	if len(grants[platformKeyARN].AlarmARNs) != 0 {
		t.Errorf("a CR being deleted has its alarms removed, so the key should not keep naming them: %v", grants[platformKeyARN].AlarmARNs)
	}
}

func TestKeyServiceGrants_ATopicOwnersOwnAlarmsAreGranted(t *testing.T) {
	platform := platformCR(nil)
	platform.Spec.SQS = checkoutQueueSQS
	platform.Spec.Alarms = &depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: &depsv1alpha1.ConsumeRef{Namespace: "data", Name: "platform", ResourceName: "events"}}

	grants, err := keyServiceGrants(context.Background(), keyPolicyScheme(t), platform, "us-east-1", "123456789012")

	if err != nil || len(grants[platformKeyARN].AlarmARNs) == 0 {
		t.Errorf("grants = %v, err = %v; want the owner's own alarms", grants, err)
	}
}

// An answer that cannot be worked out must not become "nothing", which would
// take away a grant that is delivering messages right now.
func TestKeyServiceGrants_AFailedReadIsAnErrorNotAnEmptyGrant(t *testing.T) {
	k8s := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).WithObjects(checkoutCR()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("etcd request timed out")
			},
		}).Build()

	_, err := keyServiceGrants(context.Background(), k8s, platformCR(shopGrant), "us-east-1", "123456789012")

	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable {
		t.Errorf("keyServiceGrants() error = %v, want a retryable error", err)
	}
	if apierrors.IsNotFound(err) {
		t.Error("a failed read was treated as a missing CR")
	}
}

func reconcilerWithKeys(k8s client.Client, kmsClient *fakeKMSClient) *AppDependenciesReconciler {
	return &AppDependenciesReconciler{
		Client:     k8s,
		AWSClients: &cloudctlaws.Clients{KMS: kmsClient, Region: "us-east-1", AccountID: "123456789012"},
	}
}

func fakeKeys(arns ...string) *fakeKMSClient {
	kmsClient := newFakeKMSClient()
	for _, arn := range arns {
		kmsClient.keys[arn] = &fakeKey{arn: arn, keyID: arn[strings.LastIndex(arn, "/")+1:], keyState: "Enabled"}
	}
	return kmsClient
}

func TestSyncKeyPolicies_FollowsTheSubscriptionsOfTheQueuesEncryptedWithTheKey(t *testing.T) {
	kmsClient := fakeKeys(checkoutKeyARN)
	k8s := keyPolicyScheme(t, platformCR(shopGrant))
	r := reconcilerWithKeys(k8s, kmsClient)
	cr := checkoutCR(eventsFromShop)

	if err := syncKeyPolicies(context.Background(), r, cr, nil, managedKeys(cr)); err != nil {
		t.Fatalf("syncKeyPolicies() error = %v", err)
	}
	policy := kmsClient.keys[checkoutKeyARN].policy
	if !strings.Contains(policy, "cloudctl-sns-delivery") || !strings.Contains(policy, eventsTopicARN) {
		t.Fatalf("SNS should be allowed to use the key for the subscribed topic, policy: %s", policy)
	}

	if err := syncKeyPolicies(context.Background(), r, cr, nil, managedKeys(cr)); err != nil {
		t.Fatalf("second syncKeyPolicies() error = %v", err)
	}
	if kmsClient.policyWrites != 1 {
		t.Errorf("policy writes = %d after a second identical pass, want 1", kmsClient.policyWrites)
	}

	// The queue stops subscribing: the grant goes with it.
	cr.Spec.SQS.Resources[0].SubscribesTo = nil
	if err := syncKeyPolicies(context.Background(), r, cr, nil, managedKeys(cr)); err != nil {
		t.Fatalf("syncKeyPolicies() error = %v", err)
	}
	if strings.Contains(kmsClient.keys[checkoutKeyARN].policy, "cloudctl-sns-delivery") {
		t.Errorf("the SNS grant outlived the subscription: %s", kmsClient.keys[checkoutKeyARN].policy)
	}
}

func TestSyncKeyPolicies_WritesNothingWhenWhatIsNeededCannotBeWorkedOut(t *testing.T) {
	kmsClient := fakeKeys(platformKeyARN)
	platform := platformCR([]depsv1alpha1.SharedWithEntry{{Namespace: "shop", Name: "checkout"}})
	k8s := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("etcd request timed out")
			},
		}).Build()

	err := syncKeyPolicies(context.Background(), reconcilerWithKeys(k8s, kmsClient), platform, nil, managedKeys(platform))

	if err == nil {
		t.Fatal("syncKeyPolicies() should fail so the pass is retried")
	}
	if kmsClient.policyWrites != 0 {
		t.Error("a key policy was written although the consumers could not be read")
	}
}

func TestManagedKeys_LeavesOutKeysThatAreReleasedOrBeingDeleted(t *testing.T) {
	now := metav1.Now()
	cr := &depsv1alpha1.AppDependencies{Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
		{Type: "kms", Name: "live", ARN: "arn:live", State: depsv1alpha1.ManagedResourceStateVerified},
		{Type: "kms", Name: "released", ARN: "arn:released", State: depsv1alpha1.ManagedResourceStateRelinquished},
		{Type: "kms", Name: "going", ARN: "arn:going", PendingDeletionSince: &now},
		{Type: "kms", Name: "noarn"},
		{Type: "sqs", Name: "not-a-key", ARN: "arn:queue"},
	}}}

	if got := managedKeys(cr); len(got) != 1 || got[0].ARN != "arn:live" {
		t.Errorf("managedKeys() = %v, want only the live key", got)
	}
}

const staleAccountWide = `{"Version":"2012-10-17","Statement":[` +
	`{"Sid":"cloudctl-sns-delivery","Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":["kms:Decrypt"],"Resource":"*"}]}`

// A key that grants nothing is read when it is verified, once per trust
// window, not on every pass; a key that grants something is read every pass,
// so a new subscription is never waiting on the next window.
func TestSyncKeyPolicies_ReadsAKeyThatGrantsNothingOnlyWhenItIsVerified(t *testing.T) {
	kmsClient := fakeKeys(checkoutKeyARN)
	kmsClient.keys[checkoutKeyARN].policy = staleAccountWide
	cr := checkoutCR() // no subscription: nothing to grant
	r := reconcilerWithKeys(keyPolicyScheme(t), kmsClient)
	cr.Status.ManagedResources[0].LastVerifiedAt = &metav1.Time{Time: time.Now()}
	trusted := cr.DeepCopy() // the pass began with the key already verified

	if err := syncKeyPolicies(context.Background(), r, cr, trusted, managedKeys(cr)); err != nil {
		t.Fatalf("syncKeyPolicies() error = %v", err)
	}
	if kmsClient.policyReads != 0 {
		t.Errorf("a key trusted from the ledger that grants nothing was read %d time(s)", kmsClient.policyReads)
	}

	// The same key, verified during this pass: the stale statement goes.
	began := cr.DeepCopy()
	began.Status.ManagedResources[0].LastVerifiedAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	if err := syncKeyPolicies(context.Background(), r, cr, began, managedKeys(cr)); err != nil {
		t.Fatalf("syncKeyPolicies() error = %v", err)
	}
	if strings.Contains(kmsClient.keys[checkoutKeyARN].policy, "cloudctl-sns-delivery") {
		t.Errorf("the statement an earlier version wrote was not retired when the key was verified: %s", kmsClient.keys[checkoutKeyARN].policy)
	}
}

func TestSyncKeyPolicies_ReadsAKeyThatGrantsSomethingEveryPass(t *testing.T) {
	kmsClient := fakeKeys(checkoutKeyARN)
	k8s := keyPolicyScheme(t, platformCR(shopGrant))
	r := reconcilerWithKeys(k8s, kmsClient)
	cr := checkoutCR(eventsFromShop)
	cr.Status.ManagedResources[0].LastVerifiedAt = &metav1.Time{Time: time.Now()}
	trusted := cr.DeepCopy()

	for i := 0; i < 2; i++ {
		if err := syncKeyPolicies(context.Background(), r, cr, trusted, managedKeys(cr)); err != nil {
			t.Fatalf("syncKeyPolicies() error = %v", err)
		}
	}
	if kmsClient.policyReads != 2 {
		t.Errorf("policy reads = %d over two trusted passes, want one each: a subscription must not wait for the next window", kmsClient.policyReads)
	}
}
