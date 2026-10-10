//go:build live

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

// Live tests run against a real AWS account - no LocalStack (RDS isn't
// emulated in LocalStack's community edition at all, confirmed directly
// against the exact image this project's integration tier already pins -
// not an assumption).
//
// Needs a pre-existing DB subnet group - this operator never creates one
// itself (reference-only, by design), so this test can't either. Set
// RDS_LIVE_TEST_SUBNET_GROUP to a real subnet group name in the account
// these credentials resolve to; the test skips cleanly if it's unset.
//
// Run explicitly with whatever already authenticates your AWS CLI:
//
//	RDS_LIVE_TEST_SUBNET_GROUP=my-subnet-group go test -tags=live ./internal/resources/rds/... -v -timeout 30m
package rds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

func skipUnlessLiveAWSCredentials(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Skipf("no AWS config available, skipping live test: %v", err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}); err != nil {
		t.Skipf("no live AWS credentials available, skipping live test: %v", err)
	}
	return cfg
}

func liveSubnetGroupName(t *testing.T) string {
	t.Helper()
	name := os.Getenv("RDS_LIVE_TEST_SUBNET_GROUP")
	if name == "" {
		t.Skip("RDS_LIVE_TEST_SUBNET_GROUP not set, skipping live RDS test")
	}
	return name
}

// liveUniqueSuffix produces a valid RDS/Kubernetes-style name (lowercase,
// hyphens only) - unlike KMS aliases, which tolerate underscores, RDS
// instance identifiers reject them outright, and a real CR name could
// never contain one anyway (the apiserver enforces DNS-1123 on it).
func liveUniqueSuffix(t *testing.T) string {
	raw := strings.ToLower(t.Name()[len("TestLive_"):])
	raw = strings.ReplaceAll(raw, "_", "-")
	return "live-" + raw + "-" + time.Now().UTC().Format("150405")
}

// waitForInstanceStatus polls DescribeDBInstances until status reaches
// one of want, logging every distinct status seen along the way. RDS's
// status enum is much larger and less predictable than most AWS
// resources this operator manages - a live run is the one chance to
// notice a transitional status the unit tier's fakes never produce.
// waitForInstanceStatus polls until the instance reaches one of want.
// requireSettled additionally requires having actually observed the
// instance leave "available" at least once before accepting it back -
// needed only right after the caller itself just issued a ModifyDBInstance,
// where AWS can keep reporting "available" for a few seconds before the
// status even transitions to "modifying". Not safe to apply
// unconditionally: the initial post-create wait has no prior "available"
// to have left, so it would never be satisfied.
func waitForInstanceStatus(t *testing.T, client *rds.Client, instanceID string, want []string, requireSettled bool, timeout time.Duration) types.DBInstance {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastStatus := ""
	leftAvailable := false
	for {
		out, err := client.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
		if err != nil {
			t.Fatalf("DescribeDBInstances(%q) error = %v", instanceID, err)
		}
		if len(out.DBInstances) == 0 {
			t.Fatalf("instance %q disappeared while waiting", instanceID)
		}
		instance := out.DBInstances[0]
		current := aws.ToString(instance.DBInstanceStatus)
		if current != lastStatus {
			t.Logf("instance %q status: %s", instanceID, current)
			lastStatus = current
		}
		if current != "available" {
			leftAvailable = true
		}
		for _, w := range want {
			if current == w && (!requireSettled || w != "available" || leftAvailable) {
				return instance
			}
		}
		if current == "failed" {
			t.Fatalf("instance %q entered status %q while waiting for %v", instanceID, current, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for instance %q to reach %v, last status %q", instanceID, want, current)
		}
		time.Sleep(10 * time.Second)
	}
}

func waitForSnapshotStatus(t *testing.T, client *rds.Client, snapshotID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastStatus := ""
	for {
		out, err := client.DescribeDBSnapshots(context.Background(), &rds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: &snapshotID})
		if err != nil {
			t.Fatalf("DescribeDBSnapshots(%q) error = %v", snapshotID, err)
		}
		if len(out.DBSnapshots) == 0 {
			t.Fatalf("snapshot %q disappeared while waiting", snapshotID)
		}
		current := aws.ToString(out.DBSnapshots[0].Status)
		if current != lastStatus {
			t.Logf("snapshot %q status: %s", snapshotID, current)
			lastStatus = current
		}
		if current == want {
			return
		}
		if current == "failed" {
			t.Fatalf("snapshot %q entered status %q while waiting for %q", snapshotID, current, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for snapshot %q to reach %q, last status %q", snapshotID, want, current)
		}
		time.Sleep(10 * time.Second)
	}
}

// TestLive_Ensure_FullInstanceLifecycle is the main live test: one real
// instance, checked against everything a fake or an assumption-from-docs
// could plausibly have gotten wrong - credentials delivery, ownership
// tagging, the shared-key (kmsKeyRef) encryption path, attribute drift
// correction, ingress grant/revoke, adoption, credential rotation, and
// the proactive snapshot-before-stuck-deletion safety net.
func TestLive_Ensure_FullInstanceLifecycle(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	subnetGroup := liveSubnetGroupName(t)
	ctx := context.Background()

	rdsClient := rds.NewFromConfig(cfg)
	ec2Client := ec2.NewFromConfig(cfg)
	kmsClient := kms.NewFromConfig(cfg)
	smClient := secretsmanager.NewFromConfig(cfg)

	callerID, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	accountID := aws.ToString(callerID.Account)
	region := cfg.Region

	namespace, crName, resourceName := "live", liveUniqueSuffix(t), "primary"
	instanceID := cloudctlaws.ResourceName(namespace, crName, resourceType, resourceName, 63)
	grant := &depsv1alpha1.RDSSubnetGroupGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "live-test-" + crName},
		Spec:       depsv1alpha1.RDSSubnetGroupGrantSpec{DBSubnetGroupName: subnetGroup, AllowedNamespaces: []string{namespace}},
	}

	// Reuses a pre-existing real key via the shared kmsKeyRef path rather
	// than the dedicated-key path - exercises the shared-key resolution
	// code for RDS (never otherwise covered live) and avoids paying for a
	// second real key just to prove encryption works at all.
	const reusedKeyARN = "arn:aws:kms:us-east-1:807906459006:key/223e5f35-d55d-46e3-b3a4-e47edcb8aac8"
	kmsProducerName := "kms-producer-" + crName
	kmsProducer := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: kmsProducerName},
		Spec: depsv1alpha1.AppDependenciesSpec{KMS: &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
			{Name: "shared", SharedWith: []depsv1alpha1.SharedWithEntry{{Namespace: namespace, Name: crName}}},
		}}},
		Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			{Type: "kms", Name: "shared", ARN: reusedKeyARN, State: depsv1alpha1.ManagedResourceStateVerified},
		}},
	}
	// Self-heals regardless of how the previous run's own Cleanup below left
	// this key - a failed prior run still reaches that Cleanup and
	// re-schedules deletion, which disables the key for whichever run
	// comes next. CancelKeyDeletion errors when the key isn't actually
	// pending deletion, which is the common case, so that error is expected
	// and ignored; EnableKey is a no-op when already enabled.
	_, _ = kmsClient.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{KeyId: aws.String(reusedKeyARN)})
	if _, err := kmsClient.EnableKey(ctx, &kms.EnableKeyInput{KeyId: aws.String(reusedKeyARN)}); err != nil {
		t.Fatalf("EnableKey(%q) error = %v", reusedKeyARN, err)
	}
	t.Cleanup(func() {
		windowDays := int32(7)
		_, _ = kmsClient.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
			KeyId: aws.String(reusedKeyARN), PendingWindowInDays: &windowDays,
		})
	})
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(grant, kmsProducer).Build()

	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
		Name: resourceName, DBSubnetGroupName: subnetGroup,
		Engine: "postgres", EngineVersion: "16.4", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
		DeletionPolicy: depsv1alpha1.DeletionPolicyDelete,
		// Enabled and KMSKeyRef are alternatives, not both-settable - every
		// resource type runs Enabled's dedicated-key provisioning
		// unconditionally after resolving KMSKeyRef, silently overriding it
		// if both are set. KMSKeyRef alone already turns on StorageEncrypted.
		Encryption: &depsv1alpha1.EncryptionSpec{KMSKeyRef: &depsv1alpha1.ConsumeRef{
			Namespace: namespace, Name: kmsProducerName, ResourceName: "shared",
		}},
	}}}

	var ledger []depsv1alpha1.ManagedResource
	if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, nil, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}
	entry := status.FindManagedResource(ledger, resourceType, resourceName)
	if entry == nil {
		t.Fatal("expected a ledger entry after the first Ensure() call")
	}
	t.Cleanup(func() {
		spec.Resources[0].DeletionPolicy = depsv1alpha1.DeletionPolicyRetain // keep Cleanup from re-entering the snapshot dance during teardown
		_, _ = rdsClient.DeleteDBInstance(context.Background(), &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier: &instanceID, SkipFinalSnapshot: aws.Bool(true),
		})
	})

	instance := waitForInstanceStatus(t, rdsClient, instanceID, []string{"available"}, false, 20*time.Minute)
	t.Logf("instance %q reached available with endpoint %s:%d", instanceID, aws.ToString(instance.Endpoint.Address), aws.ToInt32(instance.Endpoint.Port))

	ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() (settle) error = %v", err)
	}
	entry = status.FindManagedResource(ledger, resourceType, resourceName)
	if entry == nil || entry.State != depsv1alpha1.ManagedResourceStateVerified {
		t.Fatalf("expected a Verified ledger entry once available, got %+v", entry)
	}

	t.Run("tags and shape", func(t *testing.T) {
		tagsOut, err := rdsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: &entry.ARN})
		if err != nil {
			t.Fatalf("ListTagsForResource() error = %v", err)
		}
		tags := make(map[string]string, len(tagsOut.TagList))
		for _, tg := range tagsOut.TagList {
			tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		if !cloudctlaws.IsOwnedBy(tags, namespace, crName, "uid-1") {
			t.Errorf("real instance tags don't satisfy IsOwnedBy: %+v", tags)
		}
		if !aws.ToBool(instance.StorageEncrypted) {
			t.Error("expected the real instance to be encrypted")
		}
		if gotKeyID := aws.ToString(instance.KmsKeyId); gotKeyID != reusedKeyARN {
			t.Errorf("expected the instance to be encrypted under the shared key %q, got %q", reusedKeyARN, gotKeyID)
		}
	})

	t.Run("managed credentials secret", func(t *testing.T) {
		info, ok, err := ResolveConnectionInfo(ctx, rdsClient, entry.ARN)
		if err != nil {
			t.Fatalf("ResolveConnectionInfo() error = %v", err)
		}
		if !ok || info.CredentialsSecretARN == "" {
			t.Fatal("expected a managed credentials secret ARN once the instance is available")
		}
		secretOut, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &info.CredentialsSecretARN})
		if err != nil {
			t.Fatalf("real GetSecretValue() error = %v", err)
		}
		var parsed struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal([]byte(aws.ToString(secretOut.SecretString)), &parsed); err != nil {
			t.Fatalf("parsing real managed secret JSON: %v", err)
		}
		if parsed.Username == "" || parsed.Password == "" {
			t.Errorf("expected a real username/password in the managed secret, got %+v", parsed)
		}
	})

	t.Run("idempotent re-reconcile", func(t *testing.T) {
		before := len(ledger)
		ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil)
		if err != nil {
			t.Fatalf("Ensure() (idempotent) error = %v", err)
		}
		if len(ledger) != before {
			t.Errorf("expected no new ledger entries from an unchanged reconcile, got %d want %d", len(ledger), before)
		}
	})

	t.Run("attribute drift correction", func(t *testing.T) {
		spec.Resources[0].Backup = &depsv1alpha1.RDSBackupSpec{Enabled: true}
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			if !isRetryableStillProvisioning(err) {
				t.Fatalf("Ensure() (modify backup retention) error = %v", err)
			}
		}
		waitForInstanceStatus(t, rdsClient, instanceID, []string{"available"}, true, 10*time.Minute)
		describeOut, err := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
		if err != nil {
			t.Fatalf("DescribeDBInstances() error = %v", err)
		}
		if aws.ToInt32(describeOut.DBInstances[0].BackupRetentionPeriod) != defaultBackupRetentionDays {
			t.Errorf("expected ModifyDBInstance to have actually applied the new backup retention, got %d", aws.ToInt32(describeOut.DBInstances[0].BackupRetentionPeriod))
		}
	})

	var consumerGroupID string
	t.Run("ingress grant and revoke", func(t *testing.T) {
		vpcID, err := resolveVPCID(ctx, rdsClient, subnetGroup)
		if err != nil {
			t.Fatalf("resolveVPCID() error = %v", err)
		}
		consumerOut, err := ec2Client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
			GroupName: aws.String(crName + "-consumer"), VpcId: &vpcID,
			Description: aws.String("cloudctl-operator live test throwaway consumer identity"),
		})
		if err != nil {
			t.Fatalf("CreateSecurityGroup() (consumer) error = %v", err)
		}
		consumerGroupID = aws.ToString(consumerOut.GroupId)
		t.Cleanup(func() {
			_, _ = ec2Client.DeleteSecurityGroup(context.Background(), &ec2.DeleteSecurityGroupInput{GroupId: &consumerGroupID})
		})
		consumerGroupARN := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, consumerGroupID)

		consumer := &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: crName + "-consumer"},
			Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
				{Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName, ARN: consumerGroupARN},
			}},
		}
		if err := k8sClient.Create(ctx, consumer); err != nil {
			t.Fatalf("creating fake consumer CR: %v", err)
		}

		dedicatedGroupID := SecurityGroupIDFromARN(mustSecurityGroupARN(t, ec2Client, ctx, namespace, crName, resourceName, subnetGroup))

		spec.Resources[0].SharedWith = []depsv1alpha1.SharedWithEntry{{Namespace: namespace, Name: consumer.Name}}
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			t.Fatalf("Ensure() (grant) error = %v", err)
		}
		if !hasIngressFrom(t, ec2Client, dedicatedGroupID, consumerGroupID) {
			t.Error("expected a real ingress rule authorizing the consumer's security group")
		}

		spec.Resources[0].SharedWith = nil
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			t.Fatalf("Ensure() (revoke) error = %v", err)
		}
		if hasIngressFrom(t, ec2Client, dedicatedGroupID, consumerGroupID) {
			t.Error("expected the real ingress rule to be revoked once sharedWith was cleared")
		}
	})

	t.Run("adoption after out-of-band tag removal", func(t *testing.T) {
		// Ensure() only re-verifies ownership once the ledger entry's own
		// trust window has elapsed (status.NeedsRevalidation) - without
		// backdating it, this subtest's result would depend on how much
		// real wall-clock time happened to pass earlier in the test, the
		// same way the stuck-deletion subtest already backdates
		// PendingDeletionSince to skip its own quiet window deterministically.
		if verified := status.FindManagedResource(ledger, resourceType, resourceName); verified != nil {
			backdated := metav1.NewTime(time.Now().Add(-(status.TrustWindow + time.Minute)))
			backdatedEntry := *verified
			backdatedEntry.LastVerifiedAt = &backdated
			status.UpsertManagedResource(&ledger, backdatedEntry)
		}

		if _, err := rdsClient.RemoveTagsFromResource(ctx, &rds.RemoveTagsFromResourceInput{
			ResourceName: &entry.ARN,
			TagKeys:      []string{cloudctlaws.OwnerTagKey, cloudctlaws.OwnerUIDTagKey},
		}); err != nil {
			t.Fatalf("RemoveTagsFromResource() error = %v", err)
		}

		if _, err := Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err == nil {
			t.Error("expected Ensure() to refuse the now-untagged real instance without adopt:true")
		}

		spec.Resources[0].Adopt = true
		ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil)
		spec.Resources[0].Adopt = false
		if err != nil {
			t.Fatalf("Ensure() (adopt) error = %v", err)
		}
		tagsOut, err := rdsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: &entry.ARN})
		if err != nil {
			t.Fatalf("ListTagsForResource() error = %v", err)
		}
		tags := make(map[string]string, len(tagsOut.TagList))
		for _, tg := range tagsOut.TagList {
			tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		if !cloudctlaws.IsOwnedBy(tags, namespace, crName, "uid-1") {
			t.Errorf("expected the real instance to be re-tagged as owned after adoption, got %+v", tags)
		}
	})

	t.Run("credential rotation flows through", func(t *testing.T) {
		infoBefore, ok, err := ResolveConnectionInfo(ctx, rdsClient, entry.ARN)
		if err != nil || !ok {
			t.Fatalf("ResolveConnectionInfo() (before rotation) ok=%v err=%v", ok, err)
		}
		beforeOut, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &infoBefore.CredentialsSecretARN})
		if err != nil {
			t.Fatalf("GetSecretValue() (before rotation) error = %v", err)
		}

		if _, err := rdsClient.ModifyDBInstance(ctx, &rds.ModifyDBInstanceInput{
			DBInstanceIdentifier: &instanceID, RotateMasterUserPassword: aws.Bool(true), ApplyImmediately: aws.Bool(true),
		}); err != nil {
			t.Fatalf("ModifyDBInstance() (rotate) error = %v", err)
		}
		waitForInstanceStatus(t, rdsClient, instanceID, []string{"available"}, true, 10*time.Minute)

		afterOut, err := smClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &infoBefore.CredentialsSecretARN})
		if err != nil {
			t.Fatalf("GetSecretValue() (after rotation) error = %v", err)
		}
		if aws.ToString(afterOut.SecretString) == aws.ToString(beforeOut.SecretString) {
			t.Error("expected the managed secret's content to actually change after a real rotation")
		}
	})

	var snapshotID string
	t.Run("proactive snapshot before stuck deletion", func(t *testing.T) {
		spec.Resources[0].DeletionPolicy = depsv1alpha1.DeletionPolicyDelete
		ledger, _, err = Cleanup(ctx, rdsClient, namespace, crName, "uid-1", nil, ledger, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() (mark pending) error = %v", err)
		}
		pending := status.FindManagedResource(ledger, resourceType, resourceName)
		if pending == nil || pending.PendingDeletionSince == nil {
			t.Fatal("expected the instance to be marked pending deletion once removed from spec")
		}
		backdated := metav1.NewTime(pending.PendingDeletionSince.Time.Add(-(deletionQuietWindow + time.Minute)))
		backdatedEntry := *pending
		backdatedEntry.PendingDeletionSince = &backdated
		status.UpsertManagedResource(&ledger, backdatedEntry)

		ledger, _, err = Cleanup(ctx, rdsClient, namespace, crName, "uid-1", nil, ledger, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() (create snapshot) error = %v", err)
		}
		snapEntry := status.FindManagedResource(ledger, snapshotResourceType, resourceName)
		if snapEntry == nil {
			t.Fatal("expected a real CreateDBSnapshot call to have been made")
		}
		snapshotID = snapEntry.ARN
		t.Cleanup(func() {
			_, _ = rdsClient.DeleteDBSnapshot(context.Background(), &rds.DeleteDBSnapshotInput{DBSnapshotIdentifier: &snapshotID})
		})

		waitForSnapshotStatus(t, rdsClient, snapshotID, "available", 15*time.Minute)

		ledger, _, err = Cleanup(ctx, rdsClient, namespace, crName, "uid-1", nil, ledger, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() (verify snapshot) error = %v", err)
		}
		snapEntry = status.FindManagedResource(ledger, snapshotResourceType, resourceName)
		if snapEntry == nil || snapEntry.State != depsv1alpha1.ManagedResourceStateVerified {
			t.Errorf("expected the snapshot ledger entry to reach Verified, got %+v", snapEntry)
		}

		// Restore the instance to declared, matching what the outer
		// t.Cleanup's real DeleteDBInstance call expects to find.
		spec.Resources[0].DeletionPolicy = depsv1alpha1.DeletionPolicyRetain
	})
}

// isRetryableStillProvisioning reports whether err is specifically this
// package's own "instance %q is still %s" status-switch error - the
// expected outcome right after kicking off a real modify, before polling
// for real. Checked by message, not just the Retryable flag: several
// unrelated failures (e.g. a missing RDSSubnetGroupGrant) are also
// Retryable, and conflating them with "still provisioning" would hide a
// real problem behind a misleading tolerance.
func isRetryableStillProvisioning(err error) bool {
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || !reconcileErr.Retryable || reconcileErr.Err == nil {
		return false
	}
	return strings.Contains(reconcileErr.Err.Error(), "is still ")
}

func hasIngressFrom(t *testing.T, ec2Client cloudctlaws.EC2Client, groupID, sourceGroupID string) bool {
	t.Helper()
	out, err := ec2Client.DescribeSecurityGroups(context.Background(), &ec2.DescribeSecurityGroupsInput{GroupIds: []string{groupID}})
	if err != nil {
		t.Fatalf("DescribeSecurityGroups(%q) error = %v", groupID, err)
	}
	if len(out.SecurityGroups) == 0 {
		return false
	}
	for _, perm := range out.SecurityGroups[0].IpPermissions {
		for _, pair := range perm.UserIdGroupPairs {
			if aws.ToString(pair.GroupId) == sourceGroupID {
				return true
			}
		}
	}
	return false
}

func mustSecurityGroupARN(t *testing.T, ec2Client cloudctlaws.EC2Client, ctx context.Context, namespace, crName, resourceName, subnetGroup string) string {
	t.Helper()
	groupName := cloudctlaws.ResourceName(namespace, crName, "rds-sg", resourceName, 255)
	out, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{{Name: aws.String("group-name"), Values: []string{groupName}}},
	})
	if err != nil {
		t.Fatalf("DescribeSecurityGroups(%q) error = %v", groupName, err)
	}
	if len(out.SecurityGroups) == 0 {
		t.Fatalf("expected the dedicated security group %q to already exist", groupName)
	}
	return aws.ToString(out.SecurityGroups[0].GroupId)
}

// TestLive_Ensure_ClassifiesRealInvalidParameterAsNotRetryable confirms
// this package's error classifier against one genuine AWS rejection -
// no subnet group, VPC, or long wait needed, since AWS rejects an
// instance class that doesn't exist immediately.
func TestLive_Ensure_ClassifiesRealInvalidParameterAsNotRetryable(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	rdsClient := rds.NewFromConfig(cfg)
	id := "cloudctl-live-error-shape-probe-" + time.Now().UTC().Format("150405")

	_, err := rdsClient.CreateDBInstance(context.Background(), &rds.CreateDBInstanceInput{
		DBInstanceIdentifier:     &id,
		Engine:                   aws.String("postgres"),
		EngineVersion:            aws.String("16.4"),
		DBInstanceClass:          aws.String("db.not-a-real-class"),
		MasterUsername:           aws.String(masterUsername),
		ManageMasterUserPassword: aws.Bool(true),
		AllocatedStorage:         aws.Int32(20),
	})
	if err == nil {
		t.Fatal("expected a real validation error for a nonexistent instance class")
	}
	if cloudctlaws.IsRetryable(err) {
		t.Errorf("expected a real InvalidParameterValue-shaped error to classify as non-retryable, got retryable: %v", err)
	}
}
