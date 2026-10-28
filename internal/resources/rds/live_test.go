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
// RDS_LIVE_TEST_KMS_KEY_ARN is optional: the ARN of an existing, enabled
// KMS key to encrypt the instance with, which also exercises the shared-key
// path. The test never changes that key's state. Without it the instance
// is encrypted with the aws/rds key, the default.
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
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
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
func waitForInstanceStatus(t *testing.T, rdsClient *rds.Client, instanceID string, want []string, requireSettled bool, timeout time.Duration) types.DBInstance {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastStatus := ""
	leftAvailable := false
	for {
		out, err := rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
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

func waitForSnapshotStatus(t *testing.T, rdsClient *rds.Client, snapshotID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastStatus := ""
	for {
		out, err := rdsClient.DescribeDBSnapshots(context.Background(), &rds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: &snapshotID})
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
//
//nolint:gocyclo // one sequential lifecycle against real AWS; each step depends on the one before it, so it cannot be split into independent tests
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

	// An existing key, if supplied, is used through kmsKeyRef by seeding it
	// into the ledger as if this CR had created it. Its state is never
	// changed here.
	keyARN := os.Getenv("RDS_LIVE_TEST_KMS_KEY_ARN")
	objects := []client.Object{grant}
	var encryption *depsv1alpha1.EncryptionSpec
	var ledger []depsv1alpha1.ManagedResource
	if keyARN != "" {
		described, err := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(keyARN)})
		if err != nil || described.KeyMetadata.KeyState != kmstypes.KeyStateEnabled {
			t.Fatalf("RDS_LIVE_TEST_KMS_KEY_ARN %q must be an existing enabled key (describe error: %v)", keyARN, err)
		}
		ledger = append(ledger, depsv1alpha1.ManagedResource{
			Type: "kms", Name: "shared", ARN: keyARN, State: depsv1alpha1.ManagedResourceStateVerified,
		})
		// Enabled and KMSKeyRef are alternatives, and KMSKeyRef alone
		// already turns on StorageEncrypted.
		encryption = &depsv1alpha1.EncryptionSpec{KMSKeyRef: "shared"}
	}
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objects...).Build()

	spec := &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{
		Name: resourceName, DBSubnetGroupName: subnetGroup,
		Engine: "postgres", EngineVersion: "16.4", InstanceClass: "db.t4g.micro", AllocatedStorage: 20,
		ApplyImmediately: true,
		DeletionPolicy:   depsv1alpha1.DeletionPolicyDelete,
		Encryption:       encryption,
	}}}

	if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
		t.Fatalf("Ensure() (create) error = %v", err)
	}
	entry, entryFound := status.FindManagedResource(ledger, resourceType, resourceName)
	if !entryFound {
		t.Fatal("expected a ledger entry after the first Ensure() call")
	}
	var consumerGroupID, secondGroupID string
	t.Cleanup(func() {
		spec.Resources[0].DeletionPolicy = depsv1alpha1.DeletionPolicyRetain // keep Cleanup from re-entering the snapshot dance during teardown
		deleteInstanceWithRetry(t, rdsClient, instanceID)
		deleteSecurityGroupsAfterInstanceGone(t, rdsClient, ec2Client, instanceID, cloudctlaws.ResourceName(namespace, crName, "rds-sg", resourceName, 255), consumerGroupID, secondGroupID)
	})

	instance := waitForInstanceStatus(t, rdsClient, instanceID, []string{"available"}, false, 20*time.Minute)
	t.Logf("instance %q reached available with endpoint %s:%d", instanceID, aws.ToString(instance.Endpoint.Address), aws.ToInt32(instance.Endpoint.Port))

	ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil)
	if err != nil {
		t.Fatalf("Ensure() (settle) error = %v", err)
	}
	entry, entryFound = status.FindManagedResource(ledger, resourceType, resourceName)
	if !entryFound || entry.State != depsv1alpha1.ManagedResourceStateVerified {
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
			t.Error("expected the real instance to be encrypted by default")
		}
		if !aws.ToBool(instance.DeletionProtection) {
			t.Error("expected the real instance to have deletion protection on")
		}
		if !aws.ToBool(instance.CopyTagsToSnapshot) {
			t.Error("expected the real instance to copy its tags to snapshots")
		}
		if aws.ToString(instance.KmsKeyId) == "" {
			t.Error("expected the real instance to report the KMS key it is encrypted with")
		}
		if gotKeyID := aws.ToString(instance.KmsKeyId); keyARN != "" && gotKeyID != keyARN {
			t.Errorf("expected the instance to be encrypted under the shared key %q, got %q", keyARN, gotKeyID)
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

	t.Run("a changed engine is refused against the real instance", func(t *testing.T) {
		spec.Resources[0].Engine = "mysql"
		_, err := Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil)
		spec.Resources[0].Engine = "postgres"
		var reconcileErr *cloudctlaws.ReconcileError
		if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reasons.ImmutableSettingChanged {
			t.Errorf("expected %s, got %v", reasons.ImmutableSettingChanged, err)
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

		spec.Resources[0].Backup = nil
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			t.Fatalf("Ensure() (backup omitted) error = %v", err)
		}
		describeOut, err = rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
		if err != nil {
			t.Fatalf("DescribeDBInstances() error = %v", err)
		}
		if got := aws.ToInt32(describeOut.DBInstances[0].BackupRetentionPeriod); got != defaultBackupRetentionDays {
			t.Errorf("omitting backup from spec must not turn backups off, retention is now %d", got)
		}
	})

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
		consumerGroupARN := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, consumerGroupID)

		secondOut, err := ec2Client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
			GroupName: aws.String(crName + "-consumer2"), VpcId: &vpcID,
			Description: aws.String("cloudctl-operator live test throwaway second consumer identity"),
		})
		if err != nil {
			t.Fatalf("CreateSecurityGroup() (second consumer) error = %v", err)
		}
		secondGroupID = aws.ToString(secondOut.GroupId)
		secondGroupARN := fmt.Sprintf("arn:aws:ec2:%s:%s:security-group/%s", region, accountID, secondGroupID)

		shared := make([]depsv1alpha1.SharedWithEntry, 0, 2)
		for i, identity := range []struct{ arn string }{{consumerGroupARN}, {secondGroupARN}} {
			consumer := &depsv1alpha1.AppDependencies{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: fmt.Sprintf("%s-consumer%d", crName, i+1)},
				// Only a consumer that asks for the instance is granted access.
				Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{
					{Namespace: namespace, Name: crName, ResourceName: resourceName},
				}}},
				Status: depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
					{Type: podNetworkIdentityResourceType, Name: podNetworkIdentityLedgerName, ARN: identity.arn},
				}},
			}
			if err := k8sClient.Create(ctx, consumer); err != nil {
				t.Fatalf("creating fake consumer CR: %v", err)
			}
			shared = append(shared, depsv1alpha1.SharedWithEntry{Namespace: namespace, Name: consumer.Name})
		}

		dedicatedGroupID := SecurityGroupIDFromARN(mustSecurityGroupARN(t, ec2Client, ctx, namespace, crName, resourceName, subnetGroup))

		spec.Resources[0].SharedWith = shared
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			t.Fatalf("Ensure() (grant) error = %v", err)
		}
		for _, id := range []string{consumerGroupID, secondGroupID} {
			if !hasIngressFrom(t, ec2Client, dedicatedGroupID, id) {
				t.Errorf("expected a real ingress rule authorizing consumer group %s", id)
			}
		}

		spec.Resources[0].SharedWith = nil
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			t.Fatalf("Ensure() (revoke) error = %v", err)
		}
		for _, id := range []string{consumerGroupID, secondGroupID} {
			if hasIngressFrom(t, ec2Client, dedicatedGroupID, id) {
				t.Errorf("expected the real ingress rule for %s to be revoked once sharedWith was cleared", id)
			}
		}
	})

	t.Run("adoption after out-of-band tag removal", func(t *testing.T) {
		// Ensure() only re-verifies ownership once the ledger entry's own
		// trust window has elapsed (status.NeedsRevalidation) - without
		// backdating it, this subtest's result would depend on how much
		// real wall-clock time happened to pass earlier in the test, the
		// same way the stuck-deletion subtest already backdates
		// PendingDeletionSince to skip its own quiet window deterministically.
		if verified, verifiedFound := status.FindManagedResource(ledger, resourceType, resourceName); verifiedFound {
			backdated := metav1.NewTime(time.Now().Add(-(status.TrustWindow + time.Minute)))
			backdatedEntry := verified
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
		pending, pendingFound := status.FindManagedResource(ledger, resourceType, resourceName)
		if !pendingFound || pending.PendingDeletionSince == nil {
			t.Fatal("expected the instance to be marked pending deletion once removed from spec")
		}
		backdated := metav1.NewTime(pending.PendingDeletionSince.Add(-(deletionQuietWindow + time.Minute)))
		backdatedEntry := pending
		backdatedEntry.PendingDeletionSince = &backdated
		status.UpsertManagedResource(&ledger, backdatedEntry)

		ledger, _, err = Cleanup(ctx, rdsClient, namespace, crName, "uid-1", nil, ledger, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() (create snapshot) error = %v", err)
		}
		snapEntry, snapEntryFound := status.FindManagedResource(ledger, snapshotResourceType, resourceName)
		if !snapEntryFound {
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
		snapEntry, snapEntryFound = status.FindManagedResource(ledger, snapshotResourceType, resourceName)
		if !snapEntryFound || snapEntry.State != depsv1alpha1.ManagedResourceStateVerified {
			t.Errorf("expected the snapshot ledger entry to reach Verified, got %+v", snapEntry)
		}

		// Restore the instance to declared, matching what the outer
		// t.Cleanup's real DeleteDBInstance call expects to find.
		spec.Resources[0].DeletionPolicy = depsv1alpha1.DeletionPolicyRetain
	})

	// Last: a storage change leaves the instance in storage-optimization,
	// which blocks some other operations for a while.
	t.Run("storage growth applies at once", func(t *testing.T) {
		spec.Resources[0].ApplyImmediately = false
		before := waitForInstanceStatus(t, rdsClient, instanceID, []string{"available"}, false, 15*time.Minute)
		t.Logf("storage type %q, iops %v, allocated %d GiB", aws.ToString(before.StorageType), before.Iops, aws.ToInt32(before.AllocatedStorage))

		// AWS rounds growth up to at least 10%.
		target := aws.ToInt32(before.AllocatedStorage) * 11 / 10
		spec.Resources[0].AllocatedStorage = target
		if ledger, err = Ensure(ctx, rdsClient, kmsClient, ec2Client, k8sClient, namespace, crName, "uid-1", region, accountID, spec, ledger, nil, nil); err != nil {
			t.Fatalf("Ensure() (grow storage) error = %v", err)
		}

		deadline := time.Now().Add(5 * time.Minute)
		for {
			out, err := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
			if err != nil {
				t.Fatalf("DescribeDBInstances() error = %v", err)
			}
			got := out.DBInstances[0]
			if aws.ToInt32(got.AllocatedStorage) >= target || (got.PendingModifiedValues != nil && aws.ToInt32(got.PendingModifiedValues.AllocatedStorage) >= target) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("storage growth to %d GiB never started, instance is at %d GiB with status %q", target, aws.ToInt32(got.AllocatedStorage), aws.ToString(got.DBInstanceStatus))
			}
			time.Sleep(10 * time.Second)
		}
	})

	t.Run("a retained instance deleted by hand is forgotten with its security group", func(t *testing.T) {
		retained, retainedFound := status.FindManagedResource(ledger, resourceType, resourceName)
		if !retainedFound {
			t.Fatal("expected the instance to still be in the ledger")
		}
		sgName := instanceSecurityGroupLedgerName(resourceName)
		if !status.HasManagedResource(ledger, podNetworkIdentityResourceType, sgName) {
			t.Fatalf("expected a ledger entry %q for the instance's security group", sgName)
		}
		retained.DeletionPolicy = depsv1alpha1.DeletionPolicyRetain
		retained.PendingDeletionSince = nil
		status.UpsertManagedResource(&ledger, retained)

		ledger, _, err = Cleanup(ctx, rdsClient, namespace, crName, "uid-1", nil, ledger, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() (retain) error = %v", err)
		}
		relinquished, _ := status.FindManagedResource(ledger, resourceType, resourceName)
		if relinquished.State != depsv1alpha1.ManagedResourceStateRelinquished {
			t.Fatalf("expected a Relinquished entry, got %+v", relinquished)
		}
		tagsOut, err := rdsClient.ListTagsForResource(ctx, &rds.ListTagsForResourceInput{ResourceName: &entry.ARN})
		if err != nil {
			t.Fatalf("ListTagsForResource() error = %v", err)
		}
		gotTags := make(map[string]string, len(tagsOut.TagList))
		for _, tg := range tagsOut.TagList {
			gotTags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		if _, owned := gotTags[cloudctlaws.OwnerTagKey]; owned || gotTags[cloudctlaws.ReleasedFromTagKey] != namespace+"/"+crName {
			t.Errorf("expected the ownership tag gone and released-from left on the retained instance, got %v", gotTags)
		}

		deleteInstanceWithRetry(t, rdsClient, instanceID)
		waitForInstanceGone(t, rdsClient, instanceID, 30*time.Minute)

		due := metav1.NewTime(time.Now().Add(-(status.RelinquishedRecheckInterval + time.Hour)))
		relinquished.LastVerifiedAt = &due
		status.UpsertManagedResource(&ledger, relinquished)
		ledger, _, err = Cleanup(ctx, rdsClient, namespace, crName, "uid-1", nil, ledger, false, nil)
		if err != nil {
			t.Fatalf("Cleanup() (recheck) error = %v", err)
		}
		if status.HasManagedResource(ledger, resourceType, resourceName) {
			t.Error("expected the entry of the deleted instance to be dropped on the recheck")
		}

		deadline := time.Now().Add(10 * time.Minute)
		for {
			ledger, err = CleanupInstanceSecurityGroups(ctx, ec2Client, namespace, crName, "uid-1", nil, ledger, false)
			if err == nil || time.Now().After(deadline) {
				break
			}
			t.Logf("security group not deletable yet: %v", err)
			time.Sleep(20 * time.Second)
		}
		if err != nil {
			t.Fatalf("CleanupInstanceSecurityGroups() error = %v", err)
		}
		if status.HasManagedResource(ledger, podNetworkIdentityResourceType, sgName) {
			t.Error("expected the security group entry to be dropped")
		}
		groups, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
			Filters: []ec2types.Filter{{Name: aws.String("group-name"), Values: []string{cloudctlaws.ResourceName(namespace, crName, "rds-sg", resourceName, 255)}}},
		})
		if err != nil {
			t.Fatalf("DescribeSecurityGroups() error = %v", err)
		}
		if len(groups.SecurityGroups) != 0 {
			t.Error("expected the instance's security group to be deleted in AWS")
		}
	})
}

// waitForInstanceGone polls until DescribeDBInstances reports the instance
// as not found.
func waitForInstanceGone(t *testing.T, rdsClient *rds.Client, instanceID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, err := rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
		var notFound *types.DBInstanceNotFoundFault
		if errors.As(err, &notFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("instance %q still exists after %s", instanceID, timeout)
		}
		time.Sleep(30 * time.Second)
	}
}

// deleteSecurityGroupsAfterInstanceGone removes the instance's dedicated
// security group and the throwaway consumer group. The instance has to be
// fully gone first, and the dedicated group before the consumer group,
// since its ingress rule references the consumer group.
func deleteSecurityGroupsAfterInstanceGone(t *testing.T, rdsClient *rds.Client, ec2Client *ec2.Client, instanceID, dedicatedGroupName string, consumerGroupIDs ...string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Minute)
	for {
		_, err := rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
		var notFound *types.DBInstanceNotFoundFault
		if errors.As(err, &notFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("instance %q still exists, delete security groups %q and %v manually afterwards", instanceID, dedicatedGroupName, consumerGroupIDs)
			return
		}
		time.Sleep(30 * time.Second)
	}

	if out, err := ec2Client.DescribeSecurityGroups(context.Background(), &ec2.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{{Name: aws.String("group-name"), Values: []string{dedicatedGroupName}}},
	}); err == nil && len(out.SecurityGroups) > 0 {
		deleteSecurityGroupWithRetry(t, ec2Client, aws.ToString(out.SecurityGroups[0].GroupId))
	}
	for _, id := range consumerGroupIDs {
		if id != "" {
			deleteSecurityGroupWithRetry(t, ec2Client, id)
		}
	}
}

// deleteSecurityGroupWithRetry retries while the group is still in use,
// which lingers briefly after its instance or referencing group goes away.
func deleteSecurityGroupWithRetry(t *testing.T, ec2Client *ec2.Client, groupID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		_, err := ec2Client.DeleteSecurityGroup(context.Background(), &ec2.DeleteSecurityGroupInput{GroupId: &groupID})
		if err == nil || strings.Contains(err.Error(), "InvalidGroup.NotFound") {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("could not delete security group %q, delete it manually: %v", groupID, err)
			return
		}
		time.Sleep(15 * time.Second)
	}
}

// deleteInstanceWithRetry keeps trying: an instance mid-modification
// rejects deletion, and a silently failed delete would leave a billing
// instance behind.
func deleteInstanceWithRetry(t *testing.T, rdsClient *rds.Client, instanceID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Minute)
	for {
		// The operator creates instances with deletion protection on.
		_, _ = rdsClient.ModifyDBInstance(context.Background(), &rds.ModifyDBInstanceInput{
			DBInstanceIdentifier: &instanceID, DeletionProtection: aws.Bool(false), ApplyImmediately: aws.Bool(true),
		})
		_, err := rdsClient.DeleteDBInstance(context.Background(), &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier: &instanceID, SkipFinalSnapshot: aws.Bool(true),
		})
		var notFound *types.DBInstanceNotFoundFault
		if err == nil || errors.As(err, &notFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("could not delete instance %q, delete it manually: %v", instanceID, err)
			return
		}
		time.Sleep(30 * time.Second)
	}
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

// TestLive_FindOrCreateSecurityGroup_TakesOverFromAPreviousUID checks a
// restored CR is refused a security group tagged with its old UID, and takes
// it over when told which.
func TestLive_FindOrCreateSecurityGroup_TakesOverFromAPreviousUID(t *testing.T) {
	cfg := skipUnlessLiveAWSCredentials(t)
	subnetGroup := liveSubnetGroupName(t)
	ctx := context.Background()
	rdsClient := rds.NewFromConfig(cfg)
	ec2Client := ec2.NewFromConfig(cfg)
	namespace, crName := "live", liveUniqueSuffix(t)
	groupName := cloudctlaws.ResourceName(namespace, crName, "rds-sg", "primary", 255)
	vpcID, err := resolveVPCID(ctx, rdsClient, subnetGroup)
	if err != nil {
		t.Fatalf("resolveVPCID() error = %v", err)
	}

	groupID, _, err := findOrCreateSecurityGroup(ctx, ec2Client, namespace, crName, "uid-1", groupName, vpcID, false)
	if err != nil {
		t.Fatalf("findOrCreateSecurityGroup() (create) error = %v", err)
	}
	t.Cleanup(func() { deleteSecurityGroupWithRetry(t, ec2Client, groupID) })

	if _, _, err := findOrCreateSecurityGroup(ctx, ec2Client, namespace, crName, "uid-2", groupName, vpcID, false); err == nil {
		t.Fatal("expected a new UID to be refused without adopt-from-uid")
	}
	takeover := cloudctlaws.WithAdoptFromUID(ctx, "uid-1")
	if _, _, err := findOrCreateSecurityGroup(takeover, ec2Client, namespace, crName, "uid-2", groupName, vpcID, false); err != nil {
		t.Fatalf("findOrCreateSecurityGroup() (takeover) error = %v", err)
	}
	out, err := ec2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{groupID}})
	if err != nil || len(out.SecurityGroups) != 1 {
		t.Fatalf("DescribeSecurityGroups() groups=%d error = %v", len(out.SecurityGroups), err)
	}
	if !cloudctlaws.IsOwnedBy(ec2TagsToMap(out.SecurityGroups[0].Tags), namespace, crName, "uid-2") {
		t.Errorf("expected the group to be tagged with the new UID, got %+v", out.SecurityGroups[0].Tags)
	}
}
