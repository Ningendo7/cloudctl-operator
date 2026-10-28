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

// Package livetest holds helpers shared by the live tests.
package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	kmspkg "github.com/Ningendo7/cloudctl-operator/internal/resources/kms"
)

// SharedKeyName is the kmsKeyRef the shared key is reached through.
const SharedKeyName = "shared"

// SharedKey returns the ledger entry of one KMS key every live test reuses,
// created on the first run and found again afterwards, so the tests don't
// leave a key pending deletion each. It is never scheduled for deletion;
// schedule it by hand when the live tests are retired.
func SharedKey(t *testing.T, cfg aws.Config) depsv1alpha1.ManagedResource {
	t.Helper()
	spec := &depsv1alpha1.KMSSpec{Resources: []depsv1alpha1.KMSKeySpec{
		{Name: SharedKeyName, DeletionPolicy: depsv1alpha1.DeletionPolicyRetain, Adopt: true},
	}}
	ledger, err := kmspkg.Ensure(context.Background(), kms.NewFromConfig(cfg), "live", "shared-key", "live-shared-key", spec, nil, nil, nil)
	if err != nil {
		t.Fatalf("ensuring the shared live-test KMS key: %v", err)
	}
	for _, entry := range ledger {
		if entry.Type == "kms" && entry.Name == SharedKeyName {
			return entry
		}
	}
	t.Fatal("the shared live-test KMS key has no ledger entry")
	return depsv1alpha1.ManagedResource{}
}

// GrantServices lets AWS services use the shared key for exactly the
// resources in grants, as the operator's key policy sync does, and takes the
// grant away again when the test ends, so the next test starts from a key
// nothing may use. KMS applies a policy change eventually, so it waits a
// little before returning.
func GrantServices(t *testing.T, cfg aws.Config, key depsv1alpha1.ManagedResource, grants kmspkg.ServiceGrants) {
	t.Helper()
	client := kms.NewFromConfig(cfg)
	t.Cleanup(func() {
		if err := kmspkg.SyncServiceGrants(context.Background(), client, key.ARN, kmspkg.ServiceGrants{}, nil); err != nil {
			t.Logf("removing the service grants from the shared key: %v", err)
		}
	})
	if err := kmspkg.SyncServiceGrants(context.Background(), client, key.ARN, grants, nil); err != nil {
		t.Fatalf("granting services the shared live-test KMS key: %v", err)
	}
	time.Sleep(15 * time.Second)
}
