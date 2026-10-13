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

package rds

import (
	"context"
	"testing"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func TestResolveConnectionInfo_HappyPath(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	entry := findLedgerEntry(t, ledger, resourceType, "orders-db")
	instance := client.findByARN(entry.ARN)
	instance.endpointAddress = "orders-db.abc123.us-east-1.rds.amazonaws.com"
	instance.endpointPort = 5432
	instance.masterUserSecretARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds!db-orders-db-abcde"

	info, ok, err := ResolveConnectionInfo(context.Background(), client, entry.ARN)
	if err != nil {
		t.Fatalf("ResolveConnectionInfo() error = %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for an existing, available instance")
	}
	if info.Host != instance.endpointAddress {
		t.Errorf("Host = %q, want %q", info.Host, instance.endpointAddress)
	}
	if info.Port != 5432 {
		t.Errorf("Port = %d, want 5432", info.Port)
	}
	if info.Engine != "postgres" {
		t.Errorf("Engine = %q, want postgres", info.Engine)
	}
	if info.CredentialsSecretARN != instance.masterUserSecretARN {
		t.Errorf("CredentialsSecretARN = %q, want %q", info.CredentialsSecretARN, instance.masterUserSecretARN)
	}
}

func TestResolveConnectionInfo_NoEndpointYet_ReturnsNotOkNoError(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	entry := findLedgerEntry(t, ledger, resourceType, "orders-db")
	// No endpointAddress set - simulates an instance that's "available" per
	// status but genuinely has no Endpoint yet (defensive case; in practice
	// AWS sets both together, but nothing here should assume that).

	info, ok, err := ResolveConnectionInfo(context.Background(), client, entry.ARN)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ok {
		t.Errorf("expected ok=false without an endpoint, got info=%+v", info)
	}
}

func TestResolveConnectionInfo_InstanceGone_ReturnsNotOkNoError(t *testing.T) {
	client := newFakeRDS()
	_, ok, err := ResolveConnectionInfo(context.Background(), client, "arn:aws:rds:us-east-1:123456789012:db:does-not-exist")
	if err != nil {
		t.Fatalf("expected no error for a gone instance, got %v", err)
	}
	if ok {
		t.Error("expected ok=false for an instance that no longer exists")
	}
}

func TestResolveConnectionInfo_MalformedARN_ReturnsError(t *testing.T) {
	client := newFakeRDS()
	_, ok, err := ResolveConnectionInfo(context.Background(), client, "not-an-arn")
	if err == nil {
		t.Fatal("expected an error for a malformed ARN")
	}
	if ok {
		t.Error("expected ok=false alongside the error")
	}
}

func TestResolveConnectionInfo_PropagatesDescribeFailure(t *testing.T) {
	client := newFakeRDS()
	client.describeDBInstancesErr = &fakeAWSError{code: "ThrottlingException"}
	_, _, err := ResolveConnectionInfo(context.Background(), client, "arn:aws:rds:us-east-1:123456789012:db:orders-db")
	if err == nil {
		t.Fatal("expected DescribeDBInstances' failure to propagate")
	}
}

func TestResolveConnectionInfo_NoCredentialsSecretYet(t *testing.T) {
	client := newFakeRDS()
	ledger := setupInstance(t, client, "default", "checkout-service", "orders-db", depsv1alpha1.DeletionPolicyRetain)
	entry := findLedgerEntry(t, ledger, resourceType, "orders-db")
	instance := client.findByARN(entry.ARN)
	instance.endpointAddress = "orders-db.abc123.us-east-1.rds.amazonaws.com"
	instance.endpointPort = 5432
	// masterUserSecretARN deliberately left unset - AWS hasn't finished
	// provisioning it yet, a real lag independent of the instance itself
	// becoming available.

	info, ok, err := ResolveConnectionInfo(context.Background(), client, entry.ARN)
	if err != nil {
		t.Fatalf("ResolveConnectionInfo() error = %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true - the instance itself is ready even if the secret isn't")
	}
	if info.CredentialsSecretARN != "" {
		t.Errorf("expected an empty CredentialsSecretARN, got %q", info.CredentialsSecretARN)
	}
}

func findLedgerEntry(t *testing.T, ledger []depsv1alpha1.ManagedResource, resourceType, name string) depsv1alpha1.ManagedResource {
	t.Helper()
	for _, e := range ledger {
		if e.Type == resourceType && e.Name == name {
			return e
		}
	}
	t.Fatalf("no ledger entry found for %s/%s", resourceType, name)
	return depsv1alpha1.ManagedResource{}
}

func TestIsConsumerAuthorized(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{
		Spec: depsv1alpha1.AppDependenciesSpec{RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{
			{Name: "orders-db", SharedWith: []depsv1alpha1.SharedWithEntry{
				{Namespace: "fulfillment", Name: "fulfillment-service"},
			}},
		}}},
	}

	tests := []struct {
		name         string
		resourceName string
		namespace    string
		consumer     string
		want         bool
	}{
		{name: "authorized consumer", resourceName: "orders-db", namespace: "fulfillment", consumer: "fulfillment-service", want: true},
		{name: "unauthorized consumer", resourceName: "orders-db", namespace: "fulfillment", consumer: "other-service", want: false},
		{name: "unknown resource name", resourceName: "invoices-db", namespace: "fulfillment", consumer: "fulfillment-service", want: false},
		{name: "right namespace wrong name", resourceName: "orders-db", namespace: "fulfillment", consumer: "wrong-name", want: false},
		{name: "right name wrong namespace", resourceName: "orders-db", namespace: "wrong-ns", consumer: "fulfillment-service", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsConsumerAuthorized(producer, tc.resourceName, tc.namespace, tc.consumer); got != tc.want {
				t.Errorf("IsConsumerAuthorized() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsConsumerAuthorized_NilRDSSpec(t *testing.T) {
	producer := &depsv1alpha1.AppDependencies{}
	if IsConsumerAuthorized(producer, "orders-db", "fulfillment", "fulfillment-service") {
		t.Error("expected false when the producer declares no RDS spec at all")
	}
}
