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
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func TestDescribeDBInstanceCached_NoCacheInstalled_AlwaysCallsClient(t *testing.T) {
	client := newFakeRDS()
	client.instances["orders-db"] = &fakeInstance{status: "available"}
	ctx := context.Background() // no WithDescribeCache

	if _, err := describeDBInstanceCached(ctx, client, "orders-db"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := describeDBInstanceCached(ctx, client, "orders-db"); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := client.describeDBInstancesCalls["orders-db"]; got != 2 {
		t.Errorf("expected 2 real calls with no cache installed, got %d", got)
	}
}

func TestDescribeDBInstanceCached_CachesSuccessfulDescribe(t *testing.T) {
	client := newFakeRDS()
	client.instances["orders-db"] = &fakeInstance{status: "available"}
	ctx := WithDescribeCache(context.Background())

	if _, err := describeDBInstanceCached(ctx, client, "orders-db"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := describeDBInstanceCached(ctx, client, "orders-db"); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := client.describeDBInstancesCalls["orders-db"]; got != 1 {
		t.Errorf("expected exactly 1 real call once cached, got %d", got)
	}
}

func TestDescribeDBInstanceCached_DoesNotCacheAcrossDifferentInstances(t *testing.T) {
	client := newFakeRDS()
	client.instances["orders-db"] = &fakeInstance{status: "available"}
	client.instances["invoices-db"] = &fakeInstance{status: "available"}
	ctx := WithDescribeCache(context.Background())

	if _, err := describeDBInstanceCached(ctx, client, "orders-db"); err != nil {
		t.Fatalf("orders-db call: %v", err)
	}
	if _, err := describeDBInstanceCached(ctx, client, "invoices-db"); err != nil {
		t.Fatalf("invoices-db call: %v", err)
	}
	if got := client.describeDBInstancesCalls["orders-db"]; got != 1 {
		t.Errorf("expected 1 call for orders-db, got %d", got)
	}
	if got := client.describeDBInstancesCalls["invoices-db"]; got != 1 {
		t.Errorf("expected 1 call for invoices-db, got %d", got)
	}
}

// TestDescribeDBInstanceCached_DoesNotCacheNotFound proves the specific
// hazard this cache has to avoid: a NotFound result must never be reused
// within the same pass, since ensureInstance's own first call legitimately
// sees NotFound and then creates the instance moments later in that same
// pass - caching the miss would make a later call (resolving connection
// info for the ConfigMap/Secret) wrongly think it still doesn't exist.
func TestDescribeDBInstanceCached_DoesNotCacheNotFound(t *testing.T) {
	client := newFakeRDS()
	ctx := WithDescribeCache(context.Background())

	_, err := describeDBInstanceCached(ctx, client, "orders-db")
	var notFound *types.DBInstanceNotFoundFault
	if !errors.As(err, &notFound) {
		t.Fatalf("expected a DBInstanceNotFoundFault before the instance exists, got %v", err)
	}

	// Instance now "created" out from under the cache, same as
	// createInstance would do moments later in the same reconcile pass.
	client.instances["orders-db"] = &fakeInstance{status: "available"}

	out, err := describeDBInstanceCached(ctx, client, "orders-db")
	if err != nil {
		t.Fatalf("expected the second call to see the now-created instance, got error %v", err)
	}
	if len(out.DBInstances) == 0 {
		t.Error("expected the second call to find the instance, not a stale cached miss")
	}
	if got := client.describeDBInstancesCalls["orders-db"]; got != 2 {
		t.Errorf("expected both calls to reach the client (the first wasn't cacheable), got %d", got)
	}
}

