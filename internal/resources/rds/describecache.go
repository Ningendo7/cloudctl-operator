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
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/rds"
)

type describeCacheKey struct{}

type describeResult struct {
	out *rds.DescribeDBInstancesOutput
}

// WithDescribeCache installs an empty, request-scoped cache for
// DescribeDBInstances results into ctx. Call once per reconcile pass, not
// once per resource: within one pass, the same instance's live state gets
// read up to three times today (ensureInstance's own status check, plus
// ResolveConnectionInfo for both the generated ConfigMap and the
// credentials Secret) - sharing one ctx means they share one AWS call
// instead of three. Deliberately not cached anywhere longer-lived than
// one ctx: drift detection depends on every later reconcile pass reading
// AWS state fresh, not off a stale cache.
func WithDescribeCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, describeCacheKey{}, &sync.Map{})
}

// describeDBInstanceCached returns exactly the (output, error) pair a
// direct awsClient.DescribeDBInstances call would, routed through ctx's
// cache first if one was installed - callers keep applying whatever
// error handling (e.g. distinguishing DBInstanceNotFoundFault) they
// already applied directly on the client.
//
// Only a successful describe is ever cached. ensureInstance's own first
// call in a pass can legitimately see DBInstanceNotFoundFault and then
// create the instance moments later in that same pass - caching that
// NotFound would make a later call in the same pass (ResolveConnectionInfo,
// resolving connection info for the ConfigMap/Secret) wrongly think the
// instance still doesn't exist, when it does now. An error worth sharing
// is the rarer case; a stale false negative delaying connection-info
// delivery by a full extra reconcile isn't worth risking for it.
func describeDBInstanceCached(ctx context.Context, awsClient rdsAPI, instanceID string) (*rds.DescribeDBInstancesOutput, error) {
	cache, _ := ctx.Value(describeCacheKey{}).(*sync.Map)
	if cache != nil {
		if v, ok := cache.Load(instanceID); ok {
			result := v.(describeResult)
			return result.out, nil
		}
	}

	out, err := awsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
	if cache != nil && err == nil {
		cache.Store(instanceID, describeResult{out: out})
	}
	return out, err
}
