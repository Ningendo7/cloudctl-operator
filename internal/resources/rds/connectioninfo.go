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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// ConnectionInfo is what a workload needs to connect to one RDS instance.
// Deliberately resolved live from AWS on every call rather than cached in
// the ownership ledger - the ledger only ever persists an ARN, the same
// rule every other resource type's own connection-info delivery already
// follows (e.g. configmap.go re-derives an SQS queue URL from its ARN
// rather than storing the URL itself). CredentialsSecretARN is empty until
// AWS has finished provisioning the managed master-user secret, which can
// lag behind the instance itself first becoming available.
type ConnectionInfo struct {
	Host                 string
	Port                 int32
	Engine               string
	DBInstanceIdentifier string
	CredentialsSecretARN string
}

// ResolveConnectionInfo looks up the live endpoint/engine/credentials-
// secret details for the instance identified by instanceARN (as stored in
// the ownership ledger). ok is false - with a nil error - both when the
// instance no longer exists and when it exists but AWS hasn't assigned it
// an endpoint yet (e.g. still creating); callers already treat an
// unresolved forward reference as something that self-resolves on a later
// pass, and this is the same tolerance applied to "not ready yet" instead
// of "doesn't exist."
func ResolveConnectionInfo(ctx context.Context, awsClient rdsAPI, instanceARN string) (info ConnectionInfo, ok bool, err error) {
	instanceID, err := instanceIDFromARN(instanceARN)
	if err != nil {
		return ConnectionInfo{}, false, err
	}

	out, err := awsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: &instanceID})
	if err != nil {
		var notFound *types.DBInstanceNotFoundFault
		if errors.As(err, &notFound) {
			return ConnectionInfo{}, false, nil
		}
		return ConnectionInfo{}, false, wrapAWSError(err, "resolving RDS connection info")
	}
	if len(out.DBInstances) == 0 {
		return ConnectionInfo{}, false, nil
	}

	instance := out.DBInstances[0]
	if instance.Endpoint == nil || instance.Endpoint.Address == nil {
		return ConnectionInfo{}, false, nil
	}

	info = ConnectionInfo{
		Host:                 aws.ToString(instance.Endpoint.Address),
		Port:                 aws.ToInt32(instance.Endpoint.Port),
		Engine:               aws.ToString(instance.Engine),
		DBInstanceIdentifier: aws.ToString(instance.DBInstanceIdentifier),
	}
	if instance.MasterUserSecret != nil {
		info.CredentialsSecretARN = aws.ToString(instance.MasterUserSecret.SecretArn)
	}
	return info, true, nil
}

// IsConsumerAuthorized reports whether producer's declared rds resource
// named resourceName lists namespace/name in its sharedWith - the same
// network-authorization check EnsureSecurityGroup's ingress rule already
// applies, reused here so connection-info delivery (the ConfigMap/Secret
// a consumer reads) can never expose more than the security group itself
// would actually let that consumer reach.
func IsConsumerAuthorized(producer *depsv1alpha1.AppDependencies, resourceName, namespace, name string) bool {
	if producer.Spec.RDS == nil {
		return false
	}
	for _, r := range producer.Spec.RDS.Resources {
		if r.Name != resourceName {
			continue
		}
		for _, sw := range r.SharedWith {
			if sw.Namespace == namespace && sw.Name == name {
				return true
			}
		}
		return false
	}
	return false
}
