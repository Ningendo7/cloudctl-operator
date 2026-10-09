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

// Package configmap generates the per-CR ConfigMap that delivers resource
// identifiers (queue URLs, topic/table/bucket names) to the workload,
// mirroring both this CR's own owned resources and whatever it's
// authorized to consume from other CRs. See docs/architecture.md's
// "Resource identity delivery to workloads" section for the design.
package configmap

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/iam"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/rds"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// Resource-type tags this package switches on when deciding how to key and
// shape a connection value - named so the addOwned/addConsumed call sites
// and connectionKV's switch can't silently drift apart on a typo.
const (
	resourceTypeSQS      = "sqs"
	resourceTypeSNS      = "sns"
	resourceTypeDynamoDB = "dynamodb"
	resourceTypeS3       = "s3"
	resourceTypeRDS      = "rds"
)

// fieldOwner is this package's server-side-apply field manager name. The
// whole ConfigMap belongs to this operator (never a pre-existing or
// human-edited object like ServiceAccount can be), so there's no
// partial-ownership concern here - SSA is used for its simple
// create-or-replace semantics, not for field-level sharing.
const fieldOwner = client.FieldOwner("cloudctl-operator-configmap")

const configMapNameSuffix = "-connection"

// ConfigMapName returns the deterministic ConfigMap name for a CR.
func ConfigMapName(crName string) string {
	return crName + configMapNameSuffix
}

// Ensure regenerates this CR's connection ConfigMap from its current
// owned-resource ledger and authorized consumes, deleting it once there's
// nothing left to report. Refuses to touch a same-named ConfigMap this CR
// doesn't already own (checked via its owner reference) - the same
// name-match-isn't-ownership rule every AWS resource here follows.
// rdsClient is only ever touched when cr declares an rds resource or
// consumes one - resolving an instance's host/port/engine needs a live
// DescribeDBInstances call (unlike every other resource type here, which
// derives its connection value purely from the ARN already in the
// ledger), so a CR that never uses RDS can pass nil.
func Ensure(ctx context.Context, k8sClient client.Client, rdsClient cloudctlaws.RDSClient, region, accountID string, cr *depsv1alpha1.AppDependencies) error {
	data, err := buildConnectionData(ctx, k8sClient, rdsClient, region, accountID, cr)
	if err != nil {
		return err
	}

	name := ConfigMapName(cr.Name)
	existing := &corev1.ConfigMap{}
	getErr := k8sClient.Get(ctx, client.ObjectKey{Namespace: cr.Namespace, Name: name}, existing)
	exists := getErr == nil
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return getErr
	}

	if exists {
		if owner := metav1.GetControllerOf(existing); owner == nil || owner.UID != cr.UID {
			return fmt.Errorf("ConfigMap %q already exists and is not owned by this CR - refusing to manage it", name)
		}
	}

	if len(data) == 0 {
		if !exists {
			return nil
		}
		return client.IgnoreNotFound(k8sClient.Delete(ctx, existing))
	}

	gvk, err := apiutil.GVKForObject(cr, k8sClient.Scheme())
	if err != nil {
		return err
	}

	apply := applycorev1.ConfigMap(name, cr.Namespace).
		WithData(data).
		WithOwnerReferences(applymetav1.OwnerReference().
			WithAPIVersion(gvk.GroupVersion().String()).
			WithKind(gvk.Kind).
			WithName(cr.Name).
			WithUID(cr.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))

	return k8sClient.Apply(
		ctx,
		apply,
		fieldOwner,
		client.ForceOwnership,
	)
}

// buildConnectionData walks every implemented resource type's owned
// resources and consumes entries, producing one flat map ready to become
// a ConfigMap's Data.
func buildConnectionData(
	ctx context.Context,
	k8sClient client.Client,
	rdsClient cloudctlaws.RDSClient,
	region,
	accountID string,
	cr *depsv1alpha1.AppDependencies,
) (map[string]string, error) {
	data := map[string]string{}

	if cr.Spec.SQS != nil {
		for _, q := range cr.Spec.SQS.Resources {
			if err := addOwned(data, cr, resourceTypeSQS, q.Name, region, accountID); err != nil {
				return nil, err
			}
		}
		for _, ref := range cr.Spec.SQS.Consumes {
			if err := addConsumed(ctx, k8sClient, data, cr, resourceTypeSQS, ref, region, accountID); err != nil {
				return nil, err
			}
		}
	}
	if cr.Spec.SNS != nil {
		for _, t := range cr.Spec.SNS.Resources {
			if err := addOwned(data, cr, resourceTypeSNS, t.Name, region, accountID); err != nil {
				return nil, err
			}
		}
		for _, ref := range cr.Spec.SNS.Consumes {
			if err := addConsumed(ctx, k8sClient, data, cr, resourceTypeSNS, ref, region, accountID); err != nil {
				return nil, err
			}
		}
	}
	if cr.Spec.DynamoDB != nil {
		for _, tbl := range cr.Spec.DynamoDB.Resources {
			if err := addOwned(data, cr, resourceTypeDynamoDB, tbl.Name, region, accountID); err != nil {
				return nil, err
			}
		}
		for _, ref := range cr.Spec.DynamoDB.Consumes {
			if err := addConsumed(ctx, k8sClient, data, cr, resourceTypeDynamoDB, ref, region, accountID); err != nil {
				return nil, err
			}
		}
	}
	if cr.Spec.S3 != nil {
		for _, b := range cr.Spec.S3.Resources {
			if err := addOwned(data, cr, resourceTypeS3, b.Name, region, accountID); err != nil {
				return nil, err
			}
		}
		for _, ref := range cr.Spec.S3.Consumes {
			if err := addConsumed(ctx, k8sClient, data, cr, resourceTypeS3, ref, region, accountID); err != nil {
				return nil, err
			}
		}
	}
	if cr.Spec.RDS != nil {
		for _, r := range cr.Spec.RDS.Resources {
			if err := addOwnedRDS(ctx, rdsClient, data, cr, r.Name); err != nil {
				return nil, err
			}
		}
		for _, ref := range cr.Spec.RDS.Consumes {
			if err := addConsumedRDS(ctx, k8sClient, rdsClient, data, cr, ref); err != nil {
				return nil, err
			}
		}
	}

	return data, nil
}

// addOwnedRDS adds this CR's own RDS instance's connection info to data.
// Unlike addOwned/connectionKV's single-key-per-resource model, RDS needs
// several keys per instance (host, port, engine, identifier) - genuinely
// different in shape from every other resource type here, so it gets its
// own path rather than being forced into connectionKV's switch. A
// not-yet-reconciled or not-yet-ready instance (no ledger entry, or
// ResolveConnectionInfo's ok is false) is skipped silently, same as every
// other resource type's own forward-reference tolerance.
func addOwnedRDS(ctx context.Context, rdsClient cloudctlaws.RDSClient, data map[string]string, cr *depsv1alpha1.AppDependencies, resourceName string) error {
	entry := status.FindManagedResource(cr.Status.ManagedResources, resourceTypeRDS, resourceName)
	if entry == nil {
		return nil
	}
	info, ok, err := rds.ResolveConnectionInfo(ctx, rdsClient, entry.ARN)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	setRDSConnectionKeys(data, resourceName, "", info)
	return nil
}

// addConsumedRDS adds a consumed RDS instance's connection info to data,
// but only once the producer's own declared sharedWith actually
// authorizes this consumer - rds.IsConsumerAuthorized, not
// iam.ResolveConsumeARN, since RDS's sharedWith governs network access
// rather than feeding IAM policy derivation (RDS connects via a mirrored
// username/password, not an AWS API call), so IAM's own consume-
// resolution has no "rds" case to match against.
func addConsumedRDS(ctx context.Context, k8sClient client.Client, rdsClient cloudctlaws.RDSClient, data map[string]string, consumer *depsv1alpha1.AppDependencies, ref depsv1alpha1.ConsumeRef) error {
	var producer depsv1alpha1.AppDependencies
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &producer); err != nil {
		return nil // forward reference not resolved yet
	}
	if !rds.IsConsumerAuthorized(&producer, ref.ResourceName, consumer.Namespace, consumer.Name) {
		return nil
	}
	entry := status.FindManagedResource(producer.Status.ManagedResources, resourceTypeRDS, ref.ResourceName)
	if entry == nil {
		return nil
	}
	info, ok, err := rds.ResolveConnectionInfo(ctx, rdsClient, entry.ARN)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	setRDSConnectionKeys(data, ref.ResourceName, ref.Name, info)
	return nil
}

// setRDSConnectionKeys writes one RDS instance's host/port/engine/
// identifier into data, keyed the same owned-vs-consumed way every other
// resource type's connectionKV already keys its own single value.
func setRDSConnectionKeys(data map[string]string, resourceName, producerCRName string, info rds.ConnectionInfo) {
	key := func(suffix string) string {
		if producerCRName == "" {
			return envKey(resourceTypeRDS, resourceName, suffix)
		}
		return envKey(resourceTypeRDS, producerCRName, resourceName, suffix)
	}
	data[key("HOST")] = info.Host
	data[key("PORT")] = strconv.Itoa(int(info.Port))
	data[key("ENGINE")] = info.Engine
	data[key("DB_INSTANCE_IDENTIFIER")] = info.DBInstanceIdentifier
}

// addOwned adds this CR's own resource to data, keyed without a producer
// prefix (there's no ambiguity - it's this CR's own ConfigMap). A resource
// not yet in the ledger (not reconciled yet this pass, or a transient
// failure creating it) is silently skipped rather than erroring - it
// self-resolves on a later reconcile once that section catches up, same
// as collectGrants' own skip semantics in the iam package.
func addOwned(
	data map[string]string,
	cr *depsv1alpha1.AppDependencies,
	resourceType,
	resourceName,
	region,
	accountID string,
) error {
	entry := status.FindManagedResource(cr.Status.ManagedResources, resourceType, resourceName)
	if entry == nil {
		return nil
	}
	key, value, err := connectionKV(resourceType, resourceName, entry.ARN, region, accountID, "")
	if err != nil {
		return err
	}
	data[key] = value
	return nil
}

// addConsumed adds a consumed resource to data, but only if
// iam.ResolveConsumeARN confirms it's actually authorized - the same
// check IAM's own policy derivation applies, reused rather than
// duplicated so this can never expose more than IAM actually grants
// access to. Keyed with the producer CR's name folded in, since two
// different producers can each own a resource with the same logical name.
func addConsumed(
	ctx context.Context,
	k8sClient client.Client,
	data map[string]string,
	consumer *depsv1alpha1.AppDependencies,
	resourceType string,
	ref depsv1alpha1.ConsumeRef,
	region,
	accountID string,
) error {
	arn, ok := iam.ResolveConsumeARN(ctx, k8sClient, consumer, resourceType, ref)
	if !ok {
		return nil
	}
	key, value, err := connectionKV(resourceType, ref.ResourceName, arn, region, accountID, ref.Name)
	if err != nil {
		return err
	}
	data[key] = value
	return nil
}

// connectionKV derives the env-var-style key and the value a workload
// actually needs for one resource: SQS needs a queue URL (SendMessage/
// ReceiveMessage take a URL, not an ARN), the others hand back what
// they're natively identified by.
func connectionKV(resourceType, resourceName, arn, region, accountID, producerCRName string) (key, value string, err error) {
	id := arnResourceID(arn)

	var suffix string
	switch resourceType {
	case resourceTypeSQS:
		url, err := sqsQueueURL(region, accountID, id)
		if err != nil {
			return "", "", err
		}
		suffix, value = "URL", url
	case resourceTypeSNS:
		suffix, value = "ARN", arn
	case resourceTypeDynamoDB:
		suffix, value = "TABLE_NAME", id
	case resourceTypeS3:
		suffix, value = "BUCKET", id
	default:
		return "", "", fmt.Errorf("connectionKV: unknown resource type %q", resourceType)
	}

	if producerCRName == "" {
		key = envKey(resourceType, resourceName, suffix)
	} else {
		key = envKey(resourceType, producerCRName, resourceName, suffix)
	}
	return key, value, nil
}

// sqsQueueURL derives a queue URL from its name rather than calling
// GetQueueUrl again - the ARN (already in the ledger) plus this
// operator's own single configured region/account already fully
// determine it, in the standard AWS partition this project targets.
func sqsQueueURL(region, accountID, queueName string) (string, error) {
	if region == "" || accountID == "" {
		return "", fmt.Errorf("cannot derive an SQS queue URL without both region and account ID")
	}
	return fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", region, accountID, queueName), nil
}

// arnResourceID extracts the actual AWS resource name/ID from an ARN,
// covering both ARN shapes this codebase's ledgers store: a plain
// "...:resource-id" tail (SQS, SNS) and a "...:resource-type/resource-id"
// tail (DynamoDB's "table/name", and by the same split, anything similarly
// shaped in the future). S3's bucket-only ARN ("arn:aws:s3:::name") falls
// out of the same two-step split with no special-casing needed.
func arnResourceID(arn string) string {
	resource := arn
	if idx := strings.LastIndex(arn, ":"); idx != -1 {
		resource = arn[idx+1:]
	}
	if idx := strings.LastIndex(resource, "/"); idx != -1 {
		resource = resource[idx+1:]
	}
	return resource
}

var invalidEnvChars = regexp.MustCompile(`[^A-Z0-9_]`)

// envKey joins parts into an uppercase, underscore-separated key valid as
// both a ConfigMap data key and, once consumed via envFrom, an
// environment variable name - resource and CR names can contain hyphens,
// which aren't valid in either.
func envKey(parts ...string) string {
	joined := strings.ToUpper(strings.Join(parts, "_"))
	return invalidEnvChars.ReplaceAllString(joined, "_")
}
