package controller

import (
	"context"
	"fmt"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/dynamodb"
)

// dynamodbSection adapts the dynamodb package's Ensure/Cleanup to the
// orchestrator's uniform section shape. Takes the whole reconciler (not just
// AWSClients, unlike before) because encryption.kmsKeyRef resolution needs
// r.Client to look up the producer CR a shared key belongs to.
func dynamodbSection(r *AppDependenciesReconciler, original *depsv1alpha1.AppDependencies) section {
	return section{
		name: "DynamoDBReady",
		reconcile: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) error {
			declared := 0
			if cr.Spec.DynamoDB != nil {
				declared = len(cr.Spec.DynamoDB.Resources)
			}
			ctx, cancel := sectionContext(ctx, cr.Status.ManagedResources, "dynamodb", declared)
			defer cancel()

			ledger, ensureErr := dynamodb.Ensure(
				ctx, r.AWSClients.DynamoDB, r.AWSClients.KMS, r.Client, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.DynamoDB, cr.Status.ManagedResources, checkpointFor(r, cr, original), eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := dynamodb.Cleanup(
				ctx, r.AWSClients.DynamoDB, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.DynamoDB, cr.Status.ManagedResources, false, false, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(ctx, cr, "DynamoDBReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, []string, error) {
			declared := 0
			if cr.Spec.DynamoDB != nil {
				declared = len(cr.Spec.DynamoDB.Resources)
			}
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, "dynamodb", declared)
			defer cancel()

			forceDeleteAll := cr.Annotations[ForceDeleteAllAnnotation] != ""
			ledger, results, err := dynamodb.Cleanup(
				ctx, r.AWSClients.DynamoDB, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.DynamoDB, cr.Status.ManagedResources, true, forceDeleteAll, eventRecorderFor(r, cr),
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, nil, err
			}
			var blocked []string
			for _, res := range results {
				if res.Reason == dynamodb.CleanupReasonPendingDeletion || res.Reason == dynamodb.CleanupReasonStuckPendingDeletion {
					blocked = append(blocked, fmt.Sprintf("dynamodb %q: %s", res.Name, res.Reason))
				}
			}
			if len(blocked) > 0 {
				return false, blocked, nil
			}
			return true, nil, nil
		},
	}
}
