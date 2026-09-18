package controller

import (
	"context"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/resources/dynamodb"
)

// dynamodbSection adapts the dynamodb package's Ensure/Cleanup to the
// orchestrator's uniform section shape. Takes the whole reconciler (not just
// AWSClients, unlike before) because encryption.kmsKeyRef resolution needs
// r.Client to look up the producer CR a shared key belongs to.
func dynamodbSection(r *AppDependenciesReconciler) section {
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
				cr.Spec.DynamoDB, cr.Status.ManagedResources,
			)
			cr.Status.ManagedResources = ledger

			ledger, _, cleanupErr := dynamodb.Cleanup(
				ctx, r.AWSClients.DynamoDB, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.DynamoDB, cr.Status.ManagedResources, false,
			)
			cr.Status.ManagedResources = ledger

			err := ensureErr
			if err == nil {
				err = cleanupErr
			}
			setSectionCondition(cr, "DynamoDBReady", err)
			return err
		},
		finalize: func(ctx context.Context, cr *depsv1alpha1.AppDependencies) (bool, error) {
			declared := 0
			if cr.Spec.DynamoDB != nil {
				declared = len(cr.Spec.DynamoDB.Resources)
			}
			ctx, cancel := sectionDeletionContext(ctx, cr.Status.ManagedResources, "dynamodb", declared)
			defer cancel()

			ledger, results, err := dynamodb.Cleanup(
				ctx, r.AWSClients.DynamoDB, cr.Namespace, cr.Name, string(cr.UID),
				cr.Spec.DynamoDB, cr.Status.ManagedResources, true,
			)
			cr.Status.ManagedResources = ledger
			if err != nil {
				return false, err
			}
			for _, r := range results {
				if r.Reason == dynamodb.CleanupReasonPendingDeletion || r.Reason == dynamodb.CleanupReasonStuckPendingDeletion {
					return false, nil
				}
			}
			return true, nil
		},
	}
}
