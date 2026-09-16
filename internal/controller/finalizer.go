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

package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// finalizerName marks a CR as still needing AWS-side cleanup before
// Kubernetes is allowed to actually remove it.
const finalizerName = "deps.cloudctl.io/finalizer"

// ensureFinalizer adds finalizerName to cr if it isn't already present,
// persisting the change immediately. A no-op if already present.
func ensureFinalizer(ctx context.Context, c client.Client, cr *depsv1alpha1.AppDependencies) error {
	if controllerutil.ContainsFinalizer(cr, finalizerName) {
		return nil
	}
	controllerutil.AddFinalizer(cr, finalizerName)
	return c.Update(ctx, cr)
}

// removeFinalizer removes finalizerName from cr, persisting the change
// immediately. A no-op if already absent.
func removeFinalizer(ctx context.Context, c client.Client, cr *depsv1alpha1.AppDependencies) error {
	if !controllerutil.ContainsFinalizer(cr, finalizerName) {
		return nil
	}
	controllerutil.RemoveFinalizer(cr, finalizerName)
	return c.Update(ctx, cr)
}

// hasFinalizer reports whether cr still carries our finalizer.
func hasFinalizer(cr *depsv1alpha1.AppDependencies) bool {
	return controllerutil.ContainsFinalizer(cr, finalizerName)
}
