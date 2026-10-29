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
	"fmt"
	"runtime"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// BenchmarkReconcile_SteadyState is the drift pass: every CR reconciled again
// with nothing to change. Run it with -memprofile to see where a pass
// allocates. The AWS side is the controller tests' fakes, so it covers SQS,
// SNS, KMS, IAM and alarms, not S3, DynamoDB or RDS, and the fake Kubernetes
// client's own copying shows up in the profile next to ours.
func BenchmarkReconcile_SteadyState(b *testing.B) {
	// Sampling is off while the fixtures are built, so a memory profile
	// covers only the measured passes.
	rate := runtime.MemProfileRate
	runtime.MemProfileRate = 0
	defer func() { runtime.MemProfileRate = rate }()

	const crs = 200
	objs := make([]client.Object, 0, crs)
	for i := range crs {
		objs = append(objs, &depsv1alpha1.AppDependencies{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: fmt.Sprintf("app-%d", i)},
			Spec: depsv1alpha1.AppDependenciesSpec{
				SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{
					{Name: "orders", DLQ: true, Encryption: &depsv1alpha1.EncryptionSpec{Enabled: true}},
					{Name: "jobs"},
				}},
				SNS:    &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events"}}},
				Alarms: &depsv1alpha1.AlarmsSpec{Enabled: true},
			},
		})
	}
	patches := 0
	r := newCountingReconciler(b, &patches, objs...)
	request := func(i int) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("app-%d", i%crs)}}
	}
	// Two rounds to create everything and let the trust windows settle.
	for range 2 {
		for i := range crs {
			if _, err := r.Reconcile(context.Background(), request(i)); err != nil {
				b.Fatalf("Reconcile() warm-up error = %v", err)
			}
		}
	}
	patches = 0

	runtime.MemProfileRate = rate
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, err := r.Reconcile(context.Background(), request(i)); err != nil {
			b.Fatalf("Reconcile() error = %v", err)
		}
	}
	runtime.MemProfileRate = 0
	if patches != 0 {
		b.Errorf("a steady-state pass wrote status %d times; the measurement is not of an idle pass", patches)
	}
}
