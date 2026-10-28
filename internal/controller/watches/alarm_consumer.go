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

package watches

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// AlarmConsumerToTopicProducer maps a CR whose alarms notify another CR's
// topic to that CR. The topic's key policy lists the consumer's alarms, so
// the producer has to reconcile when a consumer adds, changes or drops them,
// or the key would not let CloudWatch publish until the producer's next
// periodic pass. Update events map the old and the new object, so a consumer
// that stops naming a topic wakes the producer it named before.
func AlarmConsumerToTopicProducer() handler.MapFunc {
	return func(_ context.Context, obj client.Object) []reconcile.Request {
		consumer, ok := obj.(*depsv1alpha1.AppDependencies)
		if !ok || consumer.Spec.Alarms == nil || consumer.Spec.Alarms.SnsTopicRef == nil {
			return nil
		}
		ref := consumer.Spec.Alarms.SnsTopicRef
		if ref.Namespace == consumer.Namespace && ref.Name == consumer.Name {
			return nil // a CR's own topic is handled by its own reconcile
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}}}
	}
}
