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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

func consumerWithAlarms(namespace, name string, ref *depsv1alpha1.ConsumeRef) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       depsv1alpha1.AppDependenciesSpec{Alarms: &depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: ref}},
	}
}

func TestAlarmConsumerToTopicProducer(t *testing.T) {
	topic := &depsv1alpha1.ConsumeRef{Namespace: "data", Name: "platform", ResourceName: "events"}
	own := &depsv1alpha1.ConsumeRef{Namespace: "shop", Name: "checkout", ResourceName: "events"}
	mapFn := AlarmConsumerToTopicProducer()

	for name, tc := range map[string]struct {
		obj  *depsv1alpha1.AppDependencies
		want []types.NamespacedName
	}{
		"alarms notifying another CR's topic wake that CR": {consumerWithAlarms("shop", "checkout", topic), []types.NamespacedName{{Namespace: "data", Name: "platform"}}},
		"a CR's own topic is not a cross-CR wake":          {consumerWithAlarms("shop", "checkout", own), nil},
		"alarms with no topic":                             {consumerWithAlarms("shop", "checkout", nil), nil},
		"no alarms at all":                                 {&depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "checkout"}}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got := mapFn(context.Background(), tc.obj)
			if len(got) != len(tc.want) {
				t.Fatalf("requests = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i].NamespacedName != tc.want[i] {
					t.Errorf("request %d = %v, want %v", i, got[i].NamespacedName, tc.want[i])
				}
			}
		})
	}
}
