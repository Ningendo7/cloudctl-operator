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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// The events API rejects a note over 1 KiB and the recorder only logs the
// rejection, so the longest messages must be shortened before they are sent.
func TestEventRecorder_ShortensANoteTheAPIWouldReject(t *testing.T) {
	fake := events.NewFakeRecorder(1)
	r := &AppDependenciesReconciler{Recorder: fake}
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "checkout"}}

	eventRecorderFor(r, cr)("Warning", "Error", "SQSReady: "+strings.Repeat("a policy statement ", 200))

	got := <-fake.Events
	if len(got) > 1024 {
		t.Errorf("the recorded event is %d bytes, the API accepts at most 1024 for the note alone", len(got))
	}
	if !strings.HasSuffix(got, "… see the condition") {
		t.Errorf("a shortened event should point at the condition, got ...%q", got[len(got)-30:])
	}
}

func TestEventRecorder_LeavesAShortNoteAlone(t *testing.T) {
	fake := events.NewFakeRecorder(1)
	r := &AppDependenciesReconciler{Recorder: fake}
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "checkout"}}

	eventRecorderFor(r, cr)("Warning", "Error", "SQSReady: queue is fine")

	if got, want := <-fake.Events, "Warning Error SQSReady: queue is fine"; got != want {
		t.Errorf("event = %q, want %q", got, want)
	}
}
