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

package status

// EventRecorder reports a single lifecycle event for the resource currently
// being reconciled - e.g. a resource created, adopted, or revived from a
// scheduled deletion. Deliberately not corev1/record.EventRecorder itself,
// so resource packages (kms, s3, ...) don't need to depend on Kubernetes'
// event API just to report what they did; the controller layer adapts this
// into a real k8s Event. eventType is "Normal" or "Warning", matching
// corev1.EventTypeNormal/EventTypeWarning. A nil EventRecorder is valid and
// means "don't report events."
type EventRecorder func(eventType, reason, message string)
