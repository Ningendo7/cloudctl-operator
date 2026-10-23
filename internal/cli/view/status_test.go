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

package view_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/clitest"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/view"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

var target = kube.Target{Context: "prod-eks", Namespace: "checkout"}

const day = 24 * time.Hour

func TestStatus_AReadyCRThatIsUpToDate(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service",
		clitest.Condition("Ready", metav1.ConditionTrue, "AllSectionsReady", "all declared sections are ready", 12, 4*day),
		clitest.Condition("SQSReady", metav1.ConditionTrue, "Reconciled", "reconciled successfully", 12, 4*day),
	)

	v := view.Status(cr, target, clitest.Now)

	if v.SchemaVersion != view.SchemaVersion || v.Kind != "Status" {
		t.Errorf("schemaVersion/kind = %q/%q, want %q/Status", v.SchemaVersion, v.Kind, view.SchemaVersion)
	}
	if v.Context != "prod-eks" || v.Namespace != "checkout" || v.Name != "checkout-service" {
		t.Errorf("identity = %q %q %q", v.Context, v.Namespace, v.Name)
	}
	if !v.Ready || !v.UpToDate || v.Deleting {
		t.Errorf("Ready=%v UpToDate=%v Deleting=%v, want true true false", v.Ready, v.UpToDate, v.Deleting)
	}
	if v.Generation != 12 || v.ObservedGeneration != 12 {
		t.Errorf("generations = %d/%d, want 12/12", v.Generation, v.ObservedGeneration)
	}
	if v.ReadyReason != "AllSectionsReady" || v.ReadyMessage != "all declared sections are ready" {
		t.Errorf("ready reason/message = %q/%q", v.ReadyReason, v.ReadyMessage)
	}
	if len(v.Sections) != 1 || v.Sections[0].Type != "SQSReady" || !v.Sections[0].Ready {
		t.Fatalf("sections = %+v, want one ready SQSReady", v.Sections)
	}
	if got := v.Sections[0]; got.Kind != "Info" || got.Reason != "Reconciled" || !got.Since.Equal(clitest.Now.Add(-4*day)) {
		t.Errorf("section = %+v, want Info, Reconciled, since four days ago", got)
	}
}

func TestStatus_ANotReadyCRCarriesTheFirstFailingReason(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service",
		clitest.Condition("Ready", metav1.ConditionFalse, "AdoptRequired", "S3Ready: bucket x exists but is not tagged", 12, 12*time.Minute),
		clitest.Condition("S3Ready", metav1.ConditionFalse, "AdoptRequired", "bucket x exists but is not tagged", 12, 12*time.Minute),
	)

	v := view.Status(cr, target, clitest.Now)

	if v.Ready {
		t.Error("Ready should be false")
	}
	if v.ReadyReason != "AdoptRequired" || v.ReadyMessage != "S3Ready: bucket x exists but is not tagged" {
		t.Errorf("ready reason/message = %q/%q, want the Ready condition's own", v.ReadyReason, v.ReadyMessage)
	}
	s := v.Sections[0]
	if s.Ready || s.Kind != "Failing" || s.Meaning != "A resource at the needed name exists and is not tagged as owned by anyone." ||
		s.Action != "Set adopt: true on the entry to bring it under management." {
		t.Errorf("section = %+v, want the catalogue's text for AdoptRequired", s)
	}
}

func TestStatus_AStaleGenerationIsBehind(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service", clitest.Generation(13),
		clitest.Condition("Ready", metav1.ConditionTrue, "AllSectionsReady", "ok", 12, day),
	)

	v := view.Status(cr, target, clitest.Now)

	if v.UpToDate || v.Generation != 13 || v.ObservedGeneration != 12 {
		t.Errorf("UpToDate=%v generation=%d observed=%d, want behind (13 vs 12)", v.UpToDate, v.Generation, v.ObservedGeneration)
	}
}

func TestStatus_NoReadyConditionMeansNoStatusYet(t *testing.T) {
	v := view.Status(clitest.CR("checkout", "checkout-service"), target, clitest.Now)

	if v.Ready || v.ReadyReason != "NoStatusYet" || v.ReadyMessage != "the operator has not reported a status for this CR yet" {
		t.Errorf("Ready=%v reason=%q message=%q", v.Ready, v.ReadyReason, v.ReadyMessage)
	}
	if len(v.Sections) != 0 {
		t.Errorf("sections = %+v, want none", v.Sections)
	}
}

func TestStatus_SectionsAndOtherConditionsAreSortedAndSeparated(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service",
		clitest.Condition("Ready", metav1.ConditionTrue, "AllSectionsReady", "ok", 12, day),
		clitest.Condition("S3Ready", metav1.ConditionTrue, "Reconciled", "ok", 12, day),
		clitest.Condition("SharedWithReferencesValid", metav1.ConditionFalse, "StaleReferencesFound", "sqs orders shares with x/y", 12, day),
		clitest.Condition("DynamoDBReady", metav1.ConditionTrue, "Reconciled", "ok", 12, day),
		clitest.Condition("ConsumeReferencesValid", metav1.ConditionTrue, "AllProducersExist", "ok", 12, day),
		clitest.Condition("SQSReady", metav1.ConditionTrue, "Reconciled", "ok", 12, day),
	)

	v := view.Status(cr, target, clitest.Now)

	var sections, other []string
	for _, s := range v.Sections {
		sections = append(sections, s.Type)
	}
	for _, s := range v.Other {
		other = append(other, s.Type)
	}
	if got := join(sections); got != "DynamoDBReady,S3Ready,SQSReady" {
		t.Errorf("sections = %s", got)
	}
	if got := join(other); got != "ConsumeReferencesValid,SharedWithReferencesValid" {
		t.Errorf("other conditions = %s", got)
	}
	if !v.Ready {
		t.Error("an other condition that is False must not make the CR not Ready")
	}
}

func TestStatus_DeletingIsReported(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service", clitest.Deleting(),
		clitest.Condition("Ready", metav1.ConditionFalse, "DeletionBlocked", "queue q still has messages", 12, time.Minute),
		clitest.Condition("DeletionBlocked", metav1.ConditionTrue, "ResourcesNotDrained", "queue q still has messages", 12, time.Minute),
	)

	v := view.Status(cr, target, clitest.Now)

	if !v.Deleting || v.Ready {
		t.Errorf("Deleting=%v Ready=%v, want true false", v.Deleting, v.Ready)
	}
	if len(v.Other) != 1 || v.Other[0].Type != "DeletionBlocked" {
		t.Errorf("other = %+v, want DeletionBlocked", v.Other)
	}
}

// Every reason the operator can report must be explained by the CLI, from the
// catalogue and nowhere else.
func TestStatus_EveryCatalogueReasonIsExplained(t *testing.T) {
	for _, r := range reasons.All() {
		name := r.Name
		if r.Prefix {
			name += "Example"
		}
		t.Run(name, func(t *testing.T) {
			cr := clitest.CR("checkout", "checkout-service",
				clitest.Condition("Ready", metav1.ConditionFalse, name, "m", 12, time.Minute),
				clitest.Condition("S3Ready", metav1.ConditionFalse, name, "m", 12, time.Minute),
			)

			s := view.Status(cr, target, clitest.Now).Sections[0]

			if s.Kind != string(r.Kind) || s.Meaning != r.Meaning || s.Action != r.Action {
				t.Errorf("section = %+v, want kind %s, meaning %q, action %q", s, r.Kind, r.Meaning, r.Action)
			}
		})
	}
}

func TestStatus_AnUnknownReasonIsShownRawNotHidden(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service",
		clitest.Condition("Ready", metav1.ConditionFalse, "SomethingNewer", "m", 12, time.Minute),
		clitest.Condition("S3Ready", metav1.ConditionFalse, "SomethingNewer", "from a newer operator", 12, time.Minute),
	)

	s := view.Status(cr, target, clitest.Now).Sections[0]

	if s.Reason != "SomethingNewer" || s.Message != "from a newer operator" || s.Kind != "Unknown" || s.Meaning != "" || s.Action != "" {
		t.Errorf("section = %+v, want the reason and message kept, Kind Unknown, no meaning or action", s)
	}
}

func TestStatus_TheLedgerIsSummarised(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service",
		clitest.Condition("Ready", metav1.ConditionTrue, "AllSectionsReady", "ok", 12, day),
		clitest.Entry(clitest.Res("sqs", "orders", func(e *depsv1alpha1.ManagedResource) { e.LastVerifiedAt = clitest.Ago(3 * time.Minute) })),
		clitest.Entry(clitest.Res("s3", "receipts", func(e *depsv1alpha1.ManagedResource) { e.LastVerifiedAt = clitest.Ago(9 * time.Minute) })),
		clitest.Entry(clitest.Res("rds", "orders-db", func(e *depsv1alpha1.ManagedResource) {
			e.State = depsv1alpha1.ManagedResourceStateCreating
		})),
		clitest.Entry(clitest.Res("kms", "shared", func(e *depsv1alpha1.ManagedResource) {
			e.State = depsv1alpha1.ManagedResourceStateRelinquished
			e.HeldBy = []string{"sqs/orders", "s3/receipts"}
		})),
		clitest.Entry(clitest.Res("dynamodb", "sessions", func(e *depsv1alpha1.ManagedResource) {
			e.PendingDeletionSince = clitest.Ago(3 * day)
		})),
	)

	l := view.Status(cr, target, clitest.Now).Ledger

	if l.Total != 5 || l.ByState["Verified"] != 3 || l.ByState["Creating"] != 1 || l.ByState["Relinquished"] != 1 {
		t.Errorf("ledger = %+v, want 5 entries: 3 Verified, 1 Creating, 1 Relinquished", l)
	}
	var attention []string
	for _, r := range l.Attention {
		attention = append(attention, r.Type+"/"+r.Name)
	}
	if got := join(attention); got != "dynamodb/sessions,kms/shared,rds/orders-db" {
		t.Errorf("attention = %s, want the non-Verified or pending-deletion entries by type then name", got)
	}
}

func TestStatus_LastVerifiedIsTheNewestInTheLedger(t *testing.T) {
	cr := clitest.CR("checkout", "checkout-service",
		clitest.Entry(clitest.Res("sqs", "orders", func(e *depsv1alpha1.ManagedResource) { e.LastVerifiedAt = clitest.Ago(9 * time.Minute) })),
		clitest.Entry(clitest.Res("s3", "receipts", func(e *depsv1alpha1.ManagedResource) { e.LastVerifiedAt = clitest.Ago(3 * time.Minute) })),
		clitest.Entry(clitest.Res("sns", "events")),
	)

	got := view.Status(cr, target, clitest.Now).LastVerifiedAt

	if got == nil || !got.Equal(clitest.Now.Add(-3*time.Minute)) {
		t.Errorf("LastVerifiedAt = %v, want three minutes ago", got)
	}
	if view.Status(clitest.CR("checkout", "x"), target, clitest.Now).LastVerifiedAt != nil {
		t.Error("with no verified entry LastVerifiedAt should be nil")
	}
}

func join(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}
