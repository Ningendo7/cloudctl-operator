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

package render_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/clitest"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/render"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/view"
)

var update = flag.Bool("update", false, "rewrite the golden files from the renderers' output")

// golden compares got with testdata/<name>.golden. The files are the written
// specification of the text format (clidesign.md, section 10); review a change
// to one as a change to that specification.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("%v; create it with: go test ./internal/cli/render -update", err)
	}
	if got != string(want) {
		t.Errorf("%s differs from the golden file.\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
}

func at(d time.Duration) time.Time { return clitest.Now.Add(-d) }

func ptr(d time.Duration) *time.Time { t := at(d); return &t }

const day = 24 * time.Hour

func notReadyView() view.StatusView {
	return view.StatusView{
		SchemaVersion: view.SchemaVersion, Kind: "Status",
		Context: "prod-eks", Namespace: "checkout", Name: "checkout-service",
		Generation: 12, ObservedGeneration: 12, UpToDate: true, Ready: false,
		ReadyReason: "AdoptRequired", ReadyMessage: "S3Ready: bucket exists but is not tagged",
		LastVerifiedAt: ptr(3 * time.Minute),
		Sections: []view.SectionView{
			{Type: "RDSReady", Reason: "InstanceCreating", Message: `instance "orders-db" is still creating`, Kind: "Waiting",
				Meaning: "AWS reports the RDS instance is not available yet, for example InstanceCreating or InstanceModifying.",
				Action:  "None; it is polled until it is available.", Since: at(2 * time.Minute)},
			{Type: "S3Ready", Reason: "AdoptRequired", Message: `bucket "receipts" exists but is not tagged as owned by this CR`, Kind: "Failing",
				Meaning: "A resource at the needed name exists and is not tagged as owned by anyone.",
				Action:  "Set adopt: true on the entry to bring it under management.", Since: at(12 * time.Minute)},
			{Type: "SQSReady", Reason: "Reconciled", Message: "reconciled successfully", Kind: "Info", Ready: true,
				Meaning: "The section reconciled successfully.", Since: at(4 * day)},
		},
		Other: []view.SectionView{
			{Type: "SharedWithReferencesValid", Reason: "StaleReferencesFound", Message: "sqs orders shares with other/consumer, which does not exist",
				Kind: "Waiting", Since: at(time.Hour)},
		},
		Ledger: view.LedgerSummary{
			Total:   7,
			ByState: map[string]int{"Verified": 5, "Creating": 1, "Relinquished": 1},
			Attention: []view.ResourceRow{
				{Type: "dynamodb", Name: "sessions", State: "Verified", PendingDeletionSince: ptr(3 * day)},
				{Type: "kms", Name: "shared", State: "Relinquished", HeldBy: []string{"sqs/orders", "s3/receipts"}},
				{Type: "rds", Name: "orders-db", State: "Creating"},
			},
		},
	}
}

func readyView() view.StatusView {
	return view.StatusView{
		SchemaVersion: view.SchemaVersion, Kind: "Status",
		Context: "prod-eks", Namespace: "checkout", Name: "checkout-service",
		Generation: 12, ObservedGeneration: 12, UpToDate: true, Ready: true,
		ReadyReason: "AllSectionsReady", ReadyMessage: "all declared sections are ready",
		Sections: []view.SectionView{
			{Type: "SQSReady", Reason: "Reconciled", Message: "reconciled successfully", Kind: "Info", Ready: true, Since: at(4 * day)},
		},
		Ledger: view.LedgerSummary{Total: 2, ByState: map[string]int{"Verified": 2}},
	}
}

func render1(t *testing.T, v view.StatusView, showSource bool) string {
	t.Helper()
	var b bytes.Buffer
	if err := render.StatusText(&b, v, clitest.Now, showSource); err != nil {
		t.Fatalf("StatusText() error = %v", err)
	}
	return b.String()
}

func TestStatusText_NotReady(t *testing.T) {
	golden(t, "status_notready", render1(t, notReadyView(), false))
}

func TestStatusText_Ready(t *testing.T) { golden(t, "status_ready", render1(t, readyView(), false)) }

func TestStatusText_ShowSource(t *testing.T) {
	golden(t, "status_ready_source", render1(t, readyView(), true))
}

func TestStatusText_Behind(t *testing.T) {
	v := readyView()
	v.Generation, v.ObservedGeneration, v.UpToDate = 13, 12, false
	golden(t, "status_behind", render1(t, v, false))
}

func TestStatusText_Deleting(t *testing.T) {
	v := notReadyView()
	v.Deleting = true
	v.Sections, v.Other, v.Ledger = nil, nil, view.LedgerSummary{}
	v.ReadyReason = "DeletionBlocked"
	golden(t, "status_deleting", render1(t, v, false))
}

func TestStatusText_NoStatusYet(t *testing.T) {
	v := view.StatusView{
		SchemaVersion: view.SchemaVersion, Kind: "Status",
		Context: "prod-eks", Namespace: "checkout", Name: "checkout-service",
		Generation: 1, ReadyReason: "NoStatusYet", ReadyMessage: "the operator has not reported a status for this CR yet",
	}
	golden(t, "status_nostatus", render1(t, v, false))
}

func TestStatusText_AnUnknownReasonShowsNoMeaningOrAction(t *testing.T) {
	v := readyView()
	v.Ready = false
	v.Sections = []view.SectionView{{Type: "S3Ready", Reason: "SomethingNewer", Message: "from a newer operator", Kind: "Unknown", Since: at(time.Minute)}}
	got := render1(t, v, false)

	if !strings.Contains(got, "S3Ready: SomethingNewer  (Unknown)\n  from a newer operator\n") {
		t.Errorf("missing the raw reason block:\n%s", got)
	}
	if strings.Contains(got, "Meaning:") || strings.Contains(got, "Do:") {
		t.Errorf("an unknown reason has no meaning or action to print:\n%s", got)
	}
}

func resourceRows() []view.ResourceRow {
	return []view.ResourceRow{
		{Type: "kms", Name: "shared", ARN: "arn:aws:kms:us-east-1:123456789012:key/k", State: "Relinquished", DeletionPolicy: "Retain",
			OnDelete: "Retain", CreatedAt: at(5 * day), LastVerifiedAt: ptr(3 * time.Minute), HeldBy: []string{"s3/receipts", "sqs/orders"}},
		{Type: "rds", Name: "orders-db", ARN: "arn:aws:rds:us-east-1:123456789012:db:orders-db", State: "Creating", DeletionPolicy: "Delete",
			OnDelete: "ManualDeletion", Force: false, CreatedAt: at(2 * time.Minute), ObservedStatus: "creating"},
		{Type: "sqs", Name: "scratch", ARN: "arn:aws:sqs:us-east-1:123456789012:scratch", State: "Verified", DeletionPolicy: "Delete",
			OnDelete: "DeleteEvenIfNotEmpty", Force: true, CreatedAt: at(49 * time.Hour), LastVerifiedAt: ptr(9 * time.Minute),
			PendingDeletionSince: ptr(3 * day)},
	}
}

func TestResourcesText(t *testing.T) {
	for name, tc := range map[string]struct{ wide, noHeaders bool }{
		"resources": {false, false}, "resources_wide": {true, false}, "resources_noheaders": {false, true},
	} {
		t.Run(name, func(t *testing.T) {
			var b bytes.Buffer
			if err := render.ResourcesText(&b, resourceRows(), clitest.Now, tc.wide, tc.noHeaders); err != nil {
				t.Fatalf("ResourcesText() error = %v", err)
			}
			golden(t, name, b.String())
		})
	}
}

func TestRetainedText(t *testing.T) {
	rows := []view.RetainedRow{
		{Namespace: "payments", CR: "ledger-service", Type: "kms", Name: "dedicated", State: "Verified", OnDelete: "Delete", Why: "KeyHeld", Detail: "held by sqs/orders"},
		{Namespace: "payments", CR: "ledger-service", Type: "sqs", Name: "audit", State: "Relinquished", OnDelete: "Retain", Why: "LetGo"},
		{Namespace: "payments", CR: "ledger-service", Type: "sqs", Name: "orders", State: "Verified", OnDelete: "Retain", Why: "KeptOnDelete"},
	}
	for name, noHeaders := range map[string]bool{"retained": false, "retained_noheaders": true} {
		t.Run(name, func(t *testing.T) {
			var b bytes.Buffer
			if err := render.RetainedText(&b, rows, noHeaders); err != nil {
				t.Fatalf("RetainedText() error = %v", err)
			}
			golden(t, name, b.String())
		})
	}
}

func TestRetainedText_NothingRetained(t *testing.T) {
	var b bytes.Buffer
	if err := render.RetainedText(&b, nil, false); err != nil {
		t.Fatalf("RetainedText() error = %v", err)
	}
	golden(t, "retained_none", b.String())
}

func TestJSONAndYAMLCarryTheSameDocument(t *testing.T) {
	v := notReadyView()

	var j, y bytes.Buffer
	if err := render.JSON(&j, v); err != nil {
		t.Fatalf("JSON() error = %v", err)
	}
	if err := render.YAML(&y, v); err != nil {
		t.Fatalf("YAML() error = %v", err)
	}

	var fromJSON, fromYAML map[string]any
	if err := json.Unmarshal(j.Bytes(), &fromJSON); err != nil {
		t.Fatalf("the JSON output is not JSON: %v", err)
	}
	if err := yaml.Unmarshal(y.Bytes(), &fromYAML); err != nil {
		t.Fatalf("the YAML output is not YAML: %v", err)
	}
	a, _ := json.Marshal(fromJSON)
	b, _ := json.Marshal(fromYAML)
	if string(a) != string(b) {
		t.Errorf("JSON and YAML differ:\n%s\n%s", a, b)
	}
	if fromJSON["schemaVersion"] != view.SchemaVersion || fromJSON["kind"] != "Status" {
		t.Errorf("schemaVersion/kind = %v/%v", fromJSON["schemaVersion"], fromJSON["kind"])
	}
	if !strings.HasSuffix(j.String(), "\n") {
		t.Error("JSON output should end with a newline")
	}
}

// The JSON field names are a published contract: within the v1 schema a field
// may be added, never removed or renamed. testdata/contract-v1.keys lists every
// key path a v1 document has had; this fails if one stops being produced.
func TestJSONNeverDropsAKeyOfTheV1Contract(t *testing.T) {
	var b bytes.Buffer
	if err := render.JSON(&b, notReadyView()); err != nil {
		t.Fatalf("JSON() error = %v", err)
	}
	var doc any
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	collectKeys("", doc, have)

	raw, err := os.ReadFile(filepath.Clean(filepath.Join("testdata", "contract-v1.keys")))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range strings.Fields(string(raw)) {
		if !have[key] {
			t.Errorf("the v1 contract key %q is no longer produced", key)
		}
	}
}

func collectKeys(prefix string, v any, out map[string]bool) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			out[path] = true
			collectKeys(path, child, out)
		}
	case []any:
		for _, child := range t {
			collectKeys(prefix+"[]", child, out)
		}
	}
}
