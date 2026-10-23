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

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/clitest"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/view"
)

func ledgerCR() *depsv1alpha1.AppDependencies {
	return clitest.CR("checkout", "checkout-service",
		clitest.Entry(clitest.Res("sqs", "orders", func(e *depsv1alpha1.ManagedResource) {
			e.DeletionPolicy = depsv1alpha1.DeletionPolicyDelete
			e.OnDelete = depsv1alpha1.OnDeleteDelete
		})),
		clitest.Entry(clitest.Res("s3", "receipts")),
		clitest.Entry(clitest.Res("kms", "shared", func(e *depsv1alpha1.ManagedResource) {
			e.HeldBy = []string{"s3/receipts", "sqs/orders"}
		})),
		clitest.Entry(clitest.Res("sqs", "audit", func(e *depsv1alpha1.ManagedResource) {
			e.State = depsv1alpha1.ManagedResourceStateRelinquished
		})),
	)
}

func names(rows []view.ResourceRow) string {
	out := ""
	for i, r := range rows {
		if i > 0 {
			out += ","
		}
		out += r.Type + "/" + r.Name
	}
	return out
}

func TestResources_AreSortedByTypeThenName(t *testing.T) {
	got := names(view.Resources(ledgerCR(), view.ResourceFilter{}))

	if got != "kms/shared,s3/receipts,sqs/audit,sqs/orders" {
		t.Errorf("rows = %s", got)
	}
}

func TestResources_FiltersMatchExactlyAndIgnoreCase(t *testing.T) {
	cr := ledgerCR()
	tests := map[string]struct {
		filter view.ResourceFilter
		want   string
	}{
		"by type":               {view.ResourceFilter{Type: "sqs"}, "sqs/audit,sqs/orders"},
		"by state":              {view.ResourceFilter{State: "relinquished"}, "sqs/audit"},
		"by on-delete":          {view.ResourceFilter{OnDelete: "DELETE"}, "sqs/orders"},
		"by two at once":        {view.ResourceFilter{Type: "sqs", OnDelete: "Retain"}, "sqs/audit"},
		"nothing matches":       {view.ResourceFilter{Type: "rds"}, ""},
		"a prefix is not a hit": {view.ResourceFilter{Type: "sq"}, ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := names(view.Resources(cr, tc.filter)); got != tc.want {
				t.Errorf("rows = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResources_RowsCarryTheLedgerFields(t *testing.T) {
	rows := view.Resources(ledgerCR(), view.ResourceFilter{Type: "kms"})

	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	r := rows[0]
	if r.ARN != "arn:aws:kms:us-east-1:123456789012:shared" || r.State != "Verified" || r.DeletionPolicy != "Retain" ||
		r.OnDelete != "Retain" || len(r.HeldBy) != 2 || !r.CreatedAt.Equal(clitest.Now.Add(-48*time.Hour)) {
		t.Errorf("row = %+v", r)
	}
}

func TestRetained_ExplainsWhyEachEntryIsKept(t *testing.T) {
	letGo := clitest.CR("payments", "ledger-service",
		clitest.Entry(clitest.Res("kms", "dedicated", func(e *depsv1alpha1.ManagedResource) {
			e.DeletionPolicy = depsv1alpha1.DeletionPolicyDelete
			e.OnDelete = depsv1alpha1.OnDeleteDelete
			e.HeldBy = []string{"sqs/orders"}
		})),
		clitest.Entry(clitest.Res("sqs", "orders")),
		clitest.Entry(clitest.Res("sqs", "audit", func(e *depsv1alpha1.ManagedResource) {
			e.State = depsv1alpha1.ManagedResourceStateRelinquished
		})),
		clitest.Entry(clitest.Res("sns", "scratch", func(e *depsv1alpha1.ManagedResource) {
			e.DeletionPolicy = depsv1alpha1.DeletionPolicyDelete
			e.OnDelete = depsv1alpha1.OnDeleteDelete
		})),
	)

	rows := view.Retained([]depsv1alpha1.AppDependencies{*letGo})

	got := map[string]string{}
	for _, r := range rows {
		got[r.Type+"/"+r.Name] = r.Why
		if r.Namespace != "payments" || r.CR != "ledger-service" {
			t.Errorf("row %+v should name its CR", r)
		}
	}
	want := map[string]string{"kms/dedicated": "KeyHeld", "sqs/orders": "KeptOnDelete", "sqs/audit": "LetGo"}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v (the entry that will be deleted is omitted)", got, want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: why = %q, want %q", k, got[k], w)
		}
	}
}

func TestRetained_ARetainedKeyThatIsHeldIsReportedAsHeld(t *testing.T) {
	cr := clitest.CR("payments", "ledger-service",
		clitest.Entry(clitest.Res("kms", "shared", func(e *depsv1alpha1.ManagedResource) { e.HeldBy = []string{"sqs/orders"} })),
		clitest.Entry(clitest.Res("sqs", "orders")),
	)

	rows := view.Retained([]depsv1alpha1.AppDependencies{*cr})

	for _, r := range rows {
		if r.Type == "kms" && r.Why != "KeyHeld" {
			t.Errorf("a retained key held by a retained resource should be KeyHeld, got %q", r.Why)
		}
	}
}

func TestRetained_AnOlderOperatorWithoutOnDeleteFallsBackToTheDeletionPolicy(t *testing.T) {
	cr := clitest.CR("payments", "ledger-service",
		clitest.Entry(clitest.Res("sqs", "kept", func(e *depsv1alpha1.ManagedResource) { e.OnDelete = "" })),
		clitest.Entry(clitest.Res("sqs", "gone", func(e *depsv1alpha1.ManagedResource) {
			e.OnDelete = ""
			e.DeletionPolicy = depsv1alpha1.DeletionPolicyDelete
		})),
	)

	rows := view.Retained([]depsv1alpha1.AppDependencies{*cr})

	if len(rows) != 1 || rows[0].Name != "kept" || rows[0].Why != "KeptOnDelete" {
		t.Errorf("rows = %+v, want only sqs/kept as KeptOnDelete", rows)
	}
}

func TestRetained_RowsSortByNamespaceCRTypeName(t *testing.T) {
	b := clitest.CR("b-ns", "app", clitest.Entry(clitest.Res("sqs", "z")), clitest.Entry(clitest.Res("s3", "a")))
	a2 := clitest.CR("a-ns", "second", clitest.Entry(clitest.Res("sqs", "q")))
	a1 := clitest.CR("a-ns", "first", clitest.Entry(clitest.Res("sqs", "q")))

	rows := view.Retained([]depsv1alpha1.AppDependencies{*b, *a2, *a1})

	var got string
	for _, r := range rows {
		got += r.Namespace + "/" + r.CR + "/" + r.Type + "/" + r.Name + " "
	}
	if want := "a-ns/first/sqs/q a-ns/second/sqs/q b-ns/app/s3/a b-ns/app/sqs/z "; got != want {
		t.Errorf("order = %q, want %q", got, want)
	}
}

func TestRetained_NothingRetainedIsAnEmptyListNotAnError(t *testing.T) {
	if rows := view.Retained(nil); len(rows) != 0 {
		t.Errorf("rows = %+v, want none", rows)
	}
}

func TestAge(t *testing.T) {
	now := clitest.Now
	tests := map[string]struct {
		t    time.Time
		want string
	}{
		"seconds":          {now.Add(-45 * time.Second), "45s"},
		"just under a min": {now.Add(-59 * time.Second), "59s"},
		"a minute":         {now.Add(-60 * time.Second), "1m"},
		"minutes":          {now.Add(-3 * time.Minute), "3m"},
		"just under hour":  {now.Add(-59*time.Minute - 59*time.Second), "59m"},
		"an hour":          {now.Add(-time.Hour), "1h"},
		"hours":            {now.Add(-5 * time.Hour), "5h"},
		"a day":            {now.Add(-24 * time.Hour), "1d"},
		"days":             {now.Add(-49 * time.Hour), "2d"},
		"zero time":        {time.Time{}, "-"},
		"in the future":    {now.Add(time.Minute), "0s"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := view.Age(now, tc.t); got != tc.want {
				t.Errorf("Age() = %q, want %q", got, tc.want)
			}
		})
	}
}
