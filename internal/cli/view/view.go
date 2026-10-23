// Package view turns CRs into the models the commands print. It is pure: the
// same CR and the same `now` always give the same result.
package view

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

// SchemaVersion identifies the JSON and YAML documents. Within v1 a field may
// be added, never removed or renamed.
const SchemaVersion = "appdeps.cloudctl.io/v1"

const kindUnknown = "Unknown"

// otherConditions are real conditions that are not a section and never decide
// Ready.
var otherConditions = map[string]bool{
	"SharedWithReferencesValid": true,
	"ConsumeReferencesValid":    true,
	"DeletionBlocked":           true,
}

// StatusView is what `status` shows.
type StatusView struct {
	SchemaVersion      string        `json:"schemaVersion"`
	Kind               string        `json:"kind"`
	Context            string        `json:"context"`
	Namespace          string        `json:"namespace"`
	Name               string        `json:"name"`
	Generation         int64         `json:"generation"`
	ObservedGeneration int64         `json:"observedGeneration"`
	UpToDate           bool          `json:"upToDate"`
	Ready              bool          `json:"ready"`
	Deleting           bool          `json:"deleting"`
	ReadyReason        string        `json:"readyReason"`
	ReadyMessage       string        `json:"readyMessage"`
	LastVerifiedAt     *time.Time    `json:"lastVerifiedAt"`
	Sections           []SectionView `json:"sections"`
	Other              []SectionView `json:"other"`
	Ledger             LedgerSummary `json:"ledger"`
}

// SectionView is one condition, explained from the reasons catalogue.
type SectionView struct {
	Type    string    `json:"type"`
	Ready   bool      `json:"ready"`
	Reason  string    `json:"reason"`
	Message string    `json:"message"`
	Kind    string    `json:"kind"`
	Meaning string    `json:"meaning"`
	Action  string    `json:"action"`
	Since   time.Time `json:"since"`
}

// LedgerSummary counts the ownership ledger and lists what needs a look.
type LedgerSummary struct {
	Total     int            `json:"total"`
	ByState   map[string]int `json:"byState"`
	Attention []ResourceRow  `json:"attention"`
}

// ResourceRow is one ledger entry.
type ResourceRow struct {
	Type                 string     `json:"type"`
	Name                 string     `json:"name"`
	ARN                  string     `json:"arn"`
	State                string     `json:"state"`
	DeletionPolicy       string     `json:"deletionPolicy"`
	OnDelete             string     `json:"onDelete"`
	ObservedStatus       string     `json:"observedStatus"`
	Force                bool       `json:"force"`
	CreatedAt            time.Time  `json:"createdAt"`
	LastVerifiedAt       *time.Time `json:"lastVerifiedAt"`
	PendingDeletionSince *time.Time `json:"pendingDeletionSince"`
	HeldBy               []string   `json:"heldBy"`
}

// RetainedRow is one entry the operator keeps, or will keep, past its CR.
type RetainedRow struct {
	Namespace string `json:"namespace"`
	CR        string `json:"cr"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	State     string `json:"state"`
	OnDelete  string `json:"onDelete"`
	Why       string `json:"why"`
	Detail    string `json:"detail"`
}

// ResourceFilter narrows `resources`; an empty field matches everything.
type ResourceFilter struct {
	Type     string `json:"type"`
	State    string `json:"state"`
	OnDelete string `json:"onDelete"`
}

// Age renders how long before now t was: 45s, 3m, 5h, 2d; "-" for a zero time.
func Age(now, t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t)
	switch {
	case d < 0:
		return "0s"
	case d < time.Minute:
		return strconv.Itoa(int(d/time.Second)) + "s"
	case d < time.Hour:
		return strconv.Itoa(int(d/time.Minute)) + "m"
	case d < 24*time.Hour:
		return strconv.Itoa(int(d/time.Hour)) + "h"
	}
	return strconv.Itoa(int(d/(24*time.Hour))) + "d"
}

// Status builds the view for one CR.
func Status(cr *depsv1alpha1.AppDependencies, target kube.Target, _ time.Time) StatusView {
	v := StatusView{
		SchemaVersion: SchemaVersion, Kind: "Status",
		Context: target.Context, Namespace: target.Namespace, Name: cr.Name,
		Generation: cr.Generation, Deleting: cr.DeletionTimestamp != nil,
		Sections: []SectionView{}, Other: []SectionView{},
		Ledger: LedgerSummary{ByState: map[string]int{}, Attention: []ResourceRow{}},
	}
	if ready := apimeta.FindStatusCondition(cr.Status.Conditions, "Ready"); ready != nil {
		v.Ready = ready.Status == metav1.ConditionTrue
		v.ObservedGeneration = ready.ObservedGeneration
		v.UpToDate = cr.Generation == ready.ObservedGeneration
		v.ReadyReason, v.ReadyMessage = ready.Reason, ready.Message
		v.Sections, v.Other = splitConditions(cr.Status.Conditions)
	} else {
		v.ReadyReason = "NoStatusYet"
		v.ReadyMessage = "the operator has not reported a status for this CR yet"
	}
	summariseLedger(&v, cr.Status.ManagedResources)
	return v
}

// splitConditions separates the sections from the other conditions, each
// sorted by type, and explains each one from the catalogue.
func splitConditions(conditions []metav1.Condition) (sections, other []SectionView) {
	sections, other = []SectionView{}, []SectionView{}
	for _, c := range conditions {
		if c.Type == "Ready" {
			continue
		}
		s := SectionView{
			Type:    c.Type,
			Ready:   c.Status == metav1.ConditionTrue,
			Reason:  c.Reason,
			Message: c.Message,
			Since:   c.LastTransitionTime.Time,
			Kind:    kindUnknown,
		}
		if r, ok := reasons.Lookup(c.Reason); ok {
			s.Kind, s.Meaning, s.Action = string(r.Kind), r.Meaning, r.Action
		}
		if otherConditions[c.Type] {
			other = append(other, s)
		} else {
			sections = append(sections, s)
		}
	}
	byType := func(a, b SectionView) int { return cmp.Compare(a.Type, b.Type) }
	slices.SortFunc(sections, byType)
	slices.SortFunc(other, byType)
	return sections, other
}

func summariseLedger(v *StatusView, entries []depsv1alpha1.ManagedResource) {
	for _, e := range entries {
		v.Ledger.Total++
		v.Ledger.ByState[string(e.State)]++
		if e.State != depsv1alpha1.ManagedResourceStateVerified || e.PendingDeletionSince != nil {
			v.Ledger.Attention = append(v.Ledger.Attention, resourceRow(e))
		}
		if e.LastVerifiedAt != nil && (v.LastVerifiedAt == nil || e.LastVerifiedAt.After(*v.LastVerifiedAt)) {
			t := e.LastVerifiedAt.Time
			v.LastVerifiedAt = &t
		}
	}
	sortRows(v.Ledger.Attention)
}

func timePtr(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	return &t.Time
}

func resourceRow(e depsv1alpha1.ManagedResource) ResourceRow {
	return ResourceRow{
		Type: e.Type, Name: e.Name, ARN: e.ARN, State: string(e.State),
		DeletionPolicy: string(e.DeletionPolicy), OnDelete: string(e.OnDelete),
		ObservedStatus: e.ObservedStatus, Force: e.Force, CreatedAt: e.CreatedAt.Time,
		LastVerifiedAt: timePtr(e.LastVerifiedAt), PendingDeletionSince: timePtr(e.PendingDeletionSince),
		HeldBy: slices.Clone(e.HeldBy),
	}
}

func sortRows(rows []ResourceRow) {
	slices.SortFunc(rows, func(a, b ResourceRow) int {
		return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.Name, b.Name))
	})
}

// Resources lists a CR's ledger, sorted by type then name.
func Resources(cr *depsv1alpha1.AppDependencies, f ResourceFilter) []ResourceRow {
	rows := []ResourceRow{}
	for _, e := range cr.Status.ManagedResources {
		r := resourceRow(e)
		if matches(f.Type, r.Type) && matches(f.State, r.State) && matches(f.OnDelete, r.OnDelete) {
			rows = append(rows, r)
		}
	}
	sortRows(rows)
	return rows
}

func matches(filter, value string) bool { return filter == "" || strings.EqualFold(filter, value) }

// Retained lists the entries kept past their CR, sorted by namespace, CR, type
// and name. An entry the operator will delete is not listed.
func Retained(crs []depsv1alpha1.AppDependencies) []RetainedRow {
	rows := []RetainedRow{}
	sorted := slices.Clone(crs)
	slices.SortFunc(sorted, func(a, b depsv1alpha1.AppDependencies) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	for _, cr := range sorted {
		entries := slices.Clone(cr.Status.ManagedResources)
		slices.SortFunc(entries, func(a, b depsv1alpha1.ManagedResource) int {
			return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.Name, b.Name))
		})
		byKey := map[string]depsv1alpha1.ManagedResource{}
		for _, e := range entries {
			byKey[e.Type+"/"+e.Name] = e
		}
		for _, e := range entries {
			why := retainedWhy(e, byKey)
			if why == "" {
				continue
			}
			row := RetainedRow{
				Namespace: cr.Namespace, CR: cr.Name, Type: e.Type, Name: e.Name,
				State: string(e.State), OnDelete: string(e.OnDelete), Why: why,
			}
			if len(e.HeldBy) > 0 {
				row.Detail = "held by " + strings.Join(e.HeldBy, ", ")
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// isRetained reads onDelete, and for a CR last written by an operator older
// than v0.8.1, which had none, the deletion policy.
func isRetained(e depsv1alpha1.ManagedResource) bool {
	if e.OnDelete != "" {
		return e.OnDelete == depsv1alpha1.OnDeleteRetain
	}
	return e.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete
}

func retainedWhy(e depsv1alpha1.ManagedResource, byKey map[string]depsv1alpha1.ManagedResource) string {
	switch {
	case e.State == depsv1alpha1.ManagedResourceStateRelinquished:
		return "LetGo"
	case e.Type == "kms" && heldByRetained(e, byKey):
		return "KeyHeld"
	case isRetained(e):
		return "KeptOnDelete"
	}
	return ""
}

func heldByRetained(key depsv1alpha1.ManagedResource, byKey map[string]depsv1alpha1.ManagedResource) bool {
	for _, holder := range key.HeldBy {
		if h, ok := byKey[holder]; ok && isRetained(h) {
			return true
		}
	}
	return false
}
