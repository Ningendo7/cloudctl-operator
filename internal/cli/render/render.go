// Package render prints view models as text, JSON or YAML. The text format is
// specified by the golden files in testdata.
package render

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/view"
)

// stateOrder is the order ledger states are listed in.
var stateOrder = []string{"Verified", "Creating", "TagPending", "NeedsVerification", "Relinquished"}

const retainedHint = "Resources released without a key leave the ledger. Find them in AWS with: " +
	"aws resourcegroupstaggingapi get-resources --tag-filters Key=cloudctl.io/released-from\n"

// table renders rows like `kubectl get`: columns padded with three spaces.
func table(rows [][]string) string {
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 8, 3, ' ', 0)
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush() // writes to a strings.Builder, which cannot fail
	return b.String()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func truth(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

func age(now time.Time, t *time.Time) string {
	if t == nil {
		return "-"
	}
	return view.Age(now, *t)
}

// StatusText prints the status of one CR.
func StatusText(w io.Writer, v view.StatusView, now time.Time, showSource bool) error {
	var b strings.Builder
	fmt.Fprintf(&b, "Context: %s   Namespace: %s   CR: %s\n\n", v.Context, v.Namespace, v.Name)
	writeReadyBlock(&b, v, now, showSource)
	if v.ReadyReason == "NoStatusYet" && len(v.Sections) == 0 {
		b.WriteString("The operator has not reported a status for this CR yet.\n\n")
	} else {
		writeSectionBlocks(&b, v, now, showSource)
	}
	writeLedgerBlock(&b, v.Ledger, now, showSource)
	_, err := io.WriteString(w, b.String())
	return err
}

func writeReadyBlock(b *strings.Builder, v view.StatusView, now time.Time, showSource bool) {
	state := "behind"
	if v.UpToDate {
		state = "up to date"
	}
	fmt.Fprintf(b, "Ready: %s   spec generation %d, observed %d (%s)\n", truth(v.Ready), v.Generation, v.ObservedGeneration, state)
	if v.LastVerifiedAt != nil {
		fmt.Fprintf(b, "Operator last verified a resource: %s ago\n", view.Age(now, *v.LastVerifiedAt))
	}
	if v.Deleting {
		b.WriteString("Deletion in progress\n")
	}
	if showSource {
		b.WriteString("  source: metadata.generation, status.conditions[Ready], status.managedResources[].lastVerifiedAt\n")
	}
	b.WriteString("\n")
}

func writeSectionBlocks(b *strings.Builder, v view.StatusView, now time.Time, showSource bool) {
	if len(v.Sections) > 0 {
		rows := [][]string{{"SECTION", "READY", "REASON", "KIND", "SINCE"}}
		for _, s := range v.Sections {
			rows = append(rows, []string{s.Type, truth(s.Ready), s.Reason, s.Kind, view.Age(now, s.Since)})
		}
		b.WriteString(table(rows))
		if showSource {
			b.WriteString("  source: status.conditions, internal/reasons catalogue\n")
		}
		b.WriteString("\n")
	}
	if len(v.Other) > 0 {
		rows := [][]string{{"CONDITION", "STATUS", "REASON", "SINCE"}}
		for _, s := range v.Other {
			rows = append(rows, []string{s.Type, truth(s.Ready), s.Reason, view.Age(now, s.Since)})
		}
		b.WriteString(table(rows) + "\n")
	}
	for _, s := range v.Sections {
		if s.Ready {
			continue
		}
		fmt.Fprintf(b, "%s: %s  (%s)\n  %s\n", s.Type, s.Reason, s.Kind, s.Message)
		if s.Meaning != "" {
			fmt.Fprintf(b, "  Meaning: %s\n", s.Meaning)
		}
		if s.Action != "" {
			fmt.Fprintf(b, "  Do:      %s\n", s.Action)
		}
		b.WriteString("\n")
	}
}

func writeLedgerBlock(b *strings.Builder, l view.LedgerSummary, now time.Time, showSource bool) {
	fmt.Fprintf(b, "Ledger: %d entries", l.Total)
	var counts []string
	for _, state := range stateOrder {
		if n := l.ByState[state]; n > 0 {
			counts = append(counts, fmt.Sprintf("%d %s", n, state))
		}
	}
	if len(counts) > 0 {
		fmt.Fprintf(b, " (%s)", strings.Join(counts, ", "))
	}
	b.WriteString("\n")
	if len(l.Attention) > 0 {
		items := make([]string, 0, len(l.Attention))
		for _, r := range l.Attention {
			item := fmt.Sprintf("%s/%s %s", r.Type, r.Name, r.State)
			if r.PendingDeletionSince != nil {
				item += " pending deletion " + view.Age(now, *r.PendingDeletionSince)
			}
			if len(r.HeldBy) > 0 {
				item += ", held by " + strconv.Itoa(len(r.HeldBy))
			}
			items = append(items, item)
		}
		fmt.Fprintf(b, "  needs attention: %s\n", strings.Join(items, "; "))
	}
	if showSource {
		b.WriteString("  source: status.managedResources\n")
	}
}

// ResourcesText prints a CR's ledger as a table.
func ResourcesText(w io.Writer, rows []view.ResourceRow, now time.Time, wide, noHeaders bool) error {
	var out [][]string
	if !noHeaders {
		head := []string{"TYPE", "NAME", "STATE", "ON-DELETE", "POLICY", "FORCE", "AGE", "VERIFIED", "HELD-BY"}
		if wide {
			head = append(head, "ARN", "PENDING-SINCE", "OBSERVED")
		}
		out = append(out, head)
	}
	for _, r := range rows {
		cells := []string{
			r.Type, r.Name, r.State, dash(r.OnDelete), r.DeletionPolicy, strconv.FormatBool(r.Force),
			view.Age(now, r.CreatedAt), age(now, r.LastVerifiedAt), dash(strings.Join(r.HeldBy, ",")),
		}
		if wide {
			cells = append(cells, r.ARN, age(now, r.PendingDeletionSince), dash(r.ObservedStatus))
		}
		out = append(out, cells)
	}
	_, err := io.WriteString(w, table(out))
	return err
}

// RetainedText prints what is kept past its CR, then how to find the rest.
func RetainedText(w io.Writer, rows []view.RetainedRow, noHeaders bool) error {
	if len(rows) == 0 {
		_, err := io.WriteString(w, "No retained resources found.\n\n"+retainedHint)
		return err
	}
	var out [][]string
	if !noHeaders {
		out = append(out, []string{"NAMESPACE", "CR", "TYPE", "NAME", "STATE", "ON-DELETE", "WHY", "DETAIL"})
	}
	for _, r := range rows {
		out = append(out, []string{r.Namespace, r.CR, r.Type, r.Name, r.State, dash(r.OnDelete), r.Why, dash(r.Detail)})
	}
	_, err := io.WriteString(w, table(out)+"\n"+retainedHint)
	return err
}

// JSON prints v as indented JSON ending in a newline.
func JSON(w io.Writer, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(raw, '\n'))
	return err
}

// YAML prints v as YAML, with the same field names as JSON.
func YAML(w io.Writer, v any) error {
	raw, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(raw)
	return err
}
