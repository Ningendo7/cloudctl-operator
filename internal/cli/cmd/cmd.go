// Package cmd wires the commands together: flags, exit codes and output.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/render"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/version"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/view"
)

// The exit codes are a documented contract.
const (
	ExitOK       = 0
	ExitNotReady = 1
	ExitUsage    = 2
	ExitCluster  = 3
)

// Streams are where a run reads and writes.
type Streams struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

// ReaderFactory builds the cluster reader; kube.New is the real one.
type ReaderFactory func(kube.Options) (kube.Reader, kube.Target, error)

// exitError carries an exit code out of a command. A nil err exits silently.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }

type app struct {
	streams   Streams
	newReader ReaderFactory
	now       func() time.Time
	display   string // "kubectl appdeps" or "appdeps"

	opts       kube.Options
	output     string
	noHeaders  bool
	showSource bool
	events     bool
	server     bool
	filter     view.ResourceFilter
}

// Execute runs the CLI as progName with args and returns the exit code.
func Execute(args []string, progName string, s Streams, newReader ReaderFactory, now func() time.Time) int {
	a := &app{streams: s, newReader: newReader, now: now, display: "appdeps"}
	if progName == "kubectl-appdeps" {
		a.display = "kubectl appdeps"
	}
	root := a.root()
	root.SetArgs(args)
	return a.exitCode(root.Execute())
}

func (a *app) root() *cobra.Command {
	root := &cobra.Command{
		// cobra takes the first word of Use as the name, so the kubectl form
		// is shown in the help text instead.
		Use:           "appdeps",
		SilenceUsage:  true,
		SilenceErrors: true,
		Long: "Usage:\n" +
			"  " + a.display + " status <cr>\n" +
			"  " + a.display + " resources <cr>\n" +
			"  " + a.display + " retained\n" +
			"  " + a.display + " version\n",
	}
	root.SetOut(a.streams.Out)
	root.SetErr(a.streams.Err)
	flags := root.PersistentFlags()
	flags.StringVar(&a.opts.Kubeconfig, "kubeconfig", "", "path to the kubeconfig")
	flags.StringVar(&a.opts.Context, "context", "", "kubeconfig context to use")
	flags.StringVarP(&a.opts.Namespace, "namespace", "n", "", "namespace")
	flags.BoolVarP(&a.opts.AllNamespaces, "all-namespaces", "A", false, "all namespaces")
	flags.StringVarP(&a.output, "output", "o", "table", "output format: table, wide, json or yaml")
	flags.BoolVar(&a.noHeaders, "no-headers", false, "leave the header row out of tables")
	root.AddCommand(a.statusCmd(), a.resourcesCmd(), a.retainedCmd(), a.versionCmd())
	return root
}

// exitCode maps what Execute got back to an exit code. Anything cobra rejects
// before a command runs (unknown command or flag, wrong argument count) is a
// plain error and so a usage error.
func (a *app) exitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	var exit *exitError
	if errors.As(err, &exit) {
		if exit.err != nil {
			fmt.Fprintln(a.streams.Err, "Error:", exit.err)
		}
		return exit.code
	}
	fmt.Fprintln(a.streams.Err, "Error:", err)
	return ExitUsage
}

func (a *app) structured() bool { return a.output == "json" || a.output == "yaml" }

func (a *app) checkOutput() error {
	switch a.output {
	case "table", "wide", "json", "yaml":
		return nil
	}
	return fmt.Errorf("unknown output format %q", a.output)
}

func (a *app) emit(doc any) error {
	if a.output == "yaml" {
		return render.YAML(a.streams.Out, doc)
	}
	return render.JSON(a.streams.Out, doc)
}

func (a *app) connect() (kube.Reader, kube.Target, error) {
	reader, target, err := a.newReader(a.opts)
	if err != nil {
		return nil, target, &exitError{ExitCluster, err}
	}
	return reader, target, nil
}

// clusterError adds the RBAC hint when the error is a forbidden one.
func clusterError(err error) error {
	if hint := kube.ExplainForbidden(err); hint != "" {
		err = fmt.Errorf("%w\n%s", err, hint)
	}
	return &exitError{ExitCluster, err}
}

func (a *app) statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status <cr>",
		Aliases: []string{"explain"},
		Short:   "Is it Ready, and if not, why and what to do",
		Args:    cobra.ExactArgs(1),
		RunE:    func(_ *cobra.Command, args []string) error { return a.runStatus(args[0]) },
	}
	cmd.Flags().BoolVar(&a.showSource, "show-source", false, "show the status fields each statement came from")
	cmd.Flags().BoolVar(&a.events, "events", false, "also show recent events")
	return cmd
}

func (a *app) runStatus(name string) error {
	if err := a.checkOutput(); err != nil {
		return err
	}
	if a.opts.AllNamespaces {
		return errors.New("--all-namespaces cannot be used with status")
	}
	reader, target, err := a.connect()
	if err != nil {
		return err
	}
	cr, err := reader.Get(context.Background(), target.Namespace, name)
	if err != nil {
		return clusterError(err)
	}
	v := view.Status(cr, target, a.now())
	if a.structured() {
		if err := a.emit(v); err != nil {
			return err
		}
	} else {
		if err := render.StatusText(a.streams.Out, v, a.now(), a.showSource); err != nil {
			return err
		}
		if a.events {
			a.printEvents(reader, target.Namespace, name)
		}
	}
	if !v.Ready {
		return &exitError{code: ExitNotReady}
	}
	return nil
}

// printEvents never fails the command: events are optional.
func (a *app) printEvents(reader kube.Reader, namespace, name string) {
	events, err := reader.Events(context.Background(), namespace, name)
	if err != nil {
		note := "could not read events: " + err.Error()
		if hint := kube.ExplainForbidden(err); hint != "" {
			note += "\n" + hint
		}
		fmt.Fprintln(a.streams.Err, note)
		return
	}
	fmt.Fprintln(a.streams.Out, "\nKubernetes keeps events for about an hour; this is not history.")
	for _, e := range events {
		fmt.Fprintf(a.streams.Out, "  %s  %s  %s  %s\n", view.Age(a.now(), e.Last), e.Type, e.Reason, e.Message)
	}
}

func (a *app) resourcesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "resources <cr>",
		Aliases: []string{"ledger"},
		Short:   "The ownership ledger of one CR",
		Args:    cobra.ExactArgs(1),
		RunE:    func(_ *cobra.Command, args []string) error { return a.runResources(args[0]) },
	}
	cmd.Flags().StringVar(&a.filter.Type, "type", "", "only this resource type")
	cmd.Flags().StringVar(&a.filter.State, "state", "", "only this ledger state")
	cmd.Flags().StringVar(&a.filter.OnDelete, "on-delete", "", "only this onDelete outcome")
	return cmd
}

func (a *app) runResources(name string) error {
	if err := a.checkOutput(); err != nil {
		return err
	}
	if a.opts.AllNamespaces {
		return errors.New("--all-namespaces cannot be used with resources")
	}
	reader, target, err := a.connect()
	if err != nil {
		return err
	}
	cr, err := reader.Get(context.Background(), target.Namespace, name)
	if err != nil {
		return clusterError(err)
	}
	rows := view.Resources(cr, a.filter)
	if a.structured() {
		return a.emit(map[string]any{
			"schemaVersion": view.SchemaVersion, "kind": "Resources",
			"context": target.Context, "namespace": target.Namespace, "name": name, "items": rows,
		})
	}
	fmt.Fprintf(a.streams.Out, "Context: %s   Namespace: %s   CR: %s\n\n", target.Context, target.Namespace, name)
	return render.ResourcesText(a.streams.Out, rows, a.now(), a.output == "wide", a.noHeaders)
}

func (a *app) retainedCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "retained",
		Short: "What the operator keeps beyond a CR, and why",
		Args:  cobra.NoArgs,
		RunE:  func(_ *cobra.Command, _ []string) error { return a.runRetained() },
	}
}

func (a *app) runRetained() error {
	if err := a.checkOutput(); err != nil {
		return err
	}
	reader, target, err := a.connect()
	if err != nil {
		return err
	}
	crs, err := reader.List(context.Background(), target.Namespace) // "" with -A lists everything
	if err != nil {
		return clusterError(err)
	}
	rows := view.Retained(crs)
	shown := target.Namespace
	if a.opts.AllNamespaces {
		shown = "all"
	}
	if a.structured() {
		return a.emit(map[string]any{
			"schemaVersion": view.SchemaVersion, "kind": "Retained",
			"context": target.Context, "namespace": shown, "items": rows,
		})
	}
	fmt.Fprintf(a.streams.Out, "Context: %s   Namespace: %s\n\n", target.Context, shown)
	return render.RetainedText(a.streams.Out, rows, a.noHeaders)
}

func (a *app) versionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "The CLI version and the cluster's API version",
		Args:  cobra.NoArgs,
		RunE:  func(_ *cobra.Command, _ []string) error { return a.runVersion() },
	}
	cmd.Flags().BoolVar(&a.server, "server", false, "also show the operator version")
	return cmd
}

func (a *app) runVersion() error {
	if err := a.checkOutput(); err != nil {
		return err
	}
	reader, _, err := a.connect()
	if err != nil {
		return err
	}
	client := version.Current()
	info, serverErr := reader.ServerVersions(context.Background())
	if a.structured() {
		apiVersions := info.APIVersions
		if apiVersions == nil {
			apiVersions = []string{}
		}
		return a.emit(map[string]any{
			"schemaVersion": view.SchemaVersion, "kind": "Version",
			"client": map[string]string{"version": client.Version, "commit": client.Commit, "goVersion": client.GoVersion},
			"server": map[string]any{"apiVersions": apiVersions, "operatorImage": info.OperatorImage},
		})
	}
	out := a.streams.Out
	fmt.Fprintf(out, "Client: %s (%s)\n", client.Version, client.Commit)
	if serverErr != nil {
		fmt.Fprintf(out, "Server API: unknown (%s)\n", serverErr)
	} else {
		fmt.Fprintf(out, "Server API: %s\n", strings.Join(info.APIVersions, ", "))
	}
	if a.server {
		printOperator(out, client.Version, info.OperatorImage)
	}
	return nil
}

func printOperator(out io.Writer, clientVersion, image string) {
	if image == "" {
		fmt.Fprintln(out, "Operator: unknown")
		return
	}
	fmt.Fprintf(out, "Operator: %s\n", image)
	operator := image[strings.LastIndex(image, ":")+1:]
	if cm, om := minor(clientVersion), minor(operator); cm != "" && om != "" && cm != om {
		fmt.Fprintf(out, "Warning: the CLI is %s and the operator is %s; output may not match\n", clientVersion, operator)
	}
}

// minor returns "0.8" for v0.8.1, or "" when v is not a version.
func minor(v string) string {
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[0] + "." + parts[1]
}
