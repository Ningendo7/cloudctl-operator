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

// Package kube is the only place the CLI talks to an API server. It reads and
// never writes.
package kube

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

const (
	apiGroup           = "deps.cloudctl.io"
	operatorLabelKey   = "app.kubernetes.io/name"
	operatorLabelValue = "cloudctl-operator"
)

// Options say which cluster, context and namespace to use.
type Options struct {
	Kubeconfig    string
	Context       string
	Namespace     string
	AllNamespaces bool
	// WrapTransport lets a test observe every request. Not set in production.
	WrapTransport func(http.RoundTripper) http.RoundTripper
}

// Target is what Options resolved to, for the header of every output.
// Namespace is empty with AllNamespaces.
type Target struct {
	Context   string
	Namespace string
}

// Event is one Kubernetes event about a CR.
type Event struct {
	Type    string
	Reason  string
	Message string
	Count   int32
	Last    time.Time
}

// ServerInfo is what can be learned about the cluster side without extra
// permissions. OperatorImage is empty when it cannot be read.
type ServerInfo struct {
	APIVersions   []string
	OperatorImage string
}

// Reader reads AppDependencies and what surrounds them.
type Reader interface {
	Get(ctx context.Context, namespace, name string) (*depsv1alpha1.AppDependencies, error)
	// List returns the CRs of one namespace, or of all when namespace is empty.
	List(ctx context.Context, namespace string) ([]depsv1alpha1.AppDependencies, error)
	Events(ctx context.Context, namespace, name string) ([]Event, error)
	ServerVersions(ctx context.Context) (ServerInfo, error)
}

// The errors a Reader's error match with errors.Is
var (
	ErrNotFound  = errors.New("not found")
	ErrForbidden = errors.New("forbidden")
)

// classify makes API errors match ErrNotFound or ErrForbidden, keeping the
// original error too.
func classify(err error) error {
	switch {
	case apierrors.IsNotFound(err):
		return errors.Join(ErrNotFound, err)
	case apierrors.IsForbidden(err):
		return errors.Join(ErrForbidden, err)
	}
	return err
}

// ExplainForbidden says which permission a forbidden error is about, or
// returns "" for any other error.
func ExplainForbidden(err error) string {
	var status *apierrors.StatusError
	if !errors.As(err, &status) || !apierrors.IsForbidden(status) {
		return ""
	}
	if details := status.ErrStatus.Details; details != nil {
		switch details.Group {
		case apiGroup:
			return "You need get and list on appdependencies.deps.cloudctl.io; the appdependencies-viewer-role ClusterRole grants it."
		case "events.k8s.io":
			return "Events are optional: reading them needs get and list on events.k8s.io"
		}
	}
	return "forbidden:" + status.Error()
}

type reader struct {
	client    client.Client
	discovery discovery.DiscoveryInterface
}

// New resolves the kubeconfig and returns a Reader for it.
func New(opts Options) (Reader, Target, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if opts.Kubeconfig != "" {
		rules.ExplicitPath = opts.Kubeconfig
	}
	config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: opts.Context})

	raw, err := config.RawConfig()
	if err != nil {
		return nil, Target{}, fmt.Errorf("reading the kubeconfig: %w", err)
	}
	restConfig, err := config.ClientConfig()
	if err != nil {
		return nil, Target{}, fmt.Errorf("loading the kubeconfig: %w", err)
	}
	if opts.WrapTransport != nil {
		restConfig.Wrap(opts.WrapTransport)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, Target{}, err
	}
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		return nil, Target{}, err
	}
	c, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, Target{}, err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return nil, Target{}, err
	}
	return &reader{
		client:    c,
		discovery: disc,
	}, target(opts, raw.CurrentContext, config), nil
}

func target(opts Options, currentContext string, config clientcmd.ClientConfig) Target {
	t := Target{Context: currentContext}
	if opts.Context != "" {
		t.Context = opts.Context
	}
	switch {
	case opts.AllNamespaces:
	case opts.Namespace != "":
		t.Namespace = opts.Namespace
	default:
		t.Namespace, _, _ = config.Namespace() // default when the context has none
	}
	return t
}

func (r *reader) Get(ctx context.Context, namespace, name string) (*depsv1alpha1.AppDependencies, error) {
	var cr depsv1alpha1.AppDependencies
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cr); err != nil {
		return nil, fmt.Errorf("getting appdependencies %q in namespace %q: %w", name, namespace, classify(err))
	}
	return &cr, nil
}

func (r *reader) List(ctx context.Context, namespace string) ([]depsv1alpha1.AppDependencies, error) {
	var list depsv1alpha1.AppDependenciesList
	var opts []client.ListOption
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := r.client.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("listing appdependencies: %w", classify(err))
	}
	return list.Items, nil
}

func (r *reader) Events(ctx context.Context, namespace, name string) ([]Event, error) {
	var list eventsv1.EventList
	if err := r.client.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing events: %w", classify(err))
	}
	var events []Event
	for _, e := range list.Items {
		if e.Regarding.Kind != "AppDependencies" || e.Regarding.Name != name {
			continue
		}
		event := Event{
			Type:    e.Type,
			Reason:  e.Reason,
			Message: e.Note,
			Last:    e.EventTime.Time,
		}
		if e.Series != nil {
			event.Count = e.Series.Count
		}
		events = append(events, event)
	}
	return events, nil
}

func (r *reader) ServerVersions(ctx context.Context) (ServerInfo, error) {
	var info ServerInfo
	groups, err := r.discovery.ServerGroups()
	if err != nil {
		return info, fmt.Errorf("discovering the API: %w", err)
	}
	for _, group := range groups.Groups {
		if group.Name != apiGroup {
			continue
		}
		for _, v := range group.Versions {
			info.APIVersions = append(info.APIVersions, v.GroupVersion)
		}
	}
	info.OperatorImage = r.operatorImage(ctx)
	return info, nil
}

// operatorImage is best effort: reading Deployments is not part of the viewer
// role, and the operator may not run in this cluster, so any failure is "unknown".
func (r *reader) operatorImage(ctx context.Context) string {
	var deployments appsv1.DeploymentList
	if err := r.client.List(ctx, &deployments, client.MatchingLabels{
		operatorLabelKey: operatorLabelValue,
	}); err != nil {
		return ""
	}
	for _, d := range deployments.Items {
		if containers := d.Spec.Template.Spec.Containers; len(containers) > 0 {
			return containers[0].Image
		}
	}
	return ""
}
