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

package observability

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// gather collects c on a fresh, pedantic registry (which also fails the test
// if Describe and Collect disagree) and returns every sample keyed as
// name{label=value,...}, with labels sorted.
func gather(t *testing.T, c prometheus.Collector) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			out[sampleKey(f.GetName(), m.GetLabel())] = m.GetGauge().GetValue()
		}
	}
	return out
}

func sampleKey(name string, labels []*dto.LabelPair) string {
	pairs := make([]string, 0, len(labels))
	for _, l := range labels {
		pairs = append(pairs, l.GetName()+"="+l.GetValue())
	}
	slices.Sort(pairs)
	return name + "{" + strings.Join(pairs, ",") + "}"
}

func TestMetricNamesAndLabels(t *testing.T) {
	AWSRequests.WithLabelValues("sqs", ResultSuccess).Add(0)
	AWSRequestDuration.WithLabelValues("sqs").Observe(0)
	AWSRateLimitWait.WithLabelValues("sqs").Add(0)
	SectionReconciles.WithLabelValues("sqs", SectionReady).Add(0)
	SectionReconcileDuration.WithLabelValues("sqs").Observe(0)
	Events.WithLabelValues("Normal", "QueueCreated").Add(0)
	AWSClientsReady.Set(0)

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	labelsOf := map[string][]string{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			names := make([]string, 0, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				names = append(names, l.GetName())
			}
			slices.Sort(names)
			labelsOf[f.GetName()] = names
		}
	}

	want := map[string][]string{
		"cloudctl_aws_requests_total":                 {"result", "service"},
		"cloudctl_aws_request_duration_seconds":       {"service"},
		"cloudctl_aws_rate_limit_wait_seconds_total":  {"service"},
		"cloudctl_section_reconcile_total":            {"result", "section"},
		"cloudctl_section_reconcile_duration_seconds": {"section"},
		"cloudctl_events_total":                       {"reason", "type"},
		"cloudctl_aws_clients_ready":                  nil,
	}
	for name, labels := range want {
		got, ok := labelsOf[name]
		if !ok {
			t.Errorf("metric %s is not registered on controller-runtime's registry", name)
			continue
		}
		if !slices.Equal(got, labels) {
			t.Errorf("metric %s has labels %v, want %v", name, got, labels)
		}
	}
}

// The label values are a contract with dashboards and alerts, and keeping
// them to fixed sets is what keeps the series count independent of how many
// CRs exist.
func TestResultLabelValues(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{ResultSuccess, "success"}, {ResultThrottled, "throttled"}, {ResultPermissionDenied, "permission_denied"},
		{ResultNotFound, "not_found"}, {ResultError, "error"},
		{SectionReady, "ready"}, {SectionTransient, "transient"}, {SectionPermissionDenied, "permission_denied"},
		{SectionRefused, "refused"}, {SectionError, "error"},
	} {
		if c.got != c.want {
			t.Errorf("label value = %q, want %q", c.got, c.want)
		}
	}
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := depsv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func ago(d time.Duration) *metav1.Time {
	t := metav1.NewTime(time.Now().Add(-d))
	return &t
}

func appDeps(namespace, name string, status depsv1alpha1.AppDependenciesStatus) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Status: status}
}

func ledgerEntry(resourceType, name string, state depsv1alpha1.ManagedResourceState) depsv1alpha1.ManagedResource {
	return depsv1alpha1.ManagedResource{Type: resourceType, Name: name, State: state}
}

func TestStateCollector_ManagedPendingAndRDS(t *testing.T) {
	grace := map[string]time.Duration{"rds": time.Hour, "s3": 7 * 24 * time.Hour}

	rdsAvailable := ledgerEntry("rds", "db", depsv1alpha1.ManagedResourceStateVerified)
	rdsAvailable.ObservedStatus = "available"
	rdsModifying := ledgerEntry("rds", "db2", depsv1alpha1.ManagedResourceStateVerified)
	rdsModifying.ObservedStatus = "modifying"
	rdsModifying.PendingDeletionSince = ago(2 * time.Hour) // past RDS's one-hour grace
	notYetObserved := ledgerEntry("rds", "db3", depsv1alpha1.ManagedResourceStateCreating)

	sqsStuck := ledgerEntry("sqs", "events", depsv1alpha1.ManagedResourceStateVerified)
	sqsStuck.PendingDeletionSince = ago(8 * 24 * time.Hour) // past the 7-day default, sqs has no entry in grace
	s3Fresh := ledgerEntry("s3", "b", depsv1alpha1.ManagedResourceStateVerified)
	s3Fresh.PendingDeletionSince = ago(24 * time.Hour)
	rdsFresh := ledgerEntry("rds", "db4", depsv1alpha1.ManagedResourceStateVerified)
	rdsFresh.PendingDeletionSince = ago(10 * time.Minute)

	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		appDeps("team-a", "one", depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{
			ledgerEntry("sqs", "orders", depsv1alpha1.ManagedResourceStateVerified),
			ledgerEntry("sqs", "dlq", depsv1alpha1.ManagedResourceStateCreating),
			rdsAvailable, rdsModifying, notYetObserved, rdsFresh,
		}}),
		appDeps("team-b", "two", depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{sqsStuck, s3Fresh}}),
	).Build()

	got := gather(t, NewStateCollector(c, grace))

	want := map[string]float64{
		"cloudctl_managed_resources{state=Creating,type=sqs}":       1,
		"cloudctl_managed_resources{state=Verified,type=sqs}":       2,
		"cloudctl_managed_resources{state=Verified,type=rds}":       3,
		"cloudctl_managed_resources{state=Creating,type=rds}":       1,
		"cloudctl_managed_resources{state=Verified,type=s3}":        1,
		"cloudctl_pending_deletion_resources{stuck=true,type=rds}":  1,
		"cloudctl_pending_deletion_resources{stuck=false,type=rds}": 1,
		"cloudctl_pending_deletion_resources{stuck=true,type=sqs}":  1,
		"cloudctl_pending_deletion_resources{stuck=false,type=s3}":  1,
		"cloudctl_rds_instances{status=available}":                  1,
		"cloudctl_rds_instances{status=modifying}":                  1,
	}
	for key, value := range want {
		if g, ok := got[key]; !ok || g != value {
			t.Errorf("%s = %v (present=%v), want %v", key, g, ok, value)
		}
	}
	for key := range got {
		if strings.HasPrefix(key, "cloudctl_app_") {
			continue // covered by the next test
		}
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected series %s", key)
		}
	}
}

func TestStateCollector_PerAppReadiness_AndNoStaleSeriesAfterDelete(t *testing.T) {
	healthy := appDeps("team-a", "healthy", depsv1alpha1.AppDependenciesStatus{Conditions: []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue},
		{Type: "SQSReady", Status: metav1.ConditionTrue},
		{Type: "IAMReady", Status: metav1.ConditionFalse},
		{Type: "ConnectionInfoReady", Status: metav1.ConditionTrue},
	}})
	brandNew := appDeps("team-a", "new", depsv1alpha1.AppDependenciesStatus{})
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(healthy, brandNew).Build()
	collector := NewStateCollector(c, nil)

	got := gather(t, collector)
	want := map[string]float64{
		"cloudctl_app_ready{name=healthy,namespace=team-a}":                                1,
		"cloudctl_app_ready{name=new,namespace=team-a}":                                    0,
		"cloudctl_app_section_ready{name=healthy,namespace=team-a,section=sqs}":            1,
		"cloudctl_app_section_ready{name=healthy,namespace=team-a,section=iam}":            0,
		"cloudctl_app_section_ready{name=healthy,namespace=team-a,section=connectioninfo}": 1,
		"cloudctl_app_generation_lag{name=healthy,namespace=team-a}":                       0,
		"cloudctl_app_generation_lag{name=new,namespace=team-a}":                           0,
	}
	for key, value := range want {
		if g, ok := got[key]; !ok || g != value {
			t.Errorf("%s = %v (present=%v), want %v", key, g, ok, value)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d series, want %d: %v", len(got), len(want), got)
	}

	if err := c.Delete(t.Context(), healthy); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	after := gather(t, collector)
	for key := range after {
		if strings.Contains(key, "name=healthy") {
			t.Errorf("series %s outlived its AppDependencies", key)
		}
	}
	if _, ok := after["cloudctl_app_ready{name=new,namespace=team-a}"]; !ok {
		t.Error("expected the remaining AppDependencies to keep its series")
	}
}

// A failed list must produce no series at all: absent reads as unknown,
// where zeros would read as "nothing is stuck".
func TestStateCollector_ListFailureEmitsNothing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(appDeps("team-a", "one", depsv1alpha1.AppDependenciesStatus{})).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("cache not synced")
			},
		}).Build()

	if got := gather(t, NewStateCollector(c, nil)); len(got) != 0 {
		t.Errorf("expected no series when the list fails, got %v", got)
	}
}

type noDeepCopyRecorder struct {
	client.Reader
	lists, noDeepCopy int
}

func (r *noDeepCopyRecorder) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var applied client.ListOptions
	applied.ApplyOptions(opts)
	r.lists++
	if applied.UnsafeDisableDeepCopy != nil && *applied.UnsafeDisableDeepCopy {
		r.noDeepCopy++
	}
	return r.Reader.List(ctx, list, opts...)
}

// The collector lists every CR on every scrape and only reads them.
func TestStateCollector_ListsWithoutDeepCopying(t *testing.T) {
	recorder := &noDeepCopyRecorder{Reader: fake.NewClientBuilder().WithScheme(scheme(t)).Build()}
	gather(t, NewStateCollector(recorder, nil))
	if recorder.lists == 0 || recorder.noDeepCopy != recorder.lists {
		t.Errorf("%d of %d lists opted out of the deep copy, want all of at least one", recorder.noDeepCopy, recorder.lists)
	}
}

// A spec change the operator has not reconciled yet shows as lag; a new CR
// that has not been seen at all is one generation behind.
func TestStateCollector_GenerationLag(t *testing.T) {
	caughtUp := appDeps("team-a", "caught-up", depsv1alpha1.AppDependenciesStatus{Conditions: []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 4},
	}})
	caughtUp.Generation = 4
	behind := appDeps("team-a", "behind", depsv1alpha1.AppDependenciesStatus{Conditions: []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 2},
	}})
	behind.Generation = 5
	unseen := appDeps("team-a", "unseen", depsv1alpha1.AppDependenciesStatus{})
	unseen.Generation = 1
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(caughtUp, behind, unseen).Build()

	got := gather(t, NewStateCollector(c, nil))

	for name, want := range map[string]float64{"caught-up": 0, "behind": 3, "unseen": 1} {
		key := "cloudctl_app_generation_lag{name=" + name + ",namespace=team-a}"
		if g, ok := got[key]; !ok || g != want {
			t.Errorf("%s = %v (present=%v), want %v", key, g, ok, want)
		}
	}
}

// The gauge is the oldest re-verification per type, so one entry that stopped
// being checked shows even when its neighbours are fresh.
func TestStateCollector_VerifiedAgeIsTheOldestPerType(t *testing.T) {
	fresh := ledgerEntry("sqs", "fresh", depsv1alpha1.ManagedResourceStateVerified)
	fresh.LastVerifiedAt = ago(time.Minute)
	stale := ledgerEntry("sqs", "stale", depsv1alpha1.ManagedResourceStateVerified)
	stale.LastVerifiedAt = ago(2 * time.Hour)
	neverVerified := ledgerEntry("s3", "creating", depsv1alpha1.ManagedResourceStateCreating)
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		appDeps("team-a", "app", depsv1alpha1.AppDependenciesStatus{ManagedResources: []depsv1alpha1.ManagedResource{fresh, stale, neverVerified}}),
	).Build()

	got := gather(t, NewStateCollector(c, nil))

	if age := got["cloudctl_resource_verified_age_seconds{type=sqs}"]; age < 7200 || age > 7260 {
		t.Errorf("sqs verified age = %v, want about 7200 (the stale entry)", age)
	}
	if _, ok := got["cloudctl_resource_verified_age_seconds{type=s3}"]; ok {
		t.Error("a type with no verification time yet should have no series, not a zero age")
	}
}
