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
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const alertRulesTemplate = "../../charts/chart/templates/prometheus/controller-manager-alert-rules.yaml"

// externalMetrics are series the rules use that controller-runtime, not this
// operator, exports.
var externalMetrics = []string{"workqueue_depth"}

func alertRules(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(alertRulesTemplate))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func registeredMetricNames(t *testing.T) []string {
	t.Helper()
	descs := make(chan *prometheus.Desc, 64)
	collectors := []prometheus.Collector{
		AWSRequests, AWSDestructiveRequests, AWSRequestDuration, AWSRateLimitWait, SectionReconciles,
		SectionReconcileDuration, Events, AWSClientsReady, NewStateCollector(nil, nil),
	}
	for _, c := range collectors {
		c.Describe(descs)
	}
	close(descs)
	fqName := regexp.MustCompile(`fqName: "([^"]+)"`)
	var names []string
	for d := range descs {
		names = append(names, fqName.FindStringSubmatch(d.String())[1])
	}
	return names
}

func TestAlertRules_OnlyUseMetricsThatExist(t *testing.T) {
	known := append(registeredMetricNames(t), externalMetrics...)
	used := regexp.MustCompile(`\b(?:cloudctl|workqueue)_[a-z_]+\b`).FindAllString(alertRules(t), -1)
	if len(used) == 0 {
		t.Fatal("found no metric names in the alert rules")
	}
	for _, name := range used {
		if !slices.Contains(known, name) {
			t.Errorf("alert rules use %q, which no collector defines", name)
		}
	}
}

// A reason the rules match on must start at zero, or its first event after a
// restart is invisible to increase() and the alert never fires.
func TestAlertRules_EventReasonsStartAtZero(t *testing.T) {
	matched := regexp.MustCompile(`reason=~"([^"]+)"`).FindAllStringSubmatch(alertRules(t), -1)
	if len(matched) == 0 {
		t.Fatal("found no reason matchers in the alert rules")
	}
	for _, m := range matched {
		for reason := range strings.SplitSeq(m[1], "|") {
			if !slices.Contains(AlertedEventReasons, reason) {
				t.Errorf("alert rules match event reason %q, which is not in AlertedEventReasons", reason)
			}
		}
	}

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	started := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "cloudctl_events_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" {
					started[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	for _, reason := range AlertedEventReasons {
		if _, ok := started[reason]; !ok {
			t.Errorf("no cloudctl_events_total series for %q at start", reason)
		}
	}
}

func TestInitAWSRequests_CreatesEveryResultAtZero(t *testing.T) {
	InitAWSRequests("init_probe")

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "cloudctl_aws_requests_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["service"] == "init_probe" {
				got[labels["result"]] = m.GetCounter().GetValue()
			}
		}
	}
	for _, result := range AWSResults {
		value, ok := got[result]
		if !ok || value != 0 {
			t.Errorf("result %q: series present = %v, value = %v; want present at 0", result, ok, value)
		}
	}
}
