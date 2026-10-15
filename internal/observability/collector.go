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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// DefaultPendingDeletionGrace is how long a resource may sit pending
// deletion before it is reported stuck, for resource types that have no
// entry of their own in the grace map given to NewStateCollector.
const DefaultPendingDeletionGrace = 7 * 24 * time.Hour

const (
	rdsType         = "rds"
	scrapeListLimit = 10 * time.Second
)

// StateCollector reports the operator's view of what it manages, computed
// from the AppDependencies in the manager's cache on every scrape. Nothing
// is stored, so there is no state to keep in sync and a deleted CR's series
// simply stops being emitted.
type StateCollector struct {
	reader client.Reader
	grace  map[string]time.Duration

	managed         *prometheus.Desc
	pendingDeletion *prometheus.Desc
	rdsInstances    *prometheus.Desc
	appReady        *prometheus.Desc
	appSectionReady *prometheus.Desc
}

// NewStateCollector builds the collector. graceByType gives, per ledger
// resource type, how long a resource may sit pending deletion before it is
// reported stuck; it comes from the resource packages' own constants, which
// this package cannot import without a cycle.
func NewStateCollector(reader client.Reader, graceByType map[string]time.Duration) *StateCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(namespace, "", name), help, labels, nil)
	}
	return &StateCollector{
		reader: reader,
		grace:  graceByType,
		managed: desc("managed_resources",
			"Resources the operator manages, by type and ledger state (Creating, TagPending, Verified, NeedsVerification).",
			"type", "state"),
		pendingDeletion: desc("pending_deletion_resources",
			"Resources removed from spec and awaiting deletion, by type; stuck=\"true\" once past that type's grace period.",
			"type", "stuck"),
		rdsInstances: desc("rds_instances",
			"RDS instances by the status AWS last reported for them.",
			"status"),
		appReady: desc("app_ready",
			"1 if the AppDependencies' aggregate Ready condition is True, else 0.",
			"namespace", "name"),
		appSectionReady: desc("app_section_ready",
			"1 if a section's Ready condition is True, else 0, per AppDependencies and section.",
			"namespace", "name", "section"),
	}
}

func (c *StateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.managed
	ch <- c.pendingDeletion
	ch <- c.rdsInstances
	ch <- c.appReady
	ch <- c.appSectionReady
}

func (c *StateCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeListLimit)
	defer cancel()

	var list depsv1alpha1.AppDependenciesList
	if err := c.reader.List(ctx, &list); err != nil {
		// Emit nothing rather than zeros: absent series read as "unknown",
		// while zeros would read as "nothing is stuck".
		logf.Log.WithName("metrics").Error(err, "Listing AppDependencies for the state collector failed; skipping this scrape's state metrics")
		return
	}

	type pair struct{ a, b string }
	managed := map[pair]int{}
	pending := map[pair]int{}
	rdsStatus := map[string]int{}
	now := time.Now()

	for i := range list.Items {
		cr := &list.Items[i]

		for _, r := range cr.Status.ManagedResources {
			managed[pair{r.Type, string(r.State)}]++
			if r.PendingDeletionSince != nil {
				stuck := now.Sub(r.PendingDeletionSince.Time) > c.graceFor(r.Type)
				pending[pair{r.Type, boolLabel(stuck)}]++
			}
			if r.Type == rdsType && r.ObservedStatus != "" {
				rdsStatus[r.ObservedStatus]++
			}
		}

		ready := 0.0
		for _, cond := range cr.Status.Conditions {
			if cond.Type == "Ready" {
				ready = condValue(cond.Status)
				continue
			}
			if section, ok := strings.CutSuffix(cond.Type, "Ready"); ok && section != "" {
				ch <- prometheus.MustNewConstMetric(c.appSectionReady, prometheus.GaugeValue, condValue(cond.Status),
					cr.Namespace, cr.Name, strings.ToLower(section))
			}
		}
		ch <- prometheus.MustNewConstMetric(c.appReady, prometheus.GaugeValue, ready, cr.Namespace, cr.Name)
	}

	for k, n := range managed {
		ch <- prometheus.MustNewConstMetric(c.managed, prometheus.GaugeValue, float64(n), k.a, k.b)
	}
	for k, n := range pending {
		ch <- prometheus.MustNewConstMetric(c.pendingDeletion, prometheus.GaugeValue, float64(n), k.a, k.b)
	}
	for status, n := range rdsStatus {
		ch <- prometheus.MustNewConstMetric(c.rdsInstances, prometheus.GaugeValue, float64(n), status)
	}
}

func (c *StateCollector) graceFor(resourceType string) time.Duration {
	if d, ok := c.grace[resourceType]; ok {
		return d
	}
	return DefaultPendingDeletionGrace
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func condValue(status metav1.ConditionStatus) float64 {
	if status == metav1.ConditionTrue {
		return 1
	}
	return 0
}
