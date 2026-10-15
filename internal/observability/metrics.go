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

// Package observability defines the operator's own Prometheus metrics, on
// top of what controller-runtime already exposes (reconcile and workqueue
// metrics, REST client metrics, leader election).
//
// Counters and histograms use fixed, small label sets, never user input, so
// the number of series does not grow with the number of CRs. The scrape-time
// gauges in collector.go are the exception: they carry the CR's name, and
// are computed from the cache on each scrape, so a deleted CR's series
// disappears with it and nothing has to be cleaned up.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const namespace = "cloudctl"

// Outcomes of one AWS API operation, for AWSRequests.
const (
	ResultSuccess          = "success"
	ResultThrottled        = "throttled"
	ResultPermissionDenied = "permission_denied"
	ResultNotFound         = "not_found"
	ResultError            = "error"
)

// Outcomes of one section's reconcile, for SectionReconciles
const (
	SectionReady            = "ready"
	SectionTransient        = "transient"
	SectionPermissionDenied = "permission_denied"
	SectionRefused          = "refused"
	SectionError            = "error"
)

var (
	// AWSRequests counts AWS API operations by service and outcome. One
	// operation is one SDK call, however many times the SDK retried it.
	AWSRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "aws", Name: "requests_total",
		Help: "AWS API operations by service and result (success, throttled, permission_denied, not_found, error).",
	}, []string{"service", "result"})

	// AWSRequestDuration is the wall time of one operation, including any
	// SDK retries and time spent waiting on this operator's own rate limiter.
	AWSRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "aws", Name: "request_duration_seconds",
		Help:    "Wall time of an AWS API operation, including SDK retries and rate-limiter waits, by service.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15},
	}, []string{"service"})

	// AWSRateLimitWait is the total time spent waiting on this operator's
	// own rate limiter. rate() of it is the average number of calls
	// queued behind the limiter at any moment.
	AWSRateLimitWait = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "aws", Name: "rate_limit_wait_seconds_total",
		Help: "Total seconds spent waiting on this operator's own outgoing rate limiter, by service.",
	}, []string{"service"})

	// SectionReconciles counts each section's reconcile by outcome. Section
	// is the condition type lowercased without its "Ready" suffix: sqs, sns,
	// dynamodb, s3, rds, kms, alarms, iam.
	SectionReconciles = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "section", Name: "reconcile_total",
		Help: "Section reconciles by section and result (ready, transient, permission_denied, refused, error).",
	}, []string{"section", "result"})

	// SectionReconcileDuration is how long one section took, including its
	// AWS calls and any waiting on them.
	SectionReconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "section", Name: "reconcile_duration_seconds",
		Help:    "Wall time of one section's reconcile, by section.",
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 15, 60, 300},
	}, []string{"section"})

	// Events counts every Kubernetes event the operator emits, by type and
	// reason. The reasons are a fixed set in the code (QueueMissing,
	// InstanceMissing, KeyMissing, InstanceAdopted, ...), so this is both an
	// audit trail of adoptions and the signal that something is being
	// changed by hand outside the operator.
	Events = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "events_total",
		Help: "Kubernetes events emitted by the operator, by type (Normal, Warning) and reason.",
	}, []string{"type", "reason"})

	// AWSClientsReady is 1 once the AWS clients have initialized, 0 while
	// the manager is up but cannot reach AWS.
	AWSClientsReady = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: "aws", Name: "clients_ready",
		Help: "1 once the AWS clients are initialized, 0 while the manager is running without them.",
	})
)

func init() {
	metrics.Registry.MustRegister(
		AWSRequests,
		AWSRequestDuration,
		AWSRateLimitWait,
		SectionReconciles,
		SectionReconcileDuration,
		Events,
		AWSClientsReady,
	)
}
