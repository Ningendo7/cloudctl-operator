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

const (
	subsystemAWS     = "aws"
	subsystemSection = "section"
	labelService     = "service"
	labelSection     = "section"
	labelResult      = "result"
)

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
		Namespace: namespace, Subsystem: subsystemAWS, Name: "requests_total",
		Help: "AWS API operations by service and result (success, throttled, permission_denied, not_found, error).",
	}, []string{labelService, labelResult})

	// AWSRequestDuration is the wall time of one operation, including any
	// SDK retries and time spent waiting on this operator's own rate limiter.
	AWSRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: subsystemAWS, Name: "request_duration_seconds",
		Help:    "Wall time of an AWS API operation, including SDK retries and rate-limiter waits, by service.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15},
	}, []string{labelService})

	// AWSRateLimitWait is the total time spent waiting on this operator's
	// own rate limiter. rate() of it is the average number of calls
	// queued behind the limiter at any moment.
	AWSRateLimitWait = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystemAWS, Name: "rate_limit_wait_seconds_total",
		Help: "Total seconds spent waiting on this operator's own outgoing rate limiter, by service.",
	}, []string{labelService})

	// SectionReconciles counts each section's reconcile by outcome. Section
	// is the condition type lowercased without its "Ready" suffix: sqs, sns,
	// dynamodb, s3, rds, kms, alarms, iam.
	SectionReconciles = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystemSection, Name: "reconcile_total",
		Help: "Section reconciles by section and result (ready, transient, permission_denied, refused, error).",
	}, []string{labelSection, labelResult})

	// SectionReconcileDuration is how long one section took, including its
	// AWS calls and any waiting on them.
	SectionReconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: subsystemSection, Name: "reconcile_duration_seconds",
		Help:    "Wall time of one section's reconcile, by section.",
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 15, 60, 300},
	}, []string{labelSection})

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
		Namespace: namespace, Subsystem: subsystemAWS, Name: "clients_ready",
		Help: "1 once the AWS clients are initialized, 0 while the manager is running without them.",
	})
)

// AlertedEventReasons are the Warning event reasons the shipped alert rules
// match on. Prometheus' increase() cannot see the first increment of a series
// that only comes into existence at 1, so these are created at zero on start;
// without that, the first such event after every restart would never alert.
var AlertedEventReasons = []string{
	"InstanceMissing", "KeyMissing", "KeyDisabled", "KeyUnavailable",
	"QueueMissing", "TopicMissing", "BucketMissing",
}

// AWSResults is every outcome AWSRequests can carry.
var AWSResults = []string{ResultSuccess, ResultThrottled, ResultPermissionDenied, ResultNotFound, ResultError}

// InitAWSRequests creates a service's request series at zero, for the same
// reason as AlertedEventReasons: the first permission_denied or throttled call
// would otherwise be invisible to the rules that watch them.
func InitAWSRequests(service string) {
	for _, result := range AWSResults {
		AWSRequests.WithLabelValues(service, result).Add(0)
	}
}

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
	for _, reason := range AlertedEventReasons {
		Events.WithLabelValues("Warning", reason).Add(0)
	}
}
