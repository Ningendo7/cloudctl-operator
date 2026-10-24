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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/controller"
	"github.com/Ningendo7/cloudctl-operator/internal/observability"
	// +kubebuilder:scaffold:imports
)

// awsClientInitRetryInterval is how often a failed startup AWS identity
// check (bad/revoked/rotated-out credentials) is retried, rather than
// crashing the process. See awsBootstrap for how the pod stays alive
// meanwhile.
const awsClientInitRetryInterval = 30 * time.Second

// credentialRecheckInterval is how often an already-running manager
// re-validates its AWS credentials via STS, independent of NewClients'
// one-time startup check. Matches DriftDetectionInterval for consistency.
const credentialRecheckInterval = 5 * time.Minute

// waitForAWSClients retries newClients (production callers always pass
// cloudctlaws.NewClients; a test passes a fake) until it succeeds or ctx is
// cancelled - see awsClientInitRetryInterval's doc comment for why this
// isn't just a single fatal attempt.
func waitForAWSClients(
	ctx context.Context,
	interval time.Duration,
	newClients func(context.Context) (*cloudctlaws.Clients, error),
) (*cloudctlaws.Clients, error) {
	var clients *cloudctlaws.Clients
	err := wait.PollUntilContextCancel(ctx, interval, true, func(ctx context.Context) (bool, error) {
		var err error
		clients, err = newClients(ctx)
		if err != nil {
			setupLog.Info("Not yet able to initialize AWS clients, will retry",
				"error", err.Error(), "retryInterval", interval)
			return false, nil
		}
		return true, nil
	})
	return clients, err
}

// awsBootstrap retries AWS setup after the manager starts, so the probe
// server is up and liveness doesn't kill the pod mid-retry.
type awsBootstrap struct {
	newClients func(context.Context) (*cloudctlaws.Clients, error)
	interval   time.Duration
	onReady    func(context.Context, *cloudctlaws.Clients) error
	ready      atomic.Bool
}

// NeedLeaderElection is false so standby replicas become ready too.
func (b *awsBootstrap) NeedLeaderElection() bool { return false }

func (b *awsBootstrap) Start(ctx context.Context) error {
	clients, err := waitForAWSClients(ctx, b.interval, b.newClients)
	if err != nil {
		// Only reachable via ctx cancellation, i.e. a normal shutdown.
		return nil //nolint:nilerr // not a failure
	}
	if err := b.onReady(ctx, clients); err != nil {
		return err
	}
	b.ready.Store(true)
	observability.AWSClientsReady.Set(1)
	setupLog.Info("AWS clients initialized")
	return nil
}

func (b *awsBootstrap) readyCheck(_ *http.Request) error {
	if !b.ready.Load() {
		return errors.New("AWS clients not initialized yet")
	}
	return nil
}

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(depsv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// config holds the command-line flags.
type config struct {
	metricsAddr                                      string
	metricsCertPath, metricsCertName, metricsCertKey string
	webhookCertPath, webhookCertName, webhookCertKey string
	enableLeaderElection                             bool
	probeAddr                                        string
	secureMetrics                                    bool
	enableHTTP2                                      bool
	oidcProviderARN                                  string
	oidcProviderURL                                  string
	podBaseSecurityGroupIDs                          string
	maxConcurrentReconciles                          int
	releaseTags                                      bool
	zapOpts                                          zap.Options
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&c.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&c.enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&c.secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&c.webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&c.webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&c.webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&c.metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&c.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&c.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&c.enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&c.oidcProviderARN, "oidc-provider-arn", "",
		"ARN of this cluster's IAM OIDC identity provider, required for the "+
			"IRSA trust policy on every IAM role this operator derives.")
	flag.StringVar(&c.oidcProviderURL, "oidc-provider-url", "",
		"URL (without https://) of this cluster's IAM OIDC identity provider, "+
			"required for the IRSA trust policy on every IAM role this operator derives.")
	flag.StringVar(&c.podBaseSecurityGroupIDs, "pod-base-security-group-ids", "",
		"Comma-separated security group IDs that the pods of a CR using rds.consumes keep next to the "+
			"database group, normally the EKS cluster security group. Security Groups for Pods replaces a "+
			"pod's groups with the ones it is given, so without these the pods can lose Service, load "+
			"balancer and probe traffic.")
	flag.BoolVar(&c.releaseTags, "release-tags", true,
		"Leave cloudctl.io/released-from and cloudctl.io/released-at tags on a retained resource when the "+
			"operator lets go of it, saying which CR it came from. They are never used as ownership.")
	flag.IntVar(&c.maxConcurrentReconciles, "max-concurrent-reconciles", 5,
		"Maximum number of AppDependencies CRs reconciled in parallel. AWS-side "+
			"call volume is independently bounded by the per-service rate-limit "+
			"env vars (AWS_SQS_RATE_LIMIT_QPS, etc.), so raising this mainly "+
			"improves fairness/latency at higher CR counts rather than raw AWS throughput.")
	c.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return c
}

func fatal(err error, msg string) {
	setupLog.Error(err, msg)
	os.Exit(1)
}

func main() {
	c := parseFlags()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&c.zapOpts)))

	if c.oidcProviderARN == "" || c.oidcProviderURL == "" {
		setupLog.Info("Starting without --oidc-provider-arn/--oidc-provider-url set - " +
			"any AppDependencies CR needing an IAM role will fail reconciliation until both are set")
	}

	baseSecurityGroups, err := parseSecurityGroupIDs(c.podBaseSecurityGroupIDs)
	if err != nil {
		fatal(err, "Invalid --pod-base-security-group-ids")
	}

	if limit := limitMemoryFromCgroup(cgroupRoot); limit > 0 {
		setupLog.Info("Set the Go memory limit from the container's", "bytes", limit)
	}

	ctx := ctrl.SetupSignalHandler()

	restConfig := ctrl.GetConfigOrDie()
	defer setupTracing(ctx, restConfig)()

	webhookOpts, metricsOpts := serverOptions(c)
	gracefulShutdownTimeout := 30 * time.Second
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOpts,
		WebhookServer:          webhook.NewServer(webhookOpts),
		HealthProbeBindAddress: c.probeAddr,
		LeaderElection:         c.enableLeaderElection,
		LeaderElectionID:       "687fa3e1.cloudctl.io",
		// Only cache our own Secrets/ConfigMaps, not the whole cluster's.
		Cache: cache.Options{
			ByObject:         controller.ManagedObjectCache(),
			DefaultTransform: controller.ManagedCacheDefaultTransform(),
		},
		// Inside the pod's terminationGracePeriodSeconds.
		GracefulShutdownTimeout: &gracefulShutdownTimeout,
		// Lets the standby take over at once; main exits right after mgr.Start.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		fatal(err, "Failed to start manager")
	}

	credHealth := cloudctlaws.NewCredentialHealth()
	bootstrap := newAWSBootstrap(mgr, c, baseSecurityGroups, credHealth)
	if err := mgr.Add(bootstrap); err != nil {
		fatal(err, "Failed to register AWS bootstrap")
	}

	// Reads the manager's cache on each scrape; registered here rather than
	// at controller setup so the gauges exist even before AWS is reachable.
	stateCollector := observability.NewStateCollector(mgr.GetClient(), controller.PendingDeletionGracePeriods())
	if err := metrics.Registry.Register(stateCollector); err != nil {
		fatal(err, "Failed to register the state metrics collector")
	}

	addHealthChecks(mgr, bootstrap, credHealth)

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctx); err != nil {
		fatal(err, "Failed to run manager")
	}
}

// setupTracing starts OpenTelemetry tracing and returns what flushes it at
// shutdown. Tracing is opt-in: with no OTLP endpoint configured nothing is
// initialised and every span the operator starts is a free no-op.
func setupTracing(ctx context.Context, restConfig *rest.Config) (flush func()) {
	if !observability.TracingEnabled() {
		setupLog.Info("OpenTelemetry tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
		return func() {}
	}
	shutdownTracing, err := observability.InitTracing(ctx)
	if err != nil {
		fatal(err, "Failed to initialize OpenTelemetry tracing")
	}
	restConfig.Wrap(observability.TraceKubernetesRequests)
	setupLog.Info("OpenTelemetry tracing enabled")
	return func() {
		// ctx is already cancelled by the time this runs; the flush gets
		// its own brief window.
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			setupLog.Error(err, "Failed to flush OpenTelemetry traces at shutdown")
		}
	}
}

// serverOptions builds the webhook and metrics server options from the flags.
func serverOptions(c config) (webhook.Options, metricsserver.Options) {
	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	var tlsOpts []func(*tls.Config)
	if !c.enableHTTP2 {
		tlsOpts = append(tlsOpts, func(cfg *tls.Config) {
			setupLog.Info("Disabling HTTP/2")
			cfg.NextProtos = []string{"http/1.1"}
		})
	}

	webhookOpts := webhook.Options{TLSOpts: tlsOpts}
	if len(c.webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", c.webhookCertPath, "webhook-cert-name", c.webhookCertName, "webhook-cert-key", c.webhookCertKey)

		webhookOpts.CertDir = c.webhookCertPath
		webhookOpts.CertName = c.webhookCertName
		webhookOpts.KeyName = c.webhookCertKey
	}

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsOpts := metricsserver.Options{
		BindAddress:   c.metricsAddr,
		SecureServing: c.secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if c.secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsOpts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(c.metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", c.metricsCertPath, "metrics-cert-name", c.metricsCertName, "metrics-cert-key", c.metricsCertKey)

		metricsOpts.CertDir = c.metricsCertPath
		metricsOpts.CertName = c.metricsCertName
		metricsOpts.KeyName = c.metricsCertKey
	}
	return webhookOpts, metricsOpts
}

// newAWSBootstrap builds what brings up the AWS clients and, once they are
// ready, the controller.
func newAWSBootstrap(
	mgr ctrl.Manager,
	c config,
	baseSecurityGroups []string,
	credHealth *cloudctlaws.CredentialHealth,
) *awsBootstrap {
	const rateLimitRampDuration = 30 * time.Second
	return &awsBootstrap{
		newClients: cloudctlaws.NewClients,
		interval:   awsClientInitRetryInterval,
		onReady: func(ctx context.Context, awsClients *cloudctlaws.Clients) error {
			if err := (&controller.AppDependenciesReconciler{
				Client:                  mgr.GetClient(),
				APIReader:               mgr.GetAPIReader(),
				Scheme:                  mgr.GetScheme(),
				AWSClients:              awsClients,
				Recorder:                mgr.GetEventRecorder("appdependencies-controller"),
				OIDCProviderARN:         c.oidcProviderARN,
				OIDCProviderURL:         c.oidcProviderURL,
				PodBaseSecurityGroupIDs: baseSecurityGroups,
				MaxConcurrentReconciles: c.maxConcurrentReconciles,
				DisableReleaseTags:      !c.releaseTags,
			}).SetupWithManager(mgr); err != nil {
				return fmt.Errorf("creating appdependencies controller: %w", err)
			}
			// +kubebuilder:scaffold:builder

			go cloudctlaws.RunPeriodicCheck(ctx, awsClients.STS, credentialRecheckInterval, credHealth)

			go func() {
				select {
				case <-mgr.Elected():
				case <-ctx.Done():
					return
				}
				for _, limiter := range awsClients.RateLimiters {
					go cloudctlaws.RampUp(ctx, limiter, rateLimitRampDuration)
				}
			}()
			return nil
		},
	}
}

// addHealthChecks registers the liveness and readiness probes.
func addHealthChecks(mgr ctrl.Manager, bootstrap *awsBootstrap, credHealth *cloudctlaws.CredentialHealth) {
	checks := []struct {
		name    string
		ready   bool
		checker healthz.Checker
		failMsg string
	}{
		{"healthz", false, healthz.Ping, "Failed to set up health check"},
		{"readyz", true, healthz.Ping, "Failed to set up ready check"},
		{"aws-clients", true, bootstrap.readyCheck, "Failed to set up AWS client readyz check"},
		{"aws-credentials", true, credHealth.Check, "Failed to set up AWS credential readyz check"},
	}
	for _, check := range checks {
		add := mgr.AddHealthzCheck
		if check.ready {
			add = mgr.AddReadyzCheck
		}
		if err := add(check.name, check.checker); err != nil {
			fatal(err, check.failMsg)
		}
	}
}

var securityGroupIDPattern = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)

// parseSecurityGroupIDs splits a comma-separated list of security group IDs.
// An empty string means none; anything that isn't a group ID is rejected at
// startup, since a typo here would otherwise only surface as pods that
// cannot start.
func parseSecurityGroupIDs(value string) ([]string, error) {
	var ids []string
	for id := range strings.SplitSeq(value, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if !securityGroupIDPattern.MatchString(id) {
			return nil, fmt.Errorf("%q is not a security group ID (want sg-<hex>)", id)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// memoryLimitHeadroom leaves room for memory outside Go's accounting.
const memoryLimitHeadroom = 0.9

// cgroupRoot is where the container's own cgroup is mounted.
const cgroupRoot = "/sys/fs/cgroup"

// limitMemoryFromCgroup keeps the heap under the container's memory limit;
// by default Go only collects at twice the live heap, which can overshoot it.
// An explicit GOMEMLIMIT wins. Returns the limit it set, or 0.
func limitMemoryFromCgroup(root string) int64 {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0
	}
	limit := cgroupMemoryLimit(root)
	if limit <= 0 {
		return 0
	}
	soft := int64(float64(limit) * memoryLimitHeadroom)
	debug.SetMemoryLimit(soft)
	return soft
}

// Files holding a cgroup's memory limit, relative to its mount point.
const (
	cgroupV2MemoryFile = "memory.max"
	cgroupV1MemoryFile = "memory/memory.limit_in_bytes"
)

// cgroupMemoryLimit returns the memory limit in bytes of the cgroup mounted
// at root, or 0 when it has none. It reads cgroup v2, then v1.
func cgroupMemoryLimit(root string) int64 {
	cgroup, err := os.OpenRoot(root)
	if err != nil {
		return 0
	}
	defer func() { _ = cgroup.Close() }()

	for _, file := range []string{cgroupV2MemoryFile, cgroupV1MemoryFile} {
		raw, err := cgroup.ReadFile(file)
		if err != nil {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		// v2 writes "max" for no limit; v1 writes a number just under 2^63.
		if err != nil || n >= 1<<60 {
			return 0
		}
		return n
	}
	return 0
}
