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

package aws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// STSClient is the subset of the AWS SDK's STS client CredentialHealth
// needs - just enough to periodically re-validate that this pod's IRSA
// credentials still work, independent of NewClients' own one-time startup
// check.
type STSClient interface {
	GetCallerIdentity(ctx context.Context, in *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// CredentialHealth tracks whether this pod's AWS credentials were valid as
// of the most recent periodic check, for wiring into a readyz probe.
// NewClients' startup check only ever runs once; so a revoked
// IRSA trust policy or deleted role would go unnoticed until some
// unrelated AWS call happened to fail during normal reconciliation - and
// only for a CR actually being reconciled, so a standby replica or a
// cluster with no CRs yet would never notice at all.
type CredentialHealth struct {
	mu      sync.RWMutex
	healthy bool
}

// NewCredentialHealth returns a tracker that starts healthy - the
// manager's own startup check (NewClients) already proved credentials work
// once, before this is ever constructed.
func NewCredentialHealth() *CredentialHealth {
	return &CredentialHealth{healthy: true}
}

// Check implements controller-runtime's healthz.Checker, for use with
// mgr.AddReadyzCheck. Returns the most recent periodic check's result
// without making a new AWS call itself - readyz probes are polled
// frequently and should stay cheap.
func (h *CredentialHealth) Check(_ *http.Request) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.healthy {
		return errors.New("AWS credentials failed their last periodic re-check")
	}
	return nil
}

func (h *CredentialHealth) setHealthy(healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy = healthy
}

// RunPeriodicCheck calls GetCallerIdentity on every tick of interval until
// ctx is cancelled, updating health's result each time. Deliberately not
// gated on leader election: a standby replica needs its own credential
// health known before it's ever asked to take over, not just the leader's.
//
// A single failed attempt is reported immediately via health, rather than
// debounced here - the readyz probe this feeds already has its own
// failure-threshold/period configured at the Kubernetes level, which is
// the right layer to absorb one transient blip, not a second copy of that
// same logic here.
func RunPeriodicCheck(ctx context.Context, stsClient STSClient, interval time.Duration, health *CredentialHealth) {
	log := logf.Log.WithName("credential-health")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err != nil {
				log.Info("AWS credential re-check failed", "error", err.Error())
				health.setHealthy(false)
				continue
			}
			health.setHealthy(true)
		}
	}
}
