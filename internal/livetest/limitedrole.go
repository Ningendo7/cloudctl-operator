//go:build live

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

package livetest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
)

// LimitedConfig returns a copy of cfg whose credentials are those of a
// temporary role allowed exactly actions, on every resource. It is how a test
// finds out whether an action is really needed: the administrator running the
// live tests passes every check, so only a role without the action can show
// that a call fails without it. The role is deleted when the test ends. It
// waits until the role can be assumed; the caller still has to wait for the
// role's policy to take effect, which IAM applies eventually.
func LimitedConfig(t *testing.T, cfg aws.Config, actions []string) aws.Config {
	t.Helper()
	ctx := context.Background()
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity() error = %v", err)
	}
	iamClient := iam.NewFromConfig(cfg)

	name := fmt.Sprintf("cloudctl-live-limited-%d", time.Now().UnixNano()%1_000_000_000)
	trust := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::%s:root"},"Action":"sts:AssumeRole"}]}`, *identity.Account)
	var created *iam.CreateRoleOutput
	retryThrottled(t, "real CreateRole() for a limited role", func() (err error) {
		created, err = iamClient.CreateRole(ctx, &iam.CreateRoleInput{RoleName: &name, AssumeRolePolicyDocument: &trust})
		return err
	})
	policyName := "limited"
	t.Cleanup(func() {
		_, _ = iamClient.DeleteRolePolicy(context.Background(), &iam.DeleteRolePolicyInput{RoleName: &name, PolicyName: &policyName})
		_, _ = iamClient.DeleteRole(context.Background(), &iam.DeleteRoleInput{RoleName: &name})
	})

	quoted := make([]string, len(actions))
	for i, a := range actions {
		quoted[i] = fmt.Sprintf("%q", a)
	}
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[%s],"Resource":"*"}]}`, strings.Join(quoted, ","))
	retryThrottled(t, "real PutRolePolicy() for a limited role", func() error {
		_, err := iamClient.PutRolePolicy(ctx, &iam.PutRolePolicyInput{RoleName: &name, PolicyName: &policyName, PolicyDocument: &policy})
		return err
	})

	provider := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), *created.Role.Arn))
	deadline := time.Now().Add(90 * time.Second)
	for {
		if _, err := provider.Retrieve(ctx); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("the limited role never became assumable: %v", err)
		}
		time.Sleep(3 * time.Second)
	}
	limited := cfg.Copy()
	limited.Credentials = provider
	return limited
}

// WaitUntil polls check until it returns true or the time is up.
func WaitUntil(within time.Duration, check func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// CheckActionNeeded judges a pair of calls made with and without an action
// the permissions table lists as needed: the call must work with it, and be
// refused without it. A call that works without the action is only logged:
// granting it is harmless, and AWS may still document it as required. One
// refused for another action means the table is missing that one.
func CheckActionNeeded(t *testing.T, action string, with, without error) {
	t.Helper()
	if with != nil {
		t.Fatalf("the call fails even with %s allowed: %v. The permissions table is incomplete", action, with)
	}
	switch {
	case without == nil:
		t.Logf("%s is listed as needed, but the call worked without it on this account", action)
	case !cloudctlaws.IsPermissionDenied(without):
		t.Errorf("without %s the call failed, but not as a permission error: %v", action, without)
	case strings.Contains(without.Error(), "not authorized to perform") && !strings.Contains(without.Error(), action):
		t.Errorf("without %s the call is refused for a different action, which the permissions table may be missing: %v", action, without)
	default:
		t.Logf("confirmed: without %s the call is refused: %v", action, without)
	}
}

// retryThrottled repeats call while IAM answers that the rate is exceeded,
// which a burst of role calls from the tests before it can cause.
func retryThrottled(t *testing.T, what string, call func() error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		err := call()
		if err == nil {
			return
		}
		if !strings.Contains(err.Error(), "Throttling") || time.Now().After(deadline) {
			t.Fatalf("%s error = %v", what, err)
		}
		time.Sleep(5 * time.Second)
	}
}
