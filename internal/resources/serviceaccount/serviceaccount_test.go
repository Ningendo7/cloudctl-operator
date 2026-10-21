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

package serviceaccount

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := depsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("depsv1alpha1.AddToScheme: %v", err)
	}
	return scheme
}

func newCR(namespace, name, serviceAccountName string) *depsv1alpha1.AppDependencies {
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       "test-uid",
		},
		Spec: depsv1alpha1.AppDependenciesSpec{
			ServiceAccountName: serviceAccountName,
		},
	}
}

func getSA(t *testing.T, c client.Client, namespace, name string) *corev1.ServiceAccount {
	t.Helper()
	sa := &corev1.ServiceAccount{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, sa); err != nil {
		t.Fatalf("get ServiceAccount %s/%s: %v", namespace, name, err)
	}
	return sa
}

func TestEnsure_NoRoleARN_IsNoop(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")
	cr.Status.ServiceAccountName = "whatever"

	got, err := Ensure(context.Background(), c, cr, "")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got != "whatever" {
		t.Errorf("expected status name to pass through unchanged, got %q", got)
	}
}

func TestEnsure_DefaultName_CreatesOwnedServiceAccount(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")

	got, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/ns-checkout-role")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got != "checkout" {
		t.Errorf("expected default name %q, got %q", "checkout", got)
	}

	sa := getSA(t, c, "ns", "checkout")
	if sa.Annotations[RoleARNAnnotation] != "arn:aws:iam::123456789012:role/ns-checkout-role" {
		t.Errorf("role-arn annotation = %q", sa.Annotations[RoleARNAnnotation])
	}
	owner := metav1.GetControllerOf(sa)
	if owner == nil || owner.Name != "checkout" || owner.Kind != "AppDependencies" {
		t.Errorf("expected owner reference to the CR on an operator-created default-named ServiceAccount, got %+v", owner)
	}
}

// Creating a ServiceAccount the user named but never made would let a CR
// author create ServiceAccounts their own RBAC may not allow, and nothing
// would ever delete it.
func TestEnsure_ExplicitNameThatDoesNotExist_IsReportedNotCreated(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "custom-sa")

	_, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/ns-checkout-role")
	assertReason(t, err, reasons.ServiceAccountNotFound)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "custom-sa"}, &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the ServiceAccount not to be created, got %v", err)
	}
}

func TestEnsure_PreExisting_MergesWithoutClobberingOtherAnnotations(t *testing.T) {
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "checkout",
			Annotations: map[string]string{"team.example.com/owner": "payments"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(existing).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	sa := getSA(t, c, "ns", "checkout")
	if sa.Annotations["team.example.com/owner"] != "payments" {
		t.Errorf("expected pre-existing unrelated annotation to survive, got %q", sa.Annotations["team.example.com/owner"])
	}
	if sa.Annotations[RoleARNAnnotation] == "" {
		t.Error("expected role-arn annotation to be merged in")
	}
	// A pre-existing object is never adopted, even at the default name.
	if owner := metav1.GetControllerOf(sa); owner != nil {
		t.Errorf("expected no owner reference on a pre-existing ServiceAccount, got %+v", owner)
	}
}

// TestEnsure_DifferentCROwnsTarget_Refused guards against two CRs sharing
// one serviceAccountName silently taking turns owning its IRSA role.
func TestEnsure_DifferentCROwnsTarget_Refused(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(plainSA("shared-sa")).Build()

	owner := newCR("ns", "checkout", "shared-sa")
	if _, err := Ensure(context.Background(), c, owner, "arn:aws:iam::123456789012:role/checkout"); err != nil {
		t.Fatalf("Ensure (establish ownership): %v", err)
	}

	intruder := &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fraud-detector", UID: "intruder-uid"},
		Spec:       depsv1alpha1.AppDependenciesSpec{ServiceAccountName: "shared-sa"},
	}
	_, err := Ensure(context.Background(), c, intruder, "arn:aws:iam::123456789012:role/fraud-detector")
	if err == nil {
		t.Fatal("expected Ensure to refuse attaching a different CR's role to an already-claimed ServiceAccount")
	}

	sa := getSA(t, c, "ns", "shared-sa")
	if sa.Annotations[RoleARNAnnotation] != "arn:aws:iam::123456789012:role/checkout" {
		t.Errorf("expected the original owner's role-arn to survive the refused claim, got %q", sa.Annotations[RoleARNAnnotation])
	}
	if sa.Annotations[cloudctlaws.OwnerTagKey] != cloudctlaws.OwnerTagValue("ns", "checkout") {
		t.Errorf("expected ownership to remain with the original claimant, got %q", sa.Annotations[cloudctlaws.OwnerTagKey])
	}
}

// TestEnsure_DefaultName_StaleOwnerFromRecreatedCR_Refused guards against
// name-match-alone counting as ownership: a deleted-and-recreated CR
// reusing the same name has a different UID.
func TestEnsure_DefaultName_StaleOwnerFromRecreatedCR_Refused(t *testing.T) {
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "checkout",
			Annotations: map[string]string{
				cloudctlaws.OwnerTagKey:    cloudctlaws.OwnerTagValue("ns", "checkout"),
				cloudctlaws.OwnerUIDTagKey: "old-uid",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(existing).Build()
	cr := newCR("ns", "checkout", "") // newCR's UID ("test-uid") differs from "old-uid"

	_, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x")
	if err == nil {
		t.Fatal("expected Ensure to refuse claiming a ServiceAccount still owned (by UID) by a stale, deleted-and-recreated CR identity")
	}
}

// TestEnsure_SameCRReconcilingAgain_UpdatesOwnRoleARN confirms the
// ownership check only blocks a *different* CR, not the owner reconciling.
func TestEnsure_SameCRReconcilingAgain_UpdatesOwnRoleARN(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(plainSA("shared-sa")).Build()
	cr := newCR("ns", "checkout", "shared-sa")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/v1"); err != nil {
		t.Fatalf("Ensure (first): %v", err)
	}
	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/v2"); err != nil {
		t.Fatalf("Ensure (second, same CR): %v", err)
	}

	sa := getSA(t, c, "ns", "shared-sa")
	if sa.Annotations[RoleARNAnnotation] != "arn:aws:iam::123456789012:role/v2" {
		t.Errorf("expected the same CR's own reconcile to update its role-arn, got %q", sa.Annotations[RoleARNAnnotation])
	}
}

// TestEnsure_PreExistingForeignSA_StampsOwnershipOnFirstClaim confirms an
// unclaimed ServiceAccount (no cloudctl marker yet) can still be claimed.
func TestEnsure_PreExistingForeignSA_StampsOwnershipOnFirstClaim(t *testing.T) {
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "checkout",
			Annotations: map[string]string{"team.example.com/owner": "payments"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(existing).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	sa := getSA(t, c, "ns", "checkout")
	if sa.Annotations[cloudctlaws.OwnerTagKey] != cloudctlaws.OwnerTagValue("ns", "checkout") {
		t.Errorf("expected ownership annotation stamped on first claim, got %q", sa.Annotations[cloudctlaws.OwnerTagKey])
	}
	if sa.Annotations[cloudctlaws.OwnerUIDTagKey] != string(cr.UID) {
		t.Errorf("expected owner-uid annotation stamped on first claim, got %q", sa.Annotations[cloudctlaws.OwnerUIDTagKey])
	}
}

// TestEnsure_Rename_CleansUpPreviousTarget deliberately establishes the old
// target's role-arn field via a real prior Ensure call (rather than
// pre-seeding it directly onto a hand-built fixture) — server-side apply's
// per-field ownership is recorded in that object's managedFields, which
// only a real Apply populates, and the "release only removes what we
// actually own" behavior this test exists to prove wouldn't be exercised
// honestly otherwise.
func TestEnsure_Rename_CleansUpPreviousTarget(t *testing.T) {
	old := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "old-sa",
			Annotations: map[string]string{"unrelated": "keep-me"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(old, plainSA("new-sa")).Build()

	crOld := newCR("ns", "checkout", "old-sa")
	if _, err := Ensure(context.Background(), c, crOld, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure (establish old target): %v", err)
	}

	crNew := newCR("ns", "checkout", "new-sa")
	crNew.Status.ServiceAccountName = "old-sa"
	got, err := Ensure(context.Background(), c, crNew, "arn:aws:iam::123456789012:role/x")
	if err != nil {
		t.Fatalf("Ensure (rename): %v", err)
	}
	if got != "new-sa" {
		t.Errorf("expected new target %q, got %q", "new-sa", got)
	}

	oldSA := getSA(t, c, "ns", "old-sa")
	if _, ok := oldSA.Annotations[RoleARNAnnotation]; ok {
		t.Error("expected role-arn annotation released from the previous target")
	}
	if oldSA.Annotations["unrelated"] != "keep-me" {
		t.Error("expected unrelated annotation on the previous target to survive cleanup")
	}

	newSA := getSA(t, c, "ns", "new-sa")
	if newSA.Annotations[RoleARNAnnotation] == "" {
		t.Error("expected role-arn annotation set on the new target")
	}
}

// TestCleanup_StripsOwnedFieldOnly follows the same real-prior-Apply setup
// as the rename test above, for the same reason.
func TestCleanup_StripsOwnedFieldOnly(t *testing.T) {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns",
			Name:        "checkout",
			Annotations: map[string]string{"team.example.com/owner": "payments"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(sa).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure (establish claim): %v", err)
	}
	cr.Status.ServiceAccountName = "checkout"

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	got := getSA(t, c, "ns", "checkout")
	if _, ok := got.Annotations[RoleARNAnnotation]; ok {
		t.Error("expected role-arn annotation removed")
	}
	if got.Annotations["team.example.com/owner"] != "payments" {
		t.Error("expected unrelated annotation to survive")
	}
}

func TestCleanup_NoStatusName_IsNoop(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
}

func TestCleanup_ServiceAccountAlreadyGone_IsNoop(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")
	cr.Status.ServiceAccountName = "checkout"

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
}

func TestEnsure_GetErrorOtherThanNotFound_IsReturned(t *testing.T) {
	c := &erroringClient{Client: fake.NewClientBuilder().WithScheme(newScheme(t)).Build()}
	cr := newCR("ns", "checkout", "new-sa")
	cr.Status.ServiceAccountName = "previous"

	_, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x")
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

// TestEnsure_DefaultName_SecondReconcile_KeepsOwnerReference guards an
// invariant server-side apply makes easy to violate by omission: a field
// manager that stops requesting a field in a later apply call has that
// field dropped, since no other manager claims it either. The owner
// reference must therefore be included in every apply call for this
// object, not just the one made when it doesn't exist yet - omitting it
// on a later call strips it, and once stripped, no later Update/Delete
// event on the ServiceAccount can be mapped back to the owning CR again.
func TestEnsure_DefaultName_SecondReconcile_KeepsOwnerReference(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/v1"); err != nil {
		t.Fatalf("Ensure (first): %v", err)
	}
	if owner := metav1.GetControllerOf(getSA(t, c, "ns", "checkout")); owner == nil {
		t.Fatal("expected an owner reference after the first reconcile")
	}

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/v1"); err != nil {
		t.Fatalf("Ensure (second, same CR, already exists): %v", err)
	}
	if owner := metav1.GetControllerOf(getSA(t, c, "ns", "checkout")); owner == nil {
		t.Error("expected the owner reference to survive a second reconcile of the same, already-owned ServiceAccount")
	}
}

// erroringClient forces every Get to fail with a non-NotFound error, to
// exercise the "something genuinely went wrong talking to the API server"
// path (inside release, when a rename requires checking the previous
// target still exists) distinctly from "the object doesn't exist yet".
type erroringClient struct {
	client.Client
}

func (e *erroringClient) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return apierrors.NewInternalError(errBoom{})
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

// Releasing a ServiceAccount this operator created must strip the role
// annotation but keep the owner reference, or garbage collection would never
// remove the object once its CR is deleted.
func TestCleanup_DefaultName_KeepsOwnerReference(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	cr := newCR("ns", "checkout", "")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	cr.Status.ServiceAccountName = "checkout"

	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	got := getSA(t, c, "ns", "checkout")
	if _, ok := got.Annotations[RoleARNAnnotation]; ok {
		t.Error("expected the role-arn annotation removed")
	}
	if owner := metav1.GetControllerOf(got); owner == nil || owner.UID != cr.UID {
		t.Errorf("expected the owner reference to survive the release, got %v", got.OwnerReferences)
	}
}

func TestCleanup_UserProvidedName_NeverGainsAnOwnerReference(t *testing.T) {
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "team-sa"}}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(sa).Build()
	cr := newCR("ns", "checkout", "team-sa")

	if _, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/x"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	cr.Status.ServiceAccountName = "team-sa"
	if err := Cleanup(context.Background(), c, cr); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if got := getSA(t, c, "ns", "team-sa"); len(got.OwnerReferences) != 0 {
		t.Errorf("a ServiceAccount the user created must never be owned, got %v", got.OwnerReferences)
	}
}

func assertReason(t *testing.T, err error, reason string) {
	t.Helper()
	var reconcileErr *cloudctlaws.ReconcileError
	if !errors.As(err, &reconcileErr) || reconcileErr.Reason != reason {
		t.Fatalf("expected reason %s, got %v", reason, err)
	}
}

const platformRole = "arn:aws:iam::123456789012:role/platform-payments"

func saWithRole(name string, annotations map[string]string) *corev1.ServiceAccount {
	all := map[string]string{RoleARNAnnotation: platformRole}
	maps.Copy(all, annotations)
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Annotations: all}}
}

// A CR author who can't edit ServiceAccounts must not be able to swap a
// workload's IAM role through the operator. The role someone else set is
// left alone unless the ServiceAccount itself consents.
func TestEnsure_RefusesToReplaceARoleSetOutsideTheOperator(t *testing.T) {
	for name, tc := range map[string]struct {
		annotations map[string]string
		wantClaimed bool
	}{
		"no consent":             {nil, false},
		"consent for another CR": {map[string]string{AllowClaimAnnotation: "someone-else"}, false},
		"consent for this CR":    {map[string]string{AllowClaimAnnotation: "checkout"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(saWithRole("payments-api", tc.annotations)).Build()
			cr := newCR("ns", "checkout", "payments-api")

			_, err := Ensure(context.Background(), c, cr, "arn:aws:iam::123456789012:role/ns-checkout-role")
			role := getSA(t, c, "ns", "payments-api").Annotations[RoleARNAnnotation]
			if tc.wantClaimed {
				if err != nil || role != "arn:aws:iam::123456789012:role/ns-checkout-role" {
					t.Fatalf("expected the consenting ServiceAccount claimed, got err=%v role=%s", err, role)
				}
				return
			}
			assertReason(t, err, reasons.ServiceAccountNotClaimable)
			if !strings.Contains(err.Error(), AllowClaimAnnotation) {
				t.Errorf("the error should say how to consent: %v", err)
			}
			if role != platformRole {
				t.Errorf("the platform's role was replaced with %s", role)
			}
		})
	}
}

// Checks come before releasing the previous ServiceAccount, or a refused
// retarget would leave the workload with no role at all.
func TestEnsure_ARefusedRetargetKeepsThePreviousServiceAccount(t *testing.T) {
	plain := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "checkout-sa"}}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(plain, saWithRole("payments-api", nil)).Build()
	cr := newCR("ns", "checkout", "checkout-sa")
	const role = "arn:aws:iam::123456789012:role/ns-checkout-role"
	got, err := Ensure(context.Background(), c, cr, role)
	if err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	cr.Status.ServiceAccountName = got

	cr.Spec.ServiceAccountName = "payments-api"
	_, err = Ensure(context.Background(), c, cr, role)
	assertReason(t, err, reasons.ServiceAccountNotClaimable)
	if getSA(t, c, "ns", "checkout-sa").Annotations[RoleARNAnnotation] != role {
		t.Error("the previous ServiceAccount lost its role although the retarget was refused")
	}
}

// --- network identity label ---

func TestEnsureNetworkIdentityLabel_OnlyLabelsServiceAccountsItMay(t *testing.T) {
	cases := map[string]struct {
		objects    []client.Object
		saName     string
		wantReason string
	}{
		"a named ServiceAccount that doesn't exist":    {nil, "payments-api", reasons.ServiceAccountNotFound},
		"one carrying a role set outside the operator": {[]client.Object{saWithRole("payments-api", nil)}, "payments-api", reasons.ServiceAccountNotClaimable},
		"the same, consenting":                         {[]client.Object{saWithRole("payments-api", map[string]string{AllowClaimAnnotation: "checkout"})}, "payments-api", ""},
		"a plain ServiceAccount":                       {[]client.Object{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "app"}}}, "app", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(tc.objects...).Build()
			err := EnsureNetworkIdentityLabel(context.Background(), c, "ns", "checkout", "uid-1", tc.saName)
			if tc.wantReason != "" {
				assertReason(t, err, tc.wantReason)
				return
			}
			if err != nil {
				t.Fatalf("EnsureNetworkIdentityLabel: %v", err)
			}
			if getSA(t, c, "ns", tc.saName).Labels[NetworkIdentityLabelKey] != tc.saName {
				t.Error("expected the label")
			}
		})
	}
}

// The CR's own default ServiceAccount is created when missing, owned by the
// CR so it goes when the CR does.
func TestEnsureNetworkIdentityLabel_CreatesTheCRsOwnServiceAccount(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	if err := EnsureNetworkIdentityLabel(context.Background(), c, "ns", "checkout", "uid-1", "checkout"); err != nil {
		t.Fatalf("EnsureNetworkIdentityLabel: %v", err)
	}
	sa := getSA(t, c, "ns", "checkout")
	if owner := metav1.GetControllerOf(sa); owner == nil || owner.UID != "uid-1" || owner.Kind != "AppDependencies" {
		t.Errorf("expected the CR as controller owner, got %+v", owner)
	}
	if sa.Labels[NetworkIdentityLabelKey] != "checkout" {
		t.Error("expected the label")
	}
}

// Each CR holds the label under its own field manager, so one releasing it
// doesn't take it away from another that still needs it.
func TestReleaseNetworkIdentityLabel_KeepsTheLabelWhileAnotherCRStillUsesIt(t *testing.T) {
	plain := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "app"}}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(plain).Build()
	ctx := context.Background()
	for _, cr := range []string{"one", "two"} {
		if err := EnsureNetworkIdentityLabel(ctx, c, "ns", cr, "uid-"+cr, "app"); err != nil {
			t.Fatalf("EnsureNetworkIdentityLabel(%s): %v", cr, err)
		}
	}

	if err := ReleaseNetworkIdentityLabel(ctx, c, "ns", "one", "uid-one", "app"); err != nil {
		t.Fatalf("release one: %v", err)
	}
	if getSA(t, c, "ns", "app").Labels[NetworkIdentityLabelKey] != "app" {
		t.Fatal("the label went away while CR two still uses it")
	}
	if err := ReleaseNetworkIdentityLabel(ctx, c, "ns", "two", "uid-two", "app"); err != nil {
		t.Fatalf("release two: %v", err)
	}
	if _, still := getSA(t, c, "ns", "app").Labels[NetworkIdentityLabelKey]; still {
		t.Error("expected the label gone once no CR uses it")
	}
}

// plainSA is a ServiceAccount a team created, with no IAM role on it yet.
func plainSA(name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}}
}

// countingApplies returns a fake client that records managedFields, as the
// API server does, and counts the applies it receives.
func countingApplies(t *testing.T, objs ...client.Object) (client.WithWatch, *int) {
	t.Helper()
	applies := 0
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).WithReturnManagedFields().
		WithInterceptorFuncs(interceptor.Funcs{
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				applies++
				return c.Apply(ctx, obj, opts...)
			},
		}).Build()
	return c, &applies
}

func TestEnsure_AppliesOnlyWhenSomethingDiffers(t *testing.T) {
	c, applies := countingApplies(t)
	cr := newCR("ns", "checkout", "")
	ensure := func(roleARN string) {
		t.Helper()
		if _, err := Ensure(context.Background(), c, cr, roleARN); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
	}

	ensure("arn:aws:iam::123456789012:role/a")
	ensure("arn:aws:iam::123456789012:role/a")
	if *applies != 1 {
		t.Fatalf("applies = %d after two passes with nothing changed, want 1", *applies)
	}
	ensure("arn:aws:iam::123456789012:role/b")
	if *applies != 2 {
		t.Fatalf("applies = %d after the role changed, want 2", *applies)
	}

	sa := getSA(t, c, "ns", "checkout")
	delete(sa.Annotations, RoleARNAnnotation)
	if err := c.Update(context.Background(), sa); err != nil {
		t.Fatalf("Update: %v", err)
	}
	ensure("arn:aws:iam::123456789012:role/b")
	if *applies != 3 {
		t.Fatalf("applies = %d after the annotation was removed by hand, want 3", *applies)
	}
	if got := getSA(t, c, "ns", "checkout").Annotations[RoleARNAnnotation]; got != "arn:aws:iam::123456789012:role/b" {
		t.Errorf("role annotation = %q, want it restored", got)
	}

	// Naming the same ServiceAccount explicitly drops the owner reference,
	// which only an apply can do.
	cr.Spec.ServiceAccountName = "checkout"
	ensure("arn:aws:iam::123456789012:role/b")
	if *applies != 4 {
		t.Errorf("applies = %d after the owner reference stopped being wanted, want 4", *applies)
	}
}

func TestEnsureNetworkIdentityLabel_AppliesOnlyWhenTheLabelIsMissing(t *testing.T) {
	c, applies := countingApplies(t, plainSA("app"))
	ensure := func() {
		t.Helper()
		if err := EnsureNetworkIdentityLabel(context.Background(), c, "ns", "checkout", "uid-1", "app"); err != nil {
			t.Fatalf("EnsureNetworkIdentityLabel: %v", err)
		}
	}

	ensure()
	ensure()
	if *applies != 1 {
		t.Fatalf("applies = %d after two passes with nothing changed, want 1", *applies)
	}

	sa := getSA(t, c, "ns", "app")
	delete(sa.Labels, NetworkIdentityLabelKey)
	if err := c.Update(context.Background(), sa); err != nil {
		t.Fatalf("Update: %v", err)
	}
	ensure()
	if *applies != 2 {
		t.Errorf("applies = %d after the label was removed by hand, want 2", *applies)
	}
}
