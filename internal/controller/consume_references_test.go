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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	"github.com/Ningendo7/cloudctl-operator/internal/reasons"
)

var checkoutGrant = []depsv1alpha1.SharedWithEntry{{Namespace: "shop", Name: "checkout"}}

// platformProducer declares one resource of each kind a consumer can
// reference, each granted to shop/checkout unless granted is false.
func platformProducer(granted bool) *depsv1alpha1.AppDependencies {
	var grant []depsv1alpha1.SharedWithEntry
	if granted {
		grant = checkoutGrant
	}
	return &depsv1alpha1.AppDependencies{
		ObjectMeta: metav1.ObjectMeta{Namespace: "data", Name: "platform"},
		Spec: depsv1alpha1.AppDependenciesSpec{
			SQS: &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "orders", SharedWith: grant}}},
			SNS: &depsv1alpha1.SNSSpec{Resources: []depsv1alpha1.SNSTopicSpec{{Name: "events", SharedWith: grant}}},
			RDS: &depsv1alpha1.RDSSpec{Resources: []depsv1alpha1.RDSInstanceSpec{{Name: "orders-db", SharedWith: grant}}},
		},
	}
}

func platformRef(resourceName string) depsv1alpha1.ConsumeRef {
	return depsv1alpha1.ConsumeRef{Namespace: "data", Name: "platform", ResourceName: resourceName}
}

// checkoutConsumer references the platform's resources through the given
// spec field.
func checkoutConsumer(field string, ref depsv1alpha1.ConsumeRef) *depsv1alpha1.AppDependencies {
	cr := &depsv1alpha1.AppDependencies{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "checkout", Generation: 1}}
	switch field {
	case "rds.consumes":
		cr.Spec.RDS = &depsv1alpha1.RDSSpec{Consumes: []depsv1alpha1.ConsumeRef{ref}}
	case "subscribesTo":
		cr.Spec.SQS = &depsv1alpha1.SQSSpec{Resources: []depsv1alpha1.SQSQueueSpec{{Name: "inbox", SubscribesTo: []depsv1alpha1.ConsumeRef{ref}}}}
	case "alarms.snsTopicRef":
		cr.Spec.Alarms = &depsv1alpha1.AlarmsSpec{Enabled: true, SnsTopicRef: &ref}
	default:
		cr.Spec.SQS = &depsv1alpha1.SQSSpec{Consumes: []depsv1alpha1.ConsumeRef{ref}}
	}
	return cr
}

func TestCheckConsumeReferences_EveryKindOfReferenceIsChecked(t *testing.T) {
	type refCase struct{ field, resourceName, typo string }
	fields := []refCase{
		{"sqs.consumes", "orders", "ordres"},
		{"rds.consumes", "orders-db", "orders-bd"},
		{"subscribesTo", "events", "evnts"},
		{"alarms.snsTopicRef", "events", "evnts"},
	}
	for _, f := range fields {
		missing := depsv1alpha1.ConsumeRef{Namespace: "data", Name: "plattform", ResourceName: f.resourceName}
		for name, tc := range map[string]struct {
			producer   *depsv1alpha1.AppDependencies
			ref        depsv1alpha1.ConsumeRef
			wantStatus metav1.ConditionStatus
			wantReason string
		}{
			"resolves":                {platformProducer(true), platformRef(f.resourceName), metav1.ConditionTrue, reasons.AllProducersExist},
			"producer CR is misnamed": {platformProducer(true), missing, metav1.ConditionFalse, reasons.ProducerNotFoundYet},
			"resourceName is a typo":  {platformProducer(true), platformRef(f.typo), metav1.ConditionFalse, reasons.ConsumedResourceNotDeclared},
			"owner never granted it":  {platformProducer(false), platformRef(f.resourceName), metav1.ConditionFalse, reasons.ConsumeNotAuthorized},
		} {
			t.Run(f.field+"/"+name, func(t *testing.T) {
				c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).WithObjects(tc.producer).Build()
				consumer := checkoutConsumer(f.field, tc.ref)

				checkConsumeReferences(context.Background(), c, consumer)

				cond := apimeta.FindStatusCondition(consumer.Status.Conditions, conditionTypeConsumeReferences)
				if cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
					t.Fatalf("ConsumeReferencesValid = %+v, want %s with reason %s", cond, tc.wantStatus, tc.wantReason)
				}
				if tc.wantStatus == metav1.ConditionFalse && !strings.Contains(cond.Message, f.field) {
					t.Errorf("the message should say which field is wrong (%s), got %q", f.field, cond.Message)
				}
			})
		}
	}
}

func TestCheckConsumeReferences_AFailedReadIsUnknownNotResolved(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).WithObjects(platformProducer(true)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("etcd request timed out")
			},
		}).Build()
	consumer := checkoutConsumer("sqs.consumes", platformRef("orders"))

	checkConsumeReferences(context.Background(), c, consumer)

	cond := apimeta.FindStatusCondition(consumer.Status.Conditions, conditionTypeConsumeReferences)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != reasons.ReferencesUnknown {
		t.Fatalf("ConsumeReferencesValid = %+v, want Unknown with reason %s", cond, reasons.ReferencesUnknown)
	}
	if !strings.Contains(cond.Message, "timed out") {
		t.Errorf("the message should carry the read error, got %q", cond.Message)
	}
}

func TestCheckSharedWithReferences_AFailedReadIsUnknownNotValid(t *testing.T) {
	producer := platformProducer(true)
	producer.Generation = 1
	c := fake.NewClientBuilder().WithScheme(newSharedWithScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("etcd request timed out")
			},
		}).Build()

	checkSharedWithReferences(context.Background(), c, producer)

	cond := apimeta.FindStatusCondition(producer.Status.Conditions, conditionTypeSharedWithReferences)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != reasons.ReferencesUnknown {
		t.Fatalf("SharedWithReferencesValid = %+v, want Unknown with reason %s", cond, reasons.ReferencesUnknown)
	}
}
