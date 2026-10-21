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

// Package decisions works out, from the ownership ledger alone, what the
// operator will do with each entry when its CR is deleted, and which entries
// still need each KMS key. The deletion gate for keys and the fields
// published in status both come from here, so they cannot disagree.
package decisions

import (
	"fmt"
	"sort"
	"strings"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
	cloudctlaws "github.com/Ningendo7/cloudctl-operator/internal/aws"
	"github.com/Ningendo7/cloudctl-operator/internal/status"
)

// Ledger entry types this package needs to tell apart.
const (
	TypeKMS = "kms"
	TypeIAM = "iam"
	TypeRDS = "rds"
)

// maxHeldBy is how many holders a key entry lists before saying "and N more".
const maxHeldBy = 20

// forceable are the types whose emptiness check force: true bypasses.
var forceable = map[string]bool{"sqs": true, "sns": true, "s3": true, "dynamodb": true}

// OnDelete says what happens to an entry's resource when its CR is deleted
// or the entry leaves spec.
func OnDelete(e depsv1alpha1.ManagedResource) depsv1alpha1.OnDeleteOutcome {
	switch {
	case e.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete:
		return depsv1alpha1.OnDeleteRetain
	case e.Type == TypeRDS:
		return depsv1alpha1.OnDeleteManual
	case e.Force && forceable[e.Type]:
		return depsv1alpha1.OnDeleteEvenIfNotEmpty
	default:
		return depsv1alpha1.OnDeleteDelete
	}
}

// Holder is a ledger entry that still needs a KMS key.
type Holder struct {
	Type, Name string
	// Retained is true when the holder outlives the CR, so its key must too.
	Retained bool
}

// KeyHolders returns, for each KMS entry by name, the entries that need it.
// The key is found through the KMSKeyARN recorded on the entry. Entries
// recorded before that field existed fall back to the dedicated key derived
// from the entry's own type and name, which covers encryption.enabled.
func KeyHolders(ledger []depsv1alpha1.ManagedResource) map[string][]Holder {
	byARN := map[string]string{}
	for _, e := range ledger {
		if e.Type == TypeKMS {
			byARN[e.ARN] = e.Name
		}
	}
	holders := map[string][]Holder{}
	for _, e := range ledger {
		if e.Type == TypeKMS || e.Type == TypeIAM {
			continue
		}
		holder := Holder{Type: e.Type, Name: e.Name, Retained: e.DeletionPolicy != depsv1alpha1.DeletionPolicyDelete}
		if name, ok := byARN[e.KMSKeyARN]; ok && e.KMSKeyARN != "" {
			holders[name] = append(holders[name], holder)
			continue
		}
		ownerType, resourceName := e.Type, e.Name
		if ownerType == cloudctlaws.ResourceTypeRDSSnapshot {
			ownerType = TypeRDS
		}
		if base, _, derived := strings.Cut(resourceName, "#"); derived {
			resourceName = base
		}
		if dedicated := cloudctlaws.DedicatedKeyLedgerName(ownerType, resourceName); status.HasManagedResource(ledger, TypeKMS, dedicated) {
			holders[dedicated] = append(holders[dedicated], holder)
		}
	}
	return holders
}

// Stamp sets onDelete on every entry and heldBy on every KMS entry, in place.
func Stamp(ledger []depsv1alpha1.ManagedResource) {
	holders := KeyHolders(ledger)
	for i := range ledger {
		e := &ledger[i]
		e.OnDelete = OnDelete(*e)
		e.HeldBy = nil
		if e.Type == TypeKMS {
			e.HeldBy = heldBy(holders[e.Name])
		}
	}
}

func heldBy(holders []Holder) []string {
	if len(holders) == 0 {
		return nil
	}
	names := make([]string, 0, len(holders))
	for _, h := range holders {
		names = append(names, h.Type+"/"+h.Name)
	}
	sort.Strings(names)
	if len(names) > maxHeldBy {
		rest := len(names) - maxHeldBy
		names = append(names[:maxHeldBy], fmt.Sprintf("and %d more", rest))
	}
	return names
}
