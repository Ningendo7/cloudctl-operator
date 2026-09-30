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

package status

import (
	"context"

	depsv1alpha1 "github.com/Ningendo7/cloudctl-operator/api/v1alpha1"
)

// Checkpoint persists the ledger immediately, independent of the whole
// reconcile pass finishing. A resource package calls it between the two AWS
// calls of a multi-step create — KMS's CreateKey then CreateAlias, S3's
// CreateBucket then PutBucketTagging — so that a crash in that window still
// leaves a record on the next reconcile, rather than none at all. A nil
// Checkpoint is valid and means "don't persist early," matching every
// caller's behavior before this existed.
type Checkpoint func(ctx context.Context, ledger []depsv1alpha1.ManagedResource) error
