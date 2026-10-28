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
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// FreshConnections returns an HTTP client that opens a new connection for every
// request. S3 and DynamoDB answer a request on an open connection from the
// policy that connection last saw, which stayed out of date for minutes after a
// policy was deleted (S3) or added (DynamoDB). A test that asks whether a policy
// is in force has to use new connections, or it measures the connection.
func FreshConnections() aws.HTTPClient {
	return awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) { tr.DisableKeepAlives = true })
}
