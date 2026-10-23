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

// Command kubectl-appdeps is the kubectl plugin for AppDependencies. Installed
// as appdeps it runs standalone.
package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/Ningendo7/cloudctl-operator/internal/cli/cmd"
	"github.com/Ningendo7/cloudctl-operator/internal/cli/kube"
)

func main() {
	streams := cmd.Streams{In: os.Stdin, Out: os.Stdout, Err: os.Stderr}
	os.Exit(cmd.Execute(os.Args[1:], filepath.Base(os.Args[0]), streams, kube.New, time.Now))
}
