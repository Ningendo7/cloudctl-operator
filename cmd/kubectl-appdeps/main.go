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
