# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

.PHONY: test-race
test-race: manifests generate fmt vet setup-envtest ## Run tests with the race detector - CI only, forces a from-scratch instrumented rebuild of the whole dependency tree (heavy; don't run this locally on a resource-constrained machine).
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test -race $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= cloudctl-operator-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

# Integration tests run the real AWS SDK against a LocalStack container
# instead of each resource package's own hand-written fakes - see
# docs/testing.md for why this tier exists and what it does and doesn't
# cover. Gated behind the "integration" build tag so a bare `go test ./...`
# (and `make test` above) never touches Docker or a network dependency;
# add new integration-tested packages to INTEGRATION_TEST_PACKAGES as they
# gain _integration_test.go files.
LOCALSTACK_CONTAINER ?= cloudctl-operator-test-localstack
LOCALSTACK_PORT ?= 4566
# Pinned by digest (not just the version tag) so a re-push to that tag
# upstream can't silently change what CI and local integration runs
# actually test against; bump both the tag and digest together on a
# deliberate upgrade.
LOCALSTACK_IMAGE ?= localstack/localstack:4.9.0@sha256:e74aa0e3dad049db6a6a86dd0d4187e054a63ec4e4273d91959b2c41093bf566
INTEGRATION_TEST_PACKAGES ?= ./internal/resources/sqs/... ./internal/resources/s3/... ./internal/resources/sns/... ./internal/resources/dynamodb/... ./internal/resources/iam/... ./internal/resources/alarms/...

.PHONY: setup-test-integration
setup-test-integration: ## Start a LocalStack container for integration tests if one isn't already running
	@command -v $(CONTAINER_TOOL) >/dev/null 2>&1 || { \
		echo "$(CONTAINER_TOOL) is not installed. Please install it manually."; \
		exit 1; \
	}
	@if [ -z "$$($(CONTAINER_TOOL) ps -q -f name=^$(LOCALSTACK_CONTAINER)$$)" ]; then \
		echo "Starting LocalStack container '$(LOCALSTACK_CONTAINER)'..."; \
		$(CONTAINER_TOOL) run -d --name $(LOCALSTACK_CONTAINER) -p $(LOCALSTACK_PORT):4566 -e SERVICES=sqs,s3,sns,dynamodb,iam,sts,cloudwatch $(LOCALSTACK_IMAGE); \
		echo "Waiting for LocalStack to report SQS/S3/SNS/DynamoDB/IAM/CloudWatch ready..."; \
		timeout 60 bash -c 'until curl -sf http://localhost:$(LOCALSTACK_PORT)/_localstack/health 2>/dev/null | grep -q "\"sqs\""; do sleep 2; done'; \
	else \
		echo "LocalStack container '$(LOCALSTACK_CONTAINER)' already running. Skipping."; \
	fi

.PHONY: test-integration
test-integration: setup-test-integration ## Run integration tests against a local LocalStack container
	go test -tags=integration $(INTEGRATION_TEST_PACKAGES) -v
	$(MAKE) cleanup-test-integration

.PHONY: cleanup-test-integration
cleanup-test-integration: ## Tear down the LocalStack container used for integration tests
	@$(CONTAINER_TOOL) rm -f $(LOCALSTACK_CONTAINER) >/dev/null 2>&1 || true

# Live tests run the real AWS SDK against a real AWS account instead of
# LocalStack - see docs/testing.md for why this tier exists on top of the
# integration one above. Gated behind the "live" build tag; never runs in
# CI, costs real (small) money per run, and needs real credentials to
# resolve via the standard AWS credential chain or every test just skips.
LIVE_TEST_PACKAGES ?= ./internal/resources/sqs/... ./internal/resources/sns/... ./internal/resources/s3/... ./internal/resources/dynamodb/... ./internal/resources/iam/... ./internal/resources/alarms/... ./internal/resources/kms/...

.PHONY: test-live
test-live: ## Run live tests against your own real AWS account (costs money, never run in CI)
	go test -tags=live $(LIVE_TEST_PACKAGES) -v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name cloudctl-operator-builder
	$(CONTAINER_TOOL) buildx use cloudctl-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm cloudctl-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef

##@ Helm Deployment

## Helm binary to use for deploying the chart
HELM ?= helm
## Namespace to deploy the Helm release
HELM_NAMESPACE ?= cloudctl-operator-system
## Name of the Helm release
HELM_RELEASE ?= cloudctl-operator
## Path to the Helm chart directory
HELM_CHART_DIR ?= charts/chart
## Additional arguments to pass to helm commands
HELM_EXTRA_ARGS ?=

.PHONY: helm-generate
helm-generate: manifests generate ## Regenerate the Helm chart in charts/ from the current config/. Not run automatically by manifests/generate - may clobber hand-tuned chart customizations, so it's a deliberate, separate step.
	kubebuilder edit --plugins=helm/v2-alpha --output-dir=charts --force
	@# Two known side effects of the command above, neither of which is a
	@# real change worth tracking: it unconditionally rewrites
	@# config/manager/kustomization.yaml with a static (and redundant -
	@# make deploy/build-installer already set the real image at build
	@# time) image pin, and it unconditionally recreates
	@# .github/workflows/test-chart.yml with a stale ./dist/chart path
	@# that --output-dir doesn't actually propagate to. See
	@# .github/workflows/helm-chart-test.yml, our own maintained
	@# equivalent with the path fixed, which this command doesn't touch.
	git checkout -- config/manager/kustomization.yaml
	rm -f .github/workflows/test-chart.yml
	@# This command also overwrites hand-tuned values in charts/chart/values.yaml
	@# wholesale (kubebuilder's own documented behavior, not a bug). Known,
	@# intentional corrections reapplied here rather than left as a "remember
	@# to redo this by hand" step - Manifests Drift CI regenerates fresh and
	@# diffs against what's committed, so anything only fixed by hand would
	@# fail that check on every single run, forever, with no way to pass it.
	@# manager.replicas: kubebuilder's own generic default is 1; this
	@# operator's real one (config/manager/manager.yaml) is 2, for the HA
	@# leader-election setup it actually ships with.
	sed -i 's/^  replicas: 1$$/  replicas: 2/' charts/chart/values.yaml
	@# manager.extraEnv: config/manager/manager.yaml (the kustomize source
	@# this command translates) has no env: field for it to carry over, and
	@# the plugin has no generic "extra env vars" concept of its own, so
	@# this chart-only addition is always wiped on regeneration and has to
	@# be reinserted here, anchored on livenessProbe: - the first stable,
	@# always-present line right after where a container's env belongs.
	sed -i '/^        livenessProbe:$$/i\
        {{- if .Values.manager.extraEnv }}\
        env:\
          {{- toYaml .Values.manager.extraEnv | nindent 10 }}\
        {{- end }}' charts/chart/templates/manager/manager.yaml
	@# manager.image.repository: the plugin's own default ("controller")
	@# can never resolve to a real, pullable image - point it at the real
	@# published one by default instead, so `helm install` with no image
	@# override at all still works.
	sed -i 's|^    repository: controller$$|    repository: ghcr.io/ningendo7/cloudctl-operator|' charts/chart/values.yaml
	@# ...and its adjacent comment, which the plugin regenerates to the
	@# generic "defaults to Chart.appVersion" wording that no longer
	@# matches the tag-default fix just below.
	sed -i 's@## Image tag (defaults to Chart.appVersion if not set)@## Image tag (defaults to v<Chart.appVersion> if not set, matching release.yml)@' charts/chart/values.yaml
	@# manager.image.tag's default: release.yml always publishes the image
	@# tagged with a leading "v" (from the git tag), but Chart.AppVersion
	@# itself never has one - the plugin's own default tag fallback
	@# (bare .Chart.AppVersion) would resolve to a tag that was never
	@# actually pushed. Prepending "v" here keeps the default correct for
	@# every release without needing a tag override either.
	sed -i 's@{{ .Values.manager.image.tag | default .Chart.AppVersion }}@{{ .Values.manager.image.tag | default (printf "v%s" .Chart.AppVersion) }}@' charts/chart/templates/manager/manager.yaml

.PHONY: install-helm
install-helm: ## Install the latest version of Helm.
	@command -v $(HELM) >/dev/null 2>&1 || { \
		echo "Installing Helm..." && \
		curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4 | bash; \
	}

.PHONY: helm-deploy
helm-deploy: install-helm ## Deploy manager to the K8s cluster via Helm. Specify an image with IMG.
	$(HELM) upgrade --install $(HELM_RELEASE) $(HELM_CHART_DIR) \
		--namespace $(HELM_NAMESPACE) \
		--create-namespace \
		--set manager.image.repository=$${IMG%:*} \
		--set manager.image.tag=$${IMG##*:} \
		--wait \
		--timeout 5m \
		$(HELM_EXTRA_ARGS)

.PHONY: helm-uninstall
helm-uninstall: ## Uninstall the Helm release from the K8s cluster.
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-status
helm-status: ## Show Helm release status.
	$(HELM) status $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-history
helm-history: ## Show Helm release history.
	$(HELM) history $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-rollback
helm-rollback: ## Rollback to previous Helm release.
	$(HELM) rollback $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)
