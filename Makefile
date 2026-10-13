.PHONY: build test test-race lint integration run setup k8s-cluster charts-lint charts-template

PROFILE ?= profiles/openclaw-tb2-fix-git-deepseek.json

export GOCACHE ?= $(CURDIR)/.cache/go-build
export GOMODCACHE ?= $(CURDIR)/.cache/go-mod

build:
	mkdir -p bin
	go build -o bin/aries ./cmd/aries
	CGO_ENABLED=0 go build -o bin/aries-ssh-client ./cmd/aries-ssh-client
	CGO_ENABLED=0 go build -o bin/aries-bridge ./cmd/aries-bridge

test:
	go test -v ./...

test-race:
	go test -race ./...

lint:
	test -z "$$(gofmt -l $$(find cmd internal pkg scripts -name '*.go' -type f))"
	go vet ./...

integration: bridge-image
	ARIES_SSH_CLIENT=$(CURDIR)/bin/aries-ssh-client go test -p=1 -count=1 -tags=integration ./...

run: build
	./bin/aries $(PROFILE)

# Optional prewarm: validate the profile and prepare benchmark/image inputs
# without contacting or starting the configured model service.
setup: build
	./bin/aries setup $(PROFILE)

# Compiler and plugins are pinned; generated bindings are checked into the tree.
PROTOC_VERSION := 29.3
PROTOC := $(CURDIR)/.cache/protobuf/bin/protoc
PROTOC_GEN_GO_VERSION := v1.36.6
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
.PHONY: proto-tools proto
proto-tools:
	mkdir -p .cache/protobuf
	curl -fL -o .cache/protobuf/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip
	echo '3e866620c5be27664f3d2fa2d656b5f3e09b5152b42f1bedbf427b333e90021a  .cache/protobuf/protoc.zip' | sha256sum -c -
	unzip -qo .cache/protobuf/protoc.zip -d .cache/protobuf
	GOBIN=$(CURDIR)/.cache/protobuf/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(CURDIR)/.cache/protobuf/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

proto:
	test "$$($(CURDIR)/.cache/protobuf/bin/protoc-gen-go --version)" = 'protoc-gen-go $(PROTOC_GEN_GO_VERSION)'
	test "$$($(CURDIR)/.cache/protobuf/bin/protoc-gen-go-grpc --version)" = 'protoc-gen-go-grpc $(patsubst v%,%,$(PROTOC_GEN_GO_GRPC_VERSION))'
	test "$$($(PROTOC) --version)" = 'libprotoc $(PROTOC_VERSION)'
	PATH=$(CURDIR)/.cache/protobuf/bin:$$PATH $(PROTOC) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/bridge/control/v1/control.proto

.PHONY: bridge-image
bridge-image: build
	go run ./scripts/build-bridge-image

# Build a cluster with Invitro's setup and apply ARIES to it. NODES lists SSH
# targets: the master, the runner (Invitro's loader node), then the workers.
#   make k8s-cluster NODES="user@master user@runner user@worker1"
k8s-cluster:
	./k8s/create-cluster.sh $(K8S_FLAGS) $(NODES)

# Render the chart without a cluster. Catches template errors and, with
# --dry-run, schema problems that only show up once values are merged.
# HELM is the helm binary to use; override it if yours is not on PATH.
HELM ?= helm
charts-lint:
	$(HELM) lint ./k8s/aries

charts-template:
	$(HELM) template aries ./k8s/aries -n aries \
	  -f ./k8s/aries/values-local.yaml >/dev/null
	$(HELM) template aries ./k8s/aries -n aries \
	  -f ./k8s/aries/values-cluster.yaml --set registry.dockerconfigjson='{}' >/dev/null
	@echo "charts render"
