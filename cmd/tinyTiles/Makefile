SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

GO ?= go
BUILD_DIR ?= dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.1.0-dev)
SQLITE_TAGS := sqliteimport
GOFILES := $(shell find . -name '*.go' -not -path './dist/*')
WASM_DIR := $(BUILD_DIR)/wasm
WASM_EXEC ?= $(shell $(GO) env GOROOT)/lib/wasm/wasm_exec.js
LDFLAGS := -s -w -X main.version=$(VERSION)
BUILD_FLAGS := -trimpath -buildvcs=false
MBTILES ?=
ARTIFACT ?=
REQUESTS ?= 1024
MEMORY ?= 16777216
READERS ?= 8

.DEFAULT_GOAL := help
.PHONY: help fmt fmt-check tidy tidy-check vet test test-short test-race coverage bench bench-fixture build build-reader-cli build-server install build-demo-server build-native-demo reader-no-sqlite-check wasm wasm-demo wasm-gzip wasm-package wasm-check demo-check serve-wasm ci vulncheck clean

help: ## Show all supported development targets.
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "%-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format all Go source files.
	gofmt -w $(GOFILES)

fmt-check: ## Fail if Go formatting would change a source file.
	@if changed="$$(gofmt -l $(GOFILES))"; test -n "$$changed"; then echo "Run make fmt:"; echo "$$changed"; exit 1; fi

tidy: ## Normalize go.mod and go.sum after dependency changes.
	$(GO) mod tidy

tidy-check: ## Fail when go.mod or go.sum would change.
	$(GO) mod tidy -diff

vet: ## Run Go's static analyzer for all native packages.
	$(GO) vet -tags=$(SQLITE_TAGS) ./...

test: ## Run the complete native test suite.
	$(GO) test -tags=$(SQLITE_TAGS) ./...

test-short: ## Run fast native tests only.
	$(GO) test -short -tags=$(SQLITE_TAGS) ./...

test-race: ## Run the complete native test suite with the race detector.
	$(GO) test -race -tags=$(SQLITE_TAGS) ./...

coverage: ## Produce atomic coverage data and print function coverage.
	mkdir -p $(BUILD_DIR)
	$(GO) test -covermode=atomic -coverprofile=$(BUILD_DIR)/coverage.out -tags=$(SQLITE_TAGS) ./...
	$(GO) tool cover -func=$(BUILD_DIR)/coverage.out

bench: ## Run deterministic package benchmarks with allocation statistics.
	$(GO) test -run '^$$' -bench . -benchmem -count=3 -tags=$(SQLITE_TAGS) ./...

bench-fixture: build ## Compare an existing flat or normalized fixture with SQLite.
	@test -n "$(MBTILES)" || { echo "MBTILES=/path/to/source.mbtiles is required"; exit 2; }
	@test -n "$(ARTIFACT)" || { echo "ARTIFACT=/path/to/dataset.ttiles is required"; exit 2; }
	$(BUILD_DIR)/tinytiles benchmark --source "$(MBTILES)" --artifact "$(ARTIFACT)" --requests $(REQUESTS) --readers $(READERS) --max-memory $(MEMORY)

build: ## Build the native tinytiles CLI into dist/.
	mkdir -p $(BUILD_DIR)
	$(GO) build -tags=$(SQLITE_TAGS) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/tinytiles ./cmd/tinytiles

build-reader-cli: ## Build the SQLite-free validate/inspect/tile CLI into dist/.
	mkdir -p $(BUILD_DIR)
	$(GO) build $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/tinytiles-reader ./cmd/tinytiles

install: ## Install the tagged native CLI into GOPATH/bin.
	$(GO) install -tags=$(SQLITE_TAGS) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" ./cmd/tinytiles

build-server: ## Build the SQLite-free standalone HTTP server into dist/.
	mkdir -p $(BUILD_DIR)
	$(GO) build $(BUILD_FLAGS) -o $(BUILD_DIR)/tinytiles-server ./cmd/tinytiles-server

build-demo-server: build-server ## Backwards-compatible alias for build-server.

reader-no-sqlite-check: ## Prove that reader CLI, importable runtime and server have no SQLite dependency.
	$(GO) test ./ ./cmd/tinytiles ./cmd/tinytiles-server ./server
	$(GO) build $(BUILD_FLAGS) ./ ./cmd/tinytiles ./cmd/tinytiles-server ./server
	@if $(GO) list -deps ./ ./cmd/tinytiles ./cmd/tinytiles-server ./server | rg -q '^modernc\.org/sqlite$$'; then echo "reader unexpectedly depends on modernc.org/sqlite"; exit 1; fi

build-native-demo: ## Build the durable native offline-sync client into dist/.
	mkdir -p $(BUILD_DIR)
	$(GO) build $(BUILD_FLAGS) -o $(BUILD_DIR)/tinytiles-native-client ./examples/native-client

wasm: ## Build the browser cache runtime and wasm_exec.js into dist/wasm/.
	mkdir -p $(WASM_DIR)
	GOOS=js GOARCH=wasm $(GO) build $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $(WASM_DIR)/tinytiles.wasm ./cmd/tinytiles-wasm
	cp "$(WASM_EXEC)" $(WASM_DIR)/wasm_exec.js

wasm-demo: wasm ## Copy the no-dependency browser demo next to the WASM bundle.
	cp examples/wasm/index.html $(WASM_DIR)/index.html
	cp examples/wasm/app.js $(WASM_DIR)/app.js

wasm-gzip: wasm ## Produce a reproducible gzip-compressed WASM asset for CDN/proxy delivery.
	gzip -n -9 -c $(WASM_DIR)/tinytiles.wasm > $(WASM_DIR)/tinytiles.wasm.gz

wasm-package: wasm-demo wasm-gzip ## Build the complete static browser bundle, including tinytiles.wasm.gz.

wasm-check: wasm-gzip demo-check ## Compile-check the WebAssembly, compressed asset and static demo targets.
	gzip -t $(WASM_DIR)/tinytiles.wasm.gz

demo-check: ## Syntax-check browser demo assets when Node.js is available.
	@test -s examples/wasm/index.html
	@test -s examples/wasm/app.js
	@if command -v node >/dev/null 2>&1; then node --check examples/wasm/app.js; else echo "node not installed; skipped JavaScript syntax check"; fi

serve-wasm: wasm-demo ## Serve the generated browser demo at http://localhost:8081.
	cd $(WASM_DIR) && python3 -m http.server 8081 --bind 127.0.0.1

ci: fmt-check tidy-check vet test test-race reader-no-sqlite-check wasm-check ## Run the checks intended for CI.

vulncheck: ## Run govulncheck when installed; otherwise show the install command.
	@if ! command -v govulncheck >/dev/null 2>&1; then echo "Install with: go install golang.org/x/vuln/cmd/govulncheck@latest"; exit 0; fi; govulncheck ./...

clean: ## Remove generated local build artifacts.
	@case "$(BUILD_DIR)" in ""|/|.|..|../*|/*) echo "refusing unsafe BUILD_DIR=$(BUILD_DIR)"; exit 2;; esac
	rm -rf -- "$(BUILD_DIR)"
