SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: help build test clean install lint fmt fmt-check vet run-repl run-server run-demo
.PHONY: build-all build-repl build-server build-demo build-cli build-debug build-catalog
.PHONY: build-wasm-browser build-wasm-node build-studio build-tinysqlpage build-migrate
.PHONY: build-query-files build-query-files-wasm run-query-files-demo
.PHONY: test-all test-unit test-integration coverage build-check verify verify-ci
.PHONY: test-query-files test-query-files-wasm
.PHONY: run-wasm-browser run-wasm-node-demo deps update-deps tidy bench script-lint docker-build info
.DEFAULT_GOAL := help

# Variables
GO := go
GOFLAGS := 
BINARY_DIR := bin
CMD_DIR := cmd
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"
GO_BUILD_FLAGS := $(GOFLAGS) $(LDFLAGS)
GO_TEST_FLAGS ?= -v
COVERPROFILE ?= coverage.out
WASM_BROWSER_SCRIPT := ./$(CMD_DIR)/wasm_browser/build.sh
WASM_NODE_SCRIPT := ./$(CMD_DIR)/wasm_node/build.sh
QUERY_FILES_DIR := ./$(CMD_DIR)/query_files
QUERY_FILES_WASM_DIR := ./$(CMD_DIR)/query_files_wasm
QUERY_FILES_WASM_SCRIPT := $(QUERY_FILES_WASM_DIR)/build.sh

# Color output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

## help: Show this help message
help:
	@echo 'Usage:'
	@echo '  $(YELLOW)make$(NC) $(GREEN)<target>$(NC)'
	@echo ''
	@echo 'Targets:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## build: Build the main tinySQL CLI
build: build-cli

## build-all: Build all binaries
build-all: build-cli build-repl build-server build-demo build-debug build-catalog build-studio build-tinysqlpage build-migrate build-query-files
	@echo "$(GREEN)✓ All binaries built successfully$(NC)"

## build-cli: Build tinySQL CLI
build-cli:
	@echo "$(GREEN)Building tinySQL CLI...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/tinysql ./$(CMD_DIR)/tinysql

## build-repl: Build interactive REPL
build-repl:
	@echo "$(GREEN)Building REPL...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/repl ./$(CMD_DIR)/repl

## build-server: Build SQL server
build-server:
	@echo "$(GREEN)Building server...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/server ./$(CMD_DIR)/server

## build-demo: Build demo application
build-demo:
	@echo "$(GREEN)Building demo...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/demo ./$(CMD_DIR)/demo

## build-debug: Build debug tool
build-debug:
	@echo "$(GREEN)Building debug tool...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/debug ./$(CMD_DIR)/debug

## build-catalog: Build catalog demo
build-catalog:
	@echo "$(GREEN)Building catalog demo...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/catalog-demo ./$(CMD_DIR)/catalog_demo

## build-studio: Build tinySQL Studio (Wails)
build-studio:
	@echo "$(GREEN)Building tinySQL Studio...$(NC)"
	@mkdir -p $(BINARY_DIR)
	cd $(CMD_DIR)/studio && $(GO) build $(GO_BUILD_FLAGS) -o ../../$(BINARY_DIR)/studio .

## build-tinysqlpage: Build tinySQLPage
build-tinysqlpage:
	@echo "$(GREEN)Building tinySQLPage...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -o $(BINARY_DIR)/tinysqlpage ./$(CMD_DIR)/tinysqlpage

## build-migrate: Build data migration tool
build-migrate:
	@echo "$(GREEN)Building data migration tool...$(NC)"
	@mkdir -p $(BINARY_DIR)
	cd $(CMD_DIR)/migrate && $(GO) build $(GOFLAGS) -o ../../$(BINARY_DIR)/migrate .

## build-wasm-browser: Build WASM for browser
build-wasm-browser:
	@echo "$(GREEN)Building WASM for browser...$(NC)"
	@$(WASM_BROWSER_SCRIPT) --build-only

## build-wasm-node: Build WASM for Node.js
build-wasm-node:
	@echo "$(GREEN)Building WASM for Node.js...$(NC)"
	@$(WASM_NODE_SCRIPT) --build-only

## build-query-files: Build standalone query_files CLI
build-query-files:
	@echo "$(GREEN)Building query_files CLI...$(NC)"
	@mkdir -p $(BINARY_DIR)
	cd $(QUERY_FILES_DIR) && $(GO) build -trimpath -o ../../$(BINARY_DIR)/query_files .

## build-query-files-wasm: Build query_files_wasm artifacts
build-query-files-wasm:
	@echo "$(GREEN)Building query_files_wasm...$(NC)"
	@$(QUERY_FILES_WASM_SCRIPT) --build-only

## run-wasm-browser: Build and serve browser WASM app on localhost
run-wasm-browser:
	@$(WASM_BROWSER_SCRIPT) --serve

## run-wasm-node-demo: Build and execute Node WASM demo query
run-wasm-node-demo:
	@$(WASM_NODE_SCRIPT) --run

## run-query-files-demo: Run query_files demo script
run-query-files-demo:
	@$(QUERY_FILES_DIR)/demo.sh

## install: Install tinySQL CLI to $GOPATH/bin
install:
	@echo "$(GREEN)Installing tinySQL...$(NC)"
	$(GO) install $(GO_BUILD_FLAGS) ./$(CMD_DIR)/tinysql

## test: Run all tests
test: test-all

## test-all: Run all tests with coverage
test-all:
	@echo "$(GREEN)Running all tests...$(NC)"
	$(GO) test $(GO_TEST_FLAGS) -race -coverprofile=$(COVERPROFILE) ./...
	$(MAKE) test-query-files
	$(MAKE) test-query-files-wasm

## test-unit: Run unit tests only
test-unit:
	@echo "$(GREEN)Running unit tests...$(NC)"
	$(GO) test $(GO_TEST_FLAGS) -short ./...

## test-integration: Run integration tests
test-integration:
	@echo "$(GREEN)Running integration tests...$(NC)"
	$(GO) test $(GO_TEST_FLAGS) -run Integration ./...

## test-query-files: Run tests for cmd/query_files module
test-query-files:
	@echo "$(GREEN)Running query_files tests...$(NC)"
	cd $(QUERY_FILES_DIR) && $(GO) test $(GO_TEST_FLAGS) ./...

## test-query-files-wasm: Run tests for cmd/query_files_wasm module
test-query-files-wasm:
	@echo "$(GREEN)Running query_files_wasm tests...$(NC)"
	cd $(QUERY_FILES_WASM_DIR) && $(GO) test $(GO_TEST_FLAGS) ./...

## coverage: Generate and view test coverage report
coverage: test-all
	@echo "$(GREEN)Generating coverage report...$(NC)"
	$(GO) tool cover -html=$(COVERPROFILE)

## bench: Run benchmarks
bench:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	$(GO) test -bench=. -benchmem ./...

## lint: Run linter (golangci-lint)
lint:
	@echo "$(GREEN)Running linter...$(NC)"
	@command -v golangci-lint >/dev/null || (echo "$(RED)golangci-lint not found. Install: https://golangci-lint.run/usage/install/$(NC)" && exit 1)
	golangci-lint run ./...

## script-lint: Run shellcheck on repository shell scripts (if available)
script-lint:
	@echo "$(GREEN)Linting shell scripts...$(NC)"
	@if ! command -v shellcheck >/dev/null; then \
		echo "$(YELLOW)shellcheck not installed, skipping$(NC)"; \
	else \
		shellcheck demo_formats.sh cmd/query_files/demo.sh cmd/query_files_wasm/build.sh cmd/wasm_node/build.sh cmd/wasm_browser/build.sh; \
	fi

## fmt: Format all Go files
fmt:
	@echo "$(GREEN)Formatting code...$(NC)"
	@files="$$(git ls-files '*.go')"; \
	if [ -z "$$files" ]; then \
		echo "$(YELLOW)No Go files found$(NC)"; \
		exit 0; \
	fi; \
	gofmt -w $$files; \
	echo "$(GREEN)✓ Go files formatted$(NC)"

## fmt-check: Verify gofmt formatting without modifying files
fmt-check:
	@echo "$(GREEN)Checking Go formatting...$(NC)"
	@files="$$(git ls-files '*.go')"; \
	if [ -z "$$files" ]; then \
		echo "$(YELLOW)No Go files found$(NC)"; \
		exit 0; \
	fi; \
	unformatted="$$(gofmt -l $$files)"; \
	if [ -n "$$unformatted" ]; then \
		echo "$(RED)Unformatted Go files:$(NC)"; \
		echo "$$unformatted"; \
		exit 1; \
	fi; \
	echo "$(GREEN)✓ Go formatting is clean$(NC)"

## vet: Run go vet
vet:
	@echo "$(GREEN)Running go vet...$(NC)"
	$(GO) vet ./...

## build-check: Ensure all Go packages compile
build-check:
	@echo "$(GREEN)Building all Go packages...$(NC)"
	$(GO) build $(GOFLAGS) ./...
	$(MAKE) build-query-files
	$(MAKE) build-query-files-wasm

## tidy: Tidy dependencies
tidy:
	@echo "$(GREEN)Tidying dependencies...$(NC)"
	$(GO) mod tidy

## verify: Run fmt, vet, lint and test
verify: fmt vet lint test
	@echo "$(GREEN)✓ All verifications passed$(NC)"

## verify-ci: CI-safe verification (non-mutating)
verify-ci: fmt-check vet build-check test-all
	@echo "$(GREEN)✓ CI verification passed$(NC)"

## clean: Remove build artifacts
clean:
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	rm -rf $(BINARY_DIR)
	rm -f $(COVERPROFILE)
	rm -f cmd/wasm_browser/web/tinySQL.wasm cmd/wasm_browser/web/wasm_exec.js
	rm -f cmd/wasm_node/tinySQL.wasm cmd/wasm_node/wasm_exec.js
	rm -f cmd/query_files_wasm/query_files.wasm cmd/query_files_wasm/query_files.wasm.gz cmd/query_files_wasm/wasm_exec.js
	find . -name "*.db.wal" -type f -delete
	find . -name "*.dat.wal" -type f -delete
	@echo "$(GREEN)✓ Clean complete$(NC)"

## run-repl: Run the REPL
run-repl: build-repl
	@echo "$(GREEN)Starting REPL...$(NC)"
	./$(BINARY_DIR)/repl

## run-server: Run the server
run-server: build-server
	@echo "$(GREEN)Starting server...$(NC)"
	./$(BINARY_DIR)/server

## run-demo: Run the demo
run-demo: build-demo
	@echo "$(GREEN)Starting demo...$(NC)"
	./$(BINARY_DIR)/demo

## deps: Download dependencies
deps:
	@echo "$(GREEN)Downloading dependencies...$(NC)"
	$(GO) mod download

## update-deps: Update dependencies
update-deps:
	@echo "$(GREEN)Updating dependencies...$(NC)"
	$(GO) get -u ./...
	$(GO) mod tidy

## docker-build: Build Docker image
docker-build:
	@echo "$(GREEN)Building Docker image...$(NC)"
	docker build -t tinysql:$(VERSION) -t tinysql:latest .

## info: Show build information
info:
	@echo "$(YELLOW)Build Information:$(NC)"
	@echo "  Version: $(VERSION)"
	@echo "  Go version: $(shell $(GO) version)"
	@echo "  Binary directory: $(BINARY_DIR)"
	@echo "  Command directory: $(CMD_DIR)"
