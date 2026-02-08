.PHONY: help build test clean install lint fmt vet run-repl run-server run-demo
.PHONY: build-all build-repl build-server build-demo build-cli build-debug build-catalog
.PHONY: build-wasm-browser build-wasm-node build-studio build-tinysqlpage
.PHONY: test-all test-unit test-integration coverage
.DEFAULT_GOAL := help

# Variables
GO := go
GOFLAGS := 
BINARY_DIR := bin
CMD_DIR := cmd
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"

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
build-all: build-cli build-repl build-server build-demo build-debug build-catalog build-studio build-tinysqlpage
	@echo "$(GREEN)✓ All binaries built successfully$(NC)"

## build-cli: Build tinySQL CLI
build-cli:
	@echo "$(GREEN)Building tinySQL CLI...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/tinysql $(CMD_DIR)/tinysql/main.go

## build-repl: Build interactive REPL
build-repl:
	@echo "$(GREEN)Building REPL...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/repl $(CMD_DIR)/repl/main.go

## build-server: Build SQL server
build-server:
	@echo "$(GREEN)Building server...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/server $(CMD_DIR)/server/main.go

## build-demo: Build demo application
build-demo:
	@echo "$(GREEN)Building demo...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/demo $(CMD_DIR)/demo/main.go

## build-debug: Build debug tool
build-debug:
	@echo "$(GREEN)Building debug tool...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/debug $(CMD_DIR)/debug/main.go

## build-catalog: Build catalog demo
build-catalog:
	@echo "$(GREEN)Building catalog demo...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/catalog-demo $(CMD_DIR)/catalog_demo/main.go

## build-studio: Build tinySQL Studio (Wails)
build-studio:
	@echo "$(GREEN)Building tinySQL Studio...$(NC)"
	@mkdir -p $(BINARY_DIR)
	cd $(CMD_DIR)/studio && $(GO) build $(GOFLAGS) $(LDFLAGS) -o ../../$(BINARY_DIR)/studio .

## build-tinysqlpage: Build tinySQLPage
build-tinysqlpage:
	@echo "$(GREEN)Building tinySQLPage...$(NC)"
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/tinysqlpage $(CMD_DIR)/tinysqlpage/main.go

## build-wasm-browser: Build WASM for browser
build-wasm-browser:
	@echo "$(GREEN)Building WASM for browser...$(NC)"
	@cd $(CMD_DIR)/wasm_browser && ./build.sh

## build-wasm-node: Build WASM for Node.js
build-wasm-node:
	@echo "$(GREEN)Building WASM for Node.js...$(NC)"
	@cd $(CMD_DIR)/wasm_node && ./build.sh

## install: Install tinySQL CLI to $GOPATH/bin
install:
	@echo "$(GREEN)Installing tinySQL...$(NC)"
	$(GO) install $(GOFLAGS) $(LDFLAGS) $(CMD_DIR)/tinysql/main.go

## test: Run all tests
test: test-all

## test-all: Run all tests with coverage
test-all:
	@echo "$(GREEN)Running all tests...$(NC)"
	$(GO) test -v -race -coverprofile=coverage.out ./...

## test-unit: Run unit tests only
test-unit:
	@echo "$(GREEN)Running unit tests...$(NC)"
	$(GO) test -v -short ./...

## test-integration: Run integration tests
test-integration:
	@echo "$(GREEN)Running integration tests...$(NC)"
	$(GO) test -v -run Integration ./...

## coverage: Generate and view test coverage report
coverage: test-all
	@echo "$(GREEN)Generating coverage report...$(NC)"
	$(GO) tool cover -html=coverage.out

## bench: Run benchmarks
bench:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	$(GO) test -bench=. -benchmem ./...

## lint: Run linter (golangci-lint)
lint:
	@echo "$(GREEN)Running linter...$(NC)"
	@which golangci-lint > /dev/null || (echo "$(RED)golangci-lint not found. Install: https://golangci-lint.run/usage/install/$(NC)" && exit 1)
	golangci-lint run ./...

## fmt: Format all Go files
fmt:
	@echo "$(GREEN)Formatting code...$(NC)"
	$(GO) fmt ./...

## vet: Run go vet
vet:
	@echo "$(GREEN)Running go vet...$(NC)"
	$(GO) vet ./...

## tidy: Tidy dependencies
tidy:
	@echo "$(GREEN)Tidying dependencies...$(NC)"
	$(GO) mod tidy

## verify: Run fmt, vet, lint and test
verify: fmt vet lint test
	@echo "$(GREEN)✓ All verifications passed$(NC)"

## clean: Remove build artifacts
clean:
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	rm -rf $(BINARY_DIR)
	rm -f coverage.out
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
