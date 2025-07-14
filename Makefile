# Qurious Makefile
# Simplified development workflow
CARGO = cargo
RUSTFMT = rustfmt
DOCKER = docker
PROJECT_NAME = qurious
TPCH_DATA_DIR = qurious/tests/tpch/data
TPCH_DOCKER_IMAGE = ghcr.io/scalytics/tpch-docker:main

# Default target
.PHONY: help
help:
	@echo "Qurious Development Tools"
	@echo "========================"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Code checking
.PHONY: check
check: ## Check code syntax and dependencies
	$(CARGO) check --all-features

.PHONY: check-all
check-all: ## Check all workspace members
	$(CARGO) check --workspace --all-features

# Build
.PHONY: build
build: ## Build project (debug mode)
	$(CARGO) build

.PHONY: build-release
build-release: ## Build project (release mode)
	$(CARGO) build --release

.PHONY: build-all
build-all: ## Build all workspace members
	$(CARGO) build --workspace

.PHONY: test
test: ## Run unit tests
	INCLUDE_TPCH=true $(CARGO) test

# Code formatting
.PHONY: fmt
fmt: ## Check code formatting
	$(CARGO) fmt -- --check

.PHONY: fmt-fix
fmt-fix: ## Format code and auto-fix
	$(CARGO) fmt

# Code quality
.PHONY: clippy
clippy: ## Run clippy code checks
	$(CARGO) clippy --all-features -- -D warnings

.PHONY: clippy-fix
clippy-fix: ## Run clippy and auto-fix
	$(CARGO) clippy --fix --all-features

# Clean
.PHONY: clean
clean: ## Clean build artifacts
	$(CARGO) clean

.PHONY: clean-all
clean-all: clean ## Clean all build artifacts and temp files
	rm -rf target/
	find . -name "*.orig" -delete
	find . -name "*.rej" -delete

# TPC-H data generation
.PHONY: tpch-data
tpch-data: ## Generate TPC-H test data
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run -it -v "$(realpath $(TPCH_DATA_DIR))":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.01

.PHONY: tpch-data-small
tpch-data-small: ## Generate small TPC-H test data
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run -it -v "$(realpath $(TPCH_DATA_DIR))":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.001

.PHONY: tpch-data-large
tpch-data-large: ## Generate large TPC-H test data
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run -it -v "$(realpath $(TPCH_DATA_DIR))":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.1

# Development workflow
.PHONY: dev-setup
dev-setup: ## Setup development environment
	rustup component add rustfmt
	rustup component add clippy
	$(CARGO) install cargo-watch

.PHONY: dev
dev: ## Development mode: watch files and run tests
	cargo watch -x check -x test

.PHONY: dev-test
dev-test: ## Development mode: watch files and run tests
	cargo watch -x test

# Documentation
.PHONY: doc
doc: ## Generate documentation
	$(CARGO) doc --no-deps

.PHONY: doc-open
doc-open: ## Generate and open documentation
	$(CARGO) doc --no-deps --open

# Benchmark
.PHONY: bench
bench: ## Run benchmarks
	$(CARGO) bench

# Dependency management
.PHONY: update
update: ## Update dependencies
	$(CARGO) update

.PHONY: audit
audit: ## Check dependency security vulnerabilities
	$(CARGO) audit

# Release preparation
.PHONY: release-check
release-check: check-all clippy test-all ## Pre-release checks
	@echo "All checks passed, ready to release!"

.PHONY: release-build
release-build: ## Build release version
	$(CARGO) build --release
	@echo "Release build completed: target/release/$(PROJECT_NAME)"

# Database
.PHONY: db-start
db-start: ## Start database services
	$(DOCKER) compose up -d

.PHONY: db-stop
db-stop: ## Stop database services
	$(DOCKER) compose down

.PHONY: db-reset
db-reset: ## Reset database
	$(DOCKER) compose down -v
	$(DOCKER) compose up -d

# Utilities
.PHONY: size
size: build-release ## Show binary file size
	@echo "Binary file size:"
	@ls -lh target/release/$(PROJECT_NAME)

.PHONY: deps-tree
deps-tree: ## Show dependency tree
	$(CARGO) tree

.PHONY: outdated
outdated: ## Check outdated dependencies
	$(CARGO) install-update -a

# Quick development command combinations
.PHONY: quick-check
quick-check: fmt clippy test ## Quick check: format, clippy, test

.PHONY: full-check
full-check: fmt clippy test-all audit ## Full check: format, clippy, all tests, security audit

# Default goal
.DEFAULT_GOAL := help