CARGO = cargo
DOCKER = docker
TPCH_DATA_DIR = qurious/tests/tpch/data
TPCH_DOCKER_IMAGE = ghcr.io/scalytics/tpch-docker:main

.PHONY: help
help: ## Show available commands
	@echo "Qurious Makefile (minimal)"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: test
# Tests in `qurious/tests/tpch/` run against the TPC-H dataset (SF 0.1), which must be
# generated first from the repository root with:
#   make tpch-data
test: ## Run unit tests (includes TPC-H tests when available)
	INCLUDE_TPCH=true $(CARGO) test

.PHONY: fmt
fmt: ## Format all crates
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting without writing (what CI runs)
	$(CARGO) fmt --all --check

.PHONY: tpch-data
# Scale factor 0.1 is not arbitrary: the expected results in qurious/tests/tpch/ are DataFusion's
# answer files, which are computed at SF 0.1. Generating any other scale makes every TPC-H case
# fail, so there is deliberately only one target here.
tpch-data: ## Generate the TPC-H test data the tpch tests expect (scale factor 0.1)
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run --rm -v "$(CURDIR)/$(TPCH_DATA_DIR)":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.1

.DEFAULT_GOAL := help