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
# Tests in `qurious/tests/tpch/` run against the TPC-H dataset and require
# data generation ahead of time (default scale factor is 0.01 here).
# Generate data from the repository root with:
#   make tpch-data
test: ## Run unit tests (includes TPC-H tests when available)
	INCLUDE_TPCH=true $(CARGO) test

.PHONY: tpch-data
tpch-data: ## Generate TPC-H test data (scale factor 0.01)
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run -it -v "$(realpath $(TPCH_DATA_DIR))":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.01

.PHONY: tpch-data-small
tpch-data-small: ## Generate small TPC-H test data (scale factor 0.001)
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run -it -v "$(realpath $(TPCH_DATA_DIR))":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.001

.PHONY: tpch-data-large
tpch-data-large: ## Generate large TPC-H test data (scale factor 0.1)
	mkdir -p $(TPCH_DATA_DIR)
	$(DOCKER) run -it -v "$(realpath $(TPCH_DATA_DIR))":/data $(TPCH_DOCKER_IMAGE) -vf -s 0.1

.DEFAULT_GOAL := help