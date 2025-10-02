
.DEFAULT_GOAL := help

.PHONY: help up down

## Show available commands
help:
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start docker-compose
	docker-compose up -d

logs: ## Tail logs from docker-compose
	docker-compose logs -f

down: ## Stop docker-compose
	docker-compose down

build: ## Build docker-compose
	docker-compose build

broker-sh: ## Open a bash to the broker
	docker exec -it rust_broker bash

consumer-sh: ## Open a bash to the consumer
	docker exec -it python_consumer bash

python-lint: ## Black linter for python
	black consumer
  
rust-lint: ## Cargo fmt
	cargo fmt --manifest-path broker/Cargo.toml
