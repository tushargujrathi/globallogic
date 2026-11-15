# Every target runs in Docker; the only host requirements are Docker (with Compose v2) and make.

COMPOSE ?= docker compose
DEV     := $(COMPOSE) --profile test run --rm --build tests

.DEFAULT_GOAL := help
.PHONY: help up down clean logs ps test lint audit check format worker

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-8s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

up: .env ## Build and boot everything (postgres, rabbitmq, api, relay, consumer, worker)
	$(COMPOSE) up --build --detach --wait

down: ## Stop the stack (data is kept)
	$(COMPOSE) --profile test down

clean: ## Stop the stack and delete all data volumes
	$(COMPOSE) --profile test down --volumes --remove-orphans

logs: ## Follow the logs of all services
	$(COMPOSE) logs --follow

ps: ## Show service status
	$(COMPOSE) ps

test: up ## Run the whole test suite (unit + integration + e2e against the live stack)
	$(DEV) pytest

lint: .env ## Quality gates: formatter check, linter, type checker, security scanner (bandit)
	$(DEV) sh -c 'ruff format --check . && ruff check . && mypy && bandit -q -c pyproject.toml -r src'

audit: .env ## Scan the locked dependencies for known vulnerabilities (pip-audit, needs network)
	$(DEV) pip-audit -r requirements.lock

check: lint test ## lint + test: everything CI runs

format: .env ## Auto-format and auto-fix the code in your working tree
	$(COMPOSE) --profile test run --rm --build --user "$$(id -u):$$(id -g)" -v "$(CURDIR):/app" \
		tests sh -c 'ruff format . && ruff check --fix .'

worker: .env ## Re-run the worker in the foreground with flags: make worker ARGS="--fail-rate=1"
	$(COMPOSE) stop worker
	$(COMPOSE) run --rm worker $(ARGS)
