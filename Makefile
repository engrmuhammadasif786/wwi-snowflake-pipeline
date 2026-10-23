.PHONY: help setup dbt-deps dbt-compile dbt-build pipeline-run dashboard-preview test lint infra-init infra-fmt infra-validate infra-plan infra-apply docker-up docker-down clean

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
DBT_DIR = dbt_wwi

help: ## Show available Make targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Create .env from .env.example, install Python dependencies, and install dbt packages
	@test -f .env || cp .env.example .env
	$(PIP) install -r requirements.txt
	$(MAKE) dbt-deps

dbt-deps: ## Install external dbt packages (dbt_utils)
	dbt deps --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-compile: dbt-deps ## Compile dbt models against Snowflake profile
	dbt compile --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-build: dbt-deps ## Run and test all dbt models in Snowflake
	dbt build --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

pipeline-run: ## Materialize the full end-to-end WWI Dagster pipeline from CLI
	$(PYTHON) -m dagster_wwi.run_pipeline

dashboard-preview: ## Generate dashboard preview tiles and architecture diagram
	$(PYTHON) dashboard/generate_dashboard_preview.py

dagster-dev: ## Launch Dagster UI locally on http://localhost:3000
	dagster dev -h 0.0.0.0 -p 3000 -m dagster_wwi.definitions

test: ## Run unit and integration test suite with pytest
	$(PYTHON) -m pytest tests/ -v

lint: ## Run ruff linter across Python codebase
	$(PYTHON) -m ruff check dagster_wwi/ dashboard/ tests/

infra-init: ## Initialize Terraform providers (AzureRM + Snowflake)
	cd terraform && terraform init -backend=false

infra-fmt: ## Check Terraform formatting
	cd terraform && terraform fmt -check

infra-validate: infra-init ## Validate Terraform configuration
	cd terraform && terraform validate

infra-plan: ## Generate Terraform execution plan
	cd terraform && terraform plan

infra-apply: ## Apply Terraform infrastructure changes
	cd terraform && terraform apply

docker-up: ## Build and start Dagster webserver and daemon containers
	@test -f .env || cp .env.example .env
	docker compose up --build -d

docker-down: ## Stop and remove Docker Compose containers
	docker compose down

clean: ## Clean temporary build, cache, and dbt target directories
	rm -rf .pytest_cache .ruff_cache dbt_wwi/target dbt_wwi/logs
