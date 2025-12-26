.PHONY: help install dev test test-unit test-integration lint format type-check clean docs build publish run-example

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Chronis - Distributed Scheduler Framework"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync

dev: ## Install with development dependencies
	uv sync --all-extras

test: ## Run all tests with coverage
	uv run pytest -v --cov=chronis --cov-report=term-missing --cov-report=html

test-unit: ## Run unit tests only
	uv run pytest tests/test_basic.py -v -m unit

test-integration: ## Run integration tests only
	uv run pytest tests/ -v -m integration

test-watch: ## Run tests in watch mode
	uv run pytest-watch

lint: ## Run linter (ruff check)
	uv run ruff check .

lint-fix: ## Run linter with auto-fix (includes unused imports)
	uv run ruff check . --fix

lint-unused: ## Check for unused imports only
	uv run ruff check . --select F401

lint-unused-fix: ## Remove unused imports automatically
	uv run ruff check . --select F401 --fix

format: ## Format code with ruff
	uv run ruff format .

format-check: ## Check code formatting without changes
	uv run ruff format . --check

type-check: ## Run type checker (mypy)
	uv run mypy chronis/

check: lint type-check ## Run all checks (lint + type-check)

fix: lint-fix format ## Fix linting issues and format code

clean: ## Clean build artifacts and cache files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage coverage.xml
	rm -rf dist build *.egg-info
	rm -rf .tox

docs: ## Build documentation
	@echo "Documentation building not yet implemented"

docs-serve: ## Serve documentation locally
	@echo "Documentation serving not yet implemented"

build: clean ## Build package
	uv build

publish: build ## Publish package to PyPI
	uv publish

publish-test: build ## Publish package to TestPyPI
	uv publish --repository testpypi

run-example: ## Run quickstart example
	uv run python examples/quickstart.py

ci: lint type-check test ## Run CI pipeline (lint, type-check, test)

pre-commit: format lint type-check test ## Run pre-commit checks

version-patch: ## Bump patch version (0.1.0 -> 0.1.1)
	@echo "Current version: $$(grep '^version = ' pyproject.toml | cut -d'"' -f2)"
	@echo "Bump patch version manually in pyproject.toml"

version-minor: ## Bump minor version (0.1.0 -> 0.2.0)
	@echo "Current version: $$(grep '^version = ' pyproject.toml | cut -d'"' -f2)"
	@echo "Bump minor version manually in pyproject.toml"

version-major: ## Bump major version (0.1.0 -> 1.0.0)
	@echo "Current version: $$(grep '^version = ' pyproject.toml | cut -d'"' -f2)"
	@echo "Bump major version manually in pyproject.toml"

setup-dev: ## Setup development environment
	@echo "Setting up development environment..."
	uv sync --all-extras
	@echo "✓ Development environment ready!"

docker-up: ## Start docker services (Redis, PostgreSQL, LocalStack)
	@echo "Docker support not yet implemented"

docker-down: ## Stop docker services
	@echo "Docker support not yet implemented"

docker-logs: ## Show docker logs
	@echo "Docker support not yet implemented"

info: ## Show project information
	@echo "Project: Chronis"
	@echo "Version: $$(grep '^version = ' pyproject.toml | cut -d'"' -f2)"
	@echo "Python: $$(uv run python --version)"
	@echo "uv: $$(uv --version)"
	@echo ""
	@echo "Package info:"
	@uv tree --depth 1 2>/dev/null || echo "  Run 'make install' first"
