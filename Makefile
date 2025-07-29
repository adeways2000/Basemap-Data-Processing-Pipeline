# Basemap Data Processing Pipeline Makefile
# Provides convenient commands for development, testing, and deployment

.PHONY: help install test lint format clean build deploy docs

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
POETRY := poetry
DOCKER := docker
DOCKER_COMPOSE := docker-compose
PROJECT_NAME := basemap-data-pipeline
ENVIRONMENT := development

# Colors for output
BLUE := \033[34m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

help: ## Show this help message
	@echo "$(BLUE)Basemap Data Processing Pipeline$(RESET)"
	@echo "================================"
	@echo ""
	@echo "Available commands:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Environment: $(YELLOW)$(ENVIRONMENT)$(RESET)"

# Installation and Setup
install: ## Install project dependencies
	@echo "$(BLUE)Installing dependencies...$(RESET)"
	$(POETRY) install
	@echo "$(GREEN)Dependencies installed successfully!$(RESET)"

install-dev: ## Install development dependencies
	@echo "$(BLUE)Installing development dependencies...$(RESET)"
	$(POETRY) install --with dev,test
	pre-commit install
	@echo "$(GREEN)Development environment ready!$(RESET)"

setup: ## Initial project setup
	@echo "$(BLUE)Setting up project...$(RESET)"
	$(POETRY) install --with dev,test
	pre-commit install
	mkdir -p data/{raw,processed,tiles} logs
	$(DOCKER_COMPOSE) up -d postgres redis
	@echo "$(GREEN)Project setup complete!$(RESET)"

# Code Quality
lint: ## Run linting checks
	@echo "$(BLUE)Running linting checks...$(RESET)"
	$(POETRY) run flake8 src/ tests/
	$(POETRY) run black --check src/ tests/
	$(POETRY) run isort --check-only src/ tests/
	$(POETRY) run mypy src/
	@echo "$(GREEN)Linting checks passed!$(RESET)"

format: ## Format code
	@echo "$(BLUE)Formatting code...$(RESET)"
	$(POETRY) run black src/ tests/
	$(POETRY) run isort src/ tests/
	@echo "$(GREEN)Code formatted!$(RESET)"

security: ## Run security checks
	@echo "$(BLUE)Running security checks...$(RESET)"
	$(POETRY) run bandit -r src/
	$(POETRY) run safety check
	@echo "$(GREEN)Security checks passed!$(RESET)"

# Testing
test: ## Run all tests
	@echo "$(BLUE)Running tests...$(RESET)"
	$(POETRY) run pytest tests/ -v --cov=src --cov-report=html --cov-report=term
	@echo "$(GREEN)Tests completed!$(RESET)"

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(RESET)"
	$(POETRY) run pytest tests/unit/ -v
	@echo "$(GREEN)Unit tests completed!$(RESET)"

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(RESET)"
	$(POETRY) run pytest tests/integration/ -v
	@echo "$(GREEN)Integration tests completed!$(RESET)"

test-performance: ## Run performance tests
	@echo "$(BLUE)Running performance tests...$(RESET)"
	$(POETRY) run pytest tests/performance/ -v --benchmark-only
	@echo "$(GREEN)Performance tests completed!$(RESET)"

test-watch: ## Run tests in watch mode
	@echo "$(BLUE)Running tests in watch mode...$(RESET)"
	$(POETRY) run ptw tests/ src/ --runner "pytest -v"

coverage: ## Generate test coverage report
	@echo "$(BLUE)Generating coverage report...$(RESET)"
	$(POETRY) run pytest tests/ --cov=src --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in htmlcov/$(RESET)"

# Docker Operations
docker-build: ## Build Docker images
	@echo "$(BLUE)Building Docker images...$(RESET)"
	$(DOCKER_COMPOSE) build
	@echo "$(GREEN)Docker images built!$(RESET)"

docker-up: ## Start Docker services
	@echo "$(BLUE)Starting Docker services...$(RESET)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Docker services started!$(RESET)"

docker-down: ## Stop Docker services
	@echo "$(BLUE)Stopping Docker services...$(RESET)"
	$(DOCKER_COMPOSE) down
	@echo "$(GREEN)Docker services stopped!$(RESET)"

docker-logs: ## View Docker logs
	$(DOCKER_COMPOSE) logs -f

docker-clean: ## Clean Docker resources
	@echo "$(BLUE)Cleaning Docker resources...$(RESET)"
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
	$(DOCKER) system prune -f
	@echo "$(GREEN)Docker resources cleaned!$(RESET)"

# Development Services
dev-up: ## Start development environment
	@echo "$(BLUE)Starting development environment...$(RESET)"
	$(DOCKER_COMPOSE) up -d postgres redis minio
	@echo "$(GREEN)Development services started!$(RESET)"
	@echo "Services available at:"
	@echo "  - PostgreSQL: localhost:5432"
	@echo "  - Redis: localhost:6379"
	@echo "  - MinIO: http://localhost:9001"

dev-down: ## Stop development environment
	@echo "$(BLUE)Stopping development environment...$(RESET)"
	$(DOCKER_COMPOSE) down
	@echo "$(GREEN)Development services stopped!$(RESET)"

airflow-up: ## Start Airflow services
	@echo "$(BLUE)Starting Airflow services...$(RESET)"
	$(DOCKER_COMPOSE) up -d airflow-webserver airflow-scheduler
	@echo "$(GREEN)Airflow services started!$(RESET)"
	@echo "Airflow UI: http://localhost:8080"

jupyter: ## Start Jupyter notebook
	@echo "$(BLUE)Starting Jupyter notebook...$(RESET)"
	$(DOCKER_COMPOSE) up -d jupyter
	@echo "$(GREEN)Jupyter started!$(RESET)"
	@echo "Jupyter Lab: http://localhost:8888"

monitoring: ## Start monitoring services
	@echo "$(BLUE)Starting monitoring services...$(RESET)"
	$(DOCKER_COMPOSE) up -d prometheus grafana
	@echo "$(GREEN)Monitoring services started!$(RESET)"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000"

# Database Operations
db-migrate: ## Run database migrations
	@echo "$(BLUE)Running database migrations...$(RESET)"
	$(POETRY) run python scripts/migrate_database.py --environment $(ENVIRONMENT)
	@echo "$(GREEN)Database migrations completed!$(RESET)"

db-seed: ## Seed database with sample data
	@echo "$(BLUE)Seeding database with sample data...$(RESET)"
	$(POETRY) run python scripts/seed_database.py --environment $(ENVIRONMENT)
	@echo "$(GREEN)Database seeded!$(RESET)"

db-reset: ## Reset database
	@echo "$(BLUE)Resetting database...$(RESET)"
	$(DOCKER_COMPOSE) exec postgres psql -U basemap_user -d basemap_dev -c "DROP SCHEMA IF EXISTS basemap CASCADE; DROP SCHEMA IF EXISTS monitoring CASCADE;"
	$(MAKE) db-migrate
	@echo "$(GREEN)Database reset completed!$(RESET)"

# Data Processing
process-sample: ## Process sample OSM data
	@echo "$(BLUE)Processing sample OSM data...$(RESET)"
	$(POETRY) run python -m src.data_ingestion.osm_ingestion --config config/$(ENVIRONMENT)/config.yaml --sample
	@echo "$(GREEN)Sample data processed!$(RESET)"

generate-tiles: ## Generate sample tiles
	@echo "$(BLUE)Generating sample tiles...$(RESET)"
	$(POETRY) run python -m src.tile_generation.vector_tile_generator --config config/$(ENVIRONMENT)/config.yaml
	@echo "$(GREEN)Tiles generated!$(RESET)"

# Deployment
deploy-dev: ## Deploy to development environment
	@echo "$(BLUE)Deploying to development...$(RESET)"
	./scripts/deployment/deploy.sh --environment development --type full
	@echo "$(GREEN)Development deployment completed!$(RESET)"

deploy-staging: ## Deploy to staging environment
	@echo "$(BLUE)Deploying to staging...$(RESET)"
	./scripts/deployment/deploy.sh --environment staging --type full
	@echo "$(GREEN)Staging deployment completed!$(RESET)"

deploy-prod: ## Deploy to production environment
	@echo "$(BLUE)Deploying to production...$(RESET)"
	./scripts/deployment/deploy.sh --environment production --type full
	@echo "$(GREEN)Production deployment completed!$(RESET)"

# Maintenance
clean: ## Clean temporary files and caches
	@echo "$(BLUE)Cleaning temporary files...$(RESET)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .coverage htmlcov/ .pytest_cache/ dist/ build/
	@echo "$(GREEN)Cleanup completed!$(RESET)"

clean-data: ## Clean processed data and logs
	@echo "$(BLUE)Cleaning data and logs...$(RESET)"
	./scripts/maintenance/cleanup.sh --environment $(ENVIRONMENT)
	@echo "$(GREEN)Data cleanup completed!$(RESET)"

backup: ## Backup important data
	@echo "$(BLUE)Creating backup...$(RESET)"
	./scripts/maintenance/backup.sh --environment $(ENVIRONMENT)
	@echo "$(GREEN)Backup completed!$(RESET)"

# Documentation
docs: ## Build documentation
	@echo "$(BLUE)Building documentation...$(RESET)"
	$(POETRY) run mkdocs build
	@echo "$(GREEN)Documentation built in site/$(RESET)"

docs-serve: ## Serve documentation locally
	@echo "$(BLUE)Serving documentation...$(RESET)"
	$(POETRY) run mkdocs serve
	@echo "$(GREEN)Documentation available at http://localhost:8000$(RESET)"

docs-deploy: ## Deploy documentation
	@echo "$(BLUE)Deploying documentation...$(RESET)"
	$(POETRY) run mkdocs gh-deploy
	@echo "$(GREEN)Documentation deployed!$(RESET)"

# Monitoring and Health Checks
health-check: ## Run health checks
	@echo "$(BLUE)Running health checks...$(RESET)"
	$(POETRY) run python scripts/health_check.py --environment $(ENVIRONMENT)
	@echo "$(GREEN)Health checks completed!$(RESET)"

logs: ## View application logs
	@echo "$(BLUE)Viewing logs...$(RESET)"
	tail -f logs/basemap.log

metrics: ## View system metrics
	@echo "$(BLUE)Collecting system metrics...$(RESET)"
	$(POETRY) run python scripts/collect_metrics.py --environment $(ENVIRONMENT)

# Utility Commands
shell: ## Open Python shell with project context
	@echo "$(BLUE)Opening Python shell...$(RESET)"
	$(POETRY) run python -i -c "import sys; sys.path.append('src')"

notebook: ## Open Jupyter notebook
	@echo "$(BLUE)Opening Jupyter notebook...$(RESET)"
	$(POETRY) run jupyter lab notebooks/

psql: ## Connect to PostgreSQL database
	@echo "$(BLUE)Connecting to PostgreSQL...$(RESET)"
	$(DOCKER_COMPOSE) exec postgres psql -U basemap_user -d basemap_dev

redis-cli: ## Connect to Redis
	@echo "$(BLUE)Connecting to Redis...$(RESET)"
	$(DOCKER_COMPOSE) exec redis redis-cli

# CI/CD Simulation
ci: ## Simulate CI pipeline locally
	@echo "$(BLUE)Running CI pipeline...$(RESET)"
	$(MAKE) lint
	$(MAKE) security
	$(MAKE) test
	$(MAKE) docker-build
	@echo "$(GREEN)CI pipeline completed successfully!$(RESET)"

# Environment-specific targets
dev: ENVIRONMENT=development
dev: dev-up ## Start development environment

staging: ENVIRONMENT=staging
staging: deploy-staging ## Deploy to staging

prod: ENVIRONMENT=production
prod: deploy-prod ## Deploy to production

# Status and Information
status: ## Show project status
	@echo "$(BLUE)Project Status$(RESET)"
	@echo "=============="
	@echo "Environment: $(YELLOW)$(ENVIRONMENT)$(RESET)"
	@echo "Python Version: $(shell $(PYTHON) --version)"
	@echo "Poetry Version: $(shell $(POETRY) --version)"
	@echo "Docker Version: $(shell $(DOCKER) --version)"
	@echo ""
	@echo "$(BLUE)Services Status:$(RESET)"
	@$(DOCKER_COMPOSE) ps 2>/dev/null || echo "Docker Compose not running"
	@echo ""
	@echo "$(BLUE)Git Status:$(RESET)"
	@git status --porcelain || echo "Not a git repository"

info: ## Show project information
	@echo "$(BLUE)Basemap Data Processing Pipeline$(RESET)"
	@echo "================================"
	@echo ""
	@echo "A production-ready data processing pipeline for generating"
	@echo "basemap tiles from OpenStreetMap data using Apache Spark,"
	@echo "Apache Airflow, and AWS services."
	@echo ""
	@echo "$(BLUE)Key Features:$(RESET)"
	@echo "- Large-scale OSM data processing"
	@echo "- Vector tile generation (MVT format)"
	@echo "- Automated workflow orchestration"
	@echo "- Comprehensive monitoring and alerting"
	@echo "- Multi-environment deployment support"
	@echo ""
	@echo "$(BLUE)Quick Start:$(RESET)"
	@echo "  make setup    # Initial setup"
	@echo "  make dev-up   # Start development services"
	@echo "  make test     # Run tests"
	@echo ""
	@echo "For more information, see: docs/README.md"
