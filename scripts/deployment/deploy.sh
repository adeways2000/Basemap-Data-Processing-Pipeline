#!/bin/bash

set -e

# Basemap Data Processing Pipeline Deployment Script
# This script handles deployment to different environments

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
ENVIRONMENT="development"
DEPLOY_TYPE="full"
SKIP_TESTS=false
SKIP_MIGRATION=false
DRY_RUN=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy the Basemap Data Processing Pipeline

OPTIONS:
    -e, --environment ENV    Target environment (development|staging|production) [default: development]
    -t, --type TYPE         Deployment type (full|infrastructure|application|airflow) [default: full]
    -s, --skip-tests        Skip running tests before deployment
    -m, --skip-migration    Skip database migrations
    -d, --dry-run          Show what would be deployed without actually deploying
    -h, --help             Show this help message

EXAMPLES:
    $0 -e staging -t full
    $0 -e production -t application --skip-tests
    $0 -e development -d

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -t|--type)
            DEPLOY_TYPE="$2"
            shift 2
            ;;
        -s|--skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        -m|--skip-migration)
            SKIP_MIGRATION=true
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(development|staging|production)$ ]]; then
    log_error "Invalid environment: $ENVIRONMENT"
    log_error "Must be one of: development, staging, production"
    exit 1
fi

# Validate deployment type
if [[ ! "$DEPLOY_TYPE" =~ ^(full|infrastructure|application|airflow)$ ]]; then
    log_error "Invalid deployment type: $DEPLOY_TYPE"
    log_error "Must be one of: full, infrastructure, application, airflow"
    exit 1
fi

log_info "Starting deployment to $ENVIRONMENT environment"
log_info "Deployment type: $DEPLOY_TYPE"
log_info "Dry run: $DRY_RUN"

# Change to project root
cd "$PROJECT_ROOT"

# Load environment-specific configuration
CONFIG_FILE="config/$ENVIRONMENT/config.yaml"
if [[ ! -f "$CONFIG_FILE" ]]; then
    log_error "Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Pre-deployment checks
pre_deployment_checks() {
    log_info "Running pre-deployment checks..."
    
    # Check if required tools are installed
    local required_tools=("docker" "docker-compose" "terraform" "aws")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "$tool is not installed or not in PATH"
            exit 1
        fi
    done
    
    # Check AWS credentials for non-development environments
    if [[ "$ENVIRONMENT" != "development" ]]; then
        if ! aws sts get-caller-identity &> /dev/null; then
            log_error "AWS credentials not configured or invalid"
            exit 1
        fi
    fi
    
    # Check if configuration is valid
    if ! python3 -c "import yaml; yaml.safe_load(open('$CONFIG_FILE'))" 2>/dev/null; then
        log_error "Invalid YAML configuration in $CONFIG_FILE"
        exit 1
    fi
    
    log_success "Pre-deployment checks passed"
}

# Run tests
run_tests() {
    if [[ "$SKIP_TESTS" == true ]]; then
        log_warning "Skipping tests as requested"
        return 0
    fi
    
    log_info "Running tests..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would run: python -m pytest tests/"
        return 0
    fi
    
    # Install test dependencies
    pip install -r requirements-test.txt
    
    # Run unit tests
    python -m pytest tests/unit/ -v --tb=short
    
    # Run integration tests for non-production environments
    if [[ "$ENVIRONMENT" != "production" ]]; then
        python -m pytest tests/integration/ -v --tb=short
    fi
    
    log_success "All tests passed"
}

# Database migrations
run_migrations() {
    if [[ "$SKIP_MIGRATION" == true ]]; then
        log_warning "Skipping database migrations as requested"
        return 0
    fi
    
    log_info "Running database migrations..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would run database migrations"
        return 0
    fi
    
    # Run database migrations using Alembic or custom migration script
    python scripts/migrate_database.py --environment "$ENVIRONMENT"
    
    log_success "Database migrations completed"
}

# Deploy infrastructure
deploy_infrastructure() {
    log_info "Deploying infrastructure..."
    
    if [[ "$ENVIRONMENT" == "development" ]]; then
        log_info "Using Docker Compose for development environment"
        
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would run: docker-compose up -d"
            return 0
        fi
        
        docker-compose down --remove-orphans
        docker-compose build
        docker-compose up -d
        
        # Wait for services to be healthy
        log_info "Waiting for services to be healthy..."
        sleep 30
        
        # Check service health
        docker-compose ps
        
    else
        log_info "Deploying AWS infrastructure with Terraform"
        
        cd terraform/
        
        # Initialize Terraform
        terraform init -backend-config="environments/$ENVIRONMENT/backend.tfvars"
        
        # Plan deployment
        terraform plan -var-file="environments/$ENVIRONMENT/terraform.tfvars" -out=tfplan
        
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Terraform plan completed. Would apply changes."
            cd ..
            return 0
        fi
        
        # Apply changes
        terraform apply tfplan
        
        cd ..
    fi
    
    log_success "Infrastructure deployment completed"
}

# Deploy application
deploy_application() {
    log_info "Deploying application..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would deploy application code"
        return 0
    fi
    
    if [[ "$ENVIRONMENT" == "development" ]]; then
        # For development, restart application containers
        docker-compose restart airflow-webserver airflow-scheduler
        
    else
        # For staging/production, deploy to AWS
        
        # Build and push Docker images
        log_info "Building and pushing Docker images..."
        
        # Build application image
        docker build -t basemap-app:$ENVIRONMENT -f docker/Dockerfile.airflow .
        
        # Tag and push to ECR
        AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        ECR_REGISTRY="$AWS_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com"
        
        # Login to ECR
        aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $ECR_REGISTRY
        
        # Tag and push
        docker tag basemap-app:$ENVIRONMENT $ECR_REGISTRY/basemap-app:$ENVIRONMENT
        docker push $ECR_REGISTRY/basemap-app:$ENVIRONMENT
        
        # Update ECS service or EMR cluster
        log_info "Updating AWS services..."
        
        # Update Airflow deployment
        aws ecs update-service \
            --cluster basemap-$ENVIRONMENT \
            --service airflow-webserver \
            --force-new-deployment
        
        aws ecs update-service \
            --cluster basemap-$ENVIRONMENT \
            --service airflow-scheduler \
            --force-new-deployment
    fi
    
    log_success "Application deployment completed"
}

# Deploy Airflow DAGs
deploy_airflow() {
    log_info "Deploying Airflow DAGs..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would deploy Airflow DAGs"
        return 0
    fi
    
    if [[ "$ENVIRONMENT" == "development" ]]; then
        # For development, DAGs are mounted as volumes
        docker-compose restart airflow-scheduler
        
    else
        # For staging/production, sync DAGs to S3
        aws s3 sync airflow/dags/ s3://basemap-$ENVIRONMENT-airflow/dags/ --delete
        
        # Trigger DAG refresh
        AIRFLOW_ENDPOINT="https://airflow-$ENVIRONMENT.company.com"
        curl -X POST "$AIRFLOW_ENDPOINT/api/v1/dags/refresh" \
            -H "Authorization: Bearer $AIRFLOW_API_TOKEN"
    fi
    
    log_success "Airflow DAGs deployment completed"
}

# Post-deployment verification
post_deployment_verification() {
    log_info "Running post-deployment verification..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would run post-deployment verification"
        return 0
    fi
    
    # Health checks
    if [[ "$ENVIRONMENT" == "development" ]]; then
        # Check local services
        local services=("postgres:5432" "redis:6379" "airflow-webserver:8080")
        for service in "${services[@]}"; do
            if docker-compose exec -T ${service%:*} nc -z localhost ${service#*:}; then
                log_success "$service is healthy"
            else
                log_error "$service is not responding"
                exit 1
            fi
        done
        
    else
        # Check AWS services
        log_info "Checking AWS service health..."
        
        # Check RDS
        aws rds describe-db-clusters \
            --db-cluster-identifier basemap-$ENVIRONMENT \
            --query 'DBClusters[0].Status' \
            --output text
        
        # Check ECS services
        aws ecs describe-services \
            --cluster basemap-$ENVIRONMENT \
            --services airflow-webserver airflow-scheduler \
            --query 'services[].{Name:serviceName,Status:status,Running:runningCount,Desired:desiredCount}'
    fi
    
    # Run smoke tests
    log_info "Running smoke tests..."
    python scripts/smoke_tests.py --environment "$ENVIRONMENT"
    
    log_success "Post-deployment verification completed"
}

# Rollback function
rollback() {
    log_error "Deployment failed. Initiating rollback..."
    
    if [[ "$ENVIRONMENT" == "development" ]]; then
        docker-compose down
        log_info "Development environment stopped"
        
    else
        # Rollback AWS deployment
        log_info "Rolling back AWS deployment..."
        
        # Rollback ECS services to previous task definition
        aws ecs update-service \
            --cluster basemap-$ENVIRONMENT \
            --service airflow-webserver \
            --task-definition basemap-airflow:PREVIOUS
        
        aws ecs update-service \
            --cluster basemap-$ENVIRONMENT \
            --service airflow-scheduler \
            --task-definition basemap-airflow:PREVIOUS
    fi
    
    log_error "Rollback completed"
    exit 1
}

# Main deployment logic
main() {
    # Set up error handling
    trap rollback ERR
    
    # Run pre-deployment checks
    pre_deployment_checks
    
    # Run tests
    run_tests
    
    # Run database migrations
    run_migrations
    
    # Deploy based on type
    case "$DEPLOY_TYPE" in
        "full")
            deploy_infrastructure
            deploy_application
            deploy_airflow
            ;;
        "infrastructure")
            deploy_infrastructure
            ;;
        "application")
            deploy_application
            ;;
        "airflow")
            deploy_airflow
            ;;
    esac
    
    # Post-deployment verification
    post_deployment_verification
    
    log_success "Deployment to $ENVIRONMENT completed successfully!"
    
    # Display useful information
    if [[ "$ENVIRONMENT" == "development" ]]; then
        echo ""
        log_info "Development environment is ready:"
        log_info "  Airflow UI: http://localhost:8080"
        log_info "  Jupyter: http://localhost:8888"
        log_info "  Grafana: http://localhost:3000"
        log_info "  MinIO: http://localhost:9001"
    fi
}

# Run main function
main "$@"
