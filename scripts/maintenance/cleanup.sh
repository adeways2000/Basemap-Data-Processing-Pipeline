#!/bin/bash

set -e

# Basemap Data Processing Pipeline Cleanup Script
# This script handles cleanup of old data, logs, and temporary files

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
ENVIRONMENT="development"
DRY_RUN=false
CLEANUP_LOGS=true
CLEANUP_TEMP=true
CLEANUP_OLD_TILES=true
CLEANUP_METRICS=true
RETENTION_DAYS=30

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

Cleanup old data and temporary files from the Basemap processing pipeline

OPTIONS:
    -e, --environment ENV    Target environment (development|staging|production) [default: development]
    -r, --retention-days N   Number of days to retain data [default: 30]
    -d, --dry-run           Show what would be cleaned without actually cleaning
    --no-logs               Skip log cleanup
    --no-temp               Skip temporary file cleanup
    --no-tiles              Skip old tile cleanup
    --no-metrics            Skip old metrics cleanup
    -h, --help              Show this help message

EXAMPLES:
    $0 -e staging -r 7
    $0 -e production --dry-run
    $0 --no-logs --no-metrics

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -r|--retention-days)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-logs)
            CLEANUP_LOGS=false
            shift
            ;;
        --no-temp)
            CLEANUP_TEMP=false
            shift
            ;;
        --no-tiles)
            CLEANUP_OLD_TILES=false
            shift
            ;;
        --no-metrics)
            CLEANUP_METRICS=false
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

log_info "Starting cleanup for $ENVIRONMENT environment"
log_info "Retention period: $RETENTION_DAYS days"
log_info "Dry run: $DRY_RUN"

# Change to project root
cd "$PROJECT_ROOT"

# Load configuration
CONFIG_FILE="config/$ENVIRONMENT/config.yaml"
if [[ ! -f "$CONFIG_FILE" ]]; then
    log_error "Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Cleanup logs
cleanup_logs() {
    if [[ "$CLEANUP_LOGS" == false ]]; then
        log_info "Skipping log cleanup"
        return 0
    fi
    
    log_info "Cleaning up old logs..."
    
    local log_dirs=("logs" "airflow/logs")
    local files_cleaned=0
    
    for log_dir in "${log_dirs[@]}"; do
        if [[ -d "$log_dir" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                local count=$(find "$log_dir" -type f -name "*.log" -mtime +$RETENTION_DAYS | wc -l)
                log_info "[DRY RUN] Would clean $count log files from $log_dir"
                files_cleaned=$((files_cleaned + count))
            else
                local count=$(find "$log_dir" -type f -name "*.log" -mtime +$RETENTION_DAYS -delete -print | wc -l)
                log_info "Cleaned $count log files from $log_dir"
                files_cleaned=$((files_cleaned + count))
            fi
        fi
    done
    
    # Cleanup Docker logs if in development
    if [[ "$ENVIRONMENT" == "development" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would clean Docker container logs"
        else
            docker system prune -f --filter "until=${RETENTION_DAYS}h"
            log_info "Cleaned Docker container logs"
        fi
    fi
    
    # Cleanup CloudWatch logs for AWS environments
    if [[ "$ENVIRONMENT" != "development" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would clean CloudWatch logs older than $RETENTION_DAYS days"
        else
            # Set retention policy for CloudWatch log groups
            local log_groups=$(aws logs describe-log-groups --log-group-name-prefix "/aws/basemap/$ENVIRONMENT" --query 'logGroups[].logGroupName' --output text)
            
            for log_group in $log_groups; do
                aws logs put-retention-policy \
                    --log-group-name "$log_group" \
                    --retention-in-days "$RETENTION_DAYS"
                log_info "Set retention policy for $log_group"
            done
        fi
    fi
    
    log_success "Log cleanup completed. Files cleaned: $files_cleaned"
}

# Cleanup temporary files
cleanup_temp_files() {
    if [[ "$CLEANUP_TEMP" == false ]]; then
        log_info "Skipping temporary file cleanup"
        return 0
    fi
    
    log_info "Cleaning up temporary files..."
    
    local temp_dirs=("/tmp/basemap" "data/temp" "data/staging")
    local files_cleaned=0
    
    for temp_dir in "${temp_dirs[@]}"; do
        if [[ -d "$temp_dir" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                local count=$(find "$temp_dir" -type f -mtime +1 | wc -l)
                log_info "[DRY RUN] Would clean $count temporary files from $temp_dir"
                files_cleaned=$((files_cleaned + count))
            else
                local count=$(find "$temp_dir" -type f -mtime +1 -delete -print | wc -l)
                log_info "Cleaned $count temporary files from $temp_dir"
                files_cleaned=$((files_cleaned + count))
                
                # Remove empty directories
                find "$temp_dir" -type d -empty -delete 2>/dev/null || true
            fi
        fi
    done
    
    # Cleanup S3 temporary files for AWS environments
    if [[ "$ENVIRONMENT" != "development" ]]; then
        local s3_bucket="basemap-$ENVIRONMENT-data"
        local cutoff_date=$(date -d "$RETENTION_DAYS days ago" +%Y-%m-%d)
        
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would clean S3 temporary files from s3://$s3_bucket/temp/"
        else
            aws s3 ls "s3://$s3_bucket/temp/" --recursive | \
            awk -v cutoff="$cutoff_date" '$1 < cutoff {print $4}' | \
            while read -r file; do
                aws s3 rm "s3://$s3_bucket/$file"
                files_cleaned=$((files_cleaned + 1))
            done
            
            log_info "Cleaned temporary files from S3"
        fi
    fi
    
    log_success "Temporary file cleanup completed. Files cleaned: $files_cleaned"
}

# Cleanup old tiles
cleanup_old_tiles() {
    if [[ "$CLEANUP_OLD_TILES" == false ]]; then
        log_info "Skipping old tile cleanup"
        return 0
    fi
    
    log_info "Cleaning up old tiles..."
    
    local tiles_cleaned=0
    
    if [[ "$ENVIRONMENT" == "development" ]]; then
        local tile_dir="data/tiles"
        
        if [[ -d "$tile_dir" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                local count=$(find "$tile_dir" -name "*.mvt" -mtime +$RETENTION_DAYS | wc -l)
                log_info "[DRY RUN] Would clean $count old tiles from $tile_dir"
                tiles_cleaned=$count
            else
                local count=$(find "$tile_dir" -name "*.mvt" -mtime +$RETENTION_DAYS -delete -print | wc -l)
                log_info "Cleaned $count old tiles from $tile_dir"
                tiles_cleaned=$count
            fi
        fi
        
    else
        # Cleanup old tiles from S3
        local s3_bucket="basemap-$ENVIRONMENT-data"
        local cutoff_date=$(date -d "$RETENTION_DAYS days ago" +%Y-%m-%d)
        
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would clean old tiles from s3://$s3_bucket/tiles/"
        else
            # List and delete old tiles
            aws s3 ls "s3://$s3_bucket/tiles/" --recursive | \
            awk -v cutoff="$cutoff_date" '$1 < cutoff && $4 ~ /\.mvt$/ {print $4}' | \
            while read -r file; do
                aws s3 rm "s3://$s3_bucket/$file"
                tiles_cleaned=$((tiles_cleaned + 1))
            done
            
            log_info "Cleaned old tiles from S3"
        fi
    fi
    
    log_success "Old tile cleanup completed. Tiles cleaned: $tiles_cleaned"
}

# Cleanup old metrics
cleanup_old_metrics() {
    if [[ "$CLEANUP_METRICS" == false ]]; then
        log_info "Skipping metrics cleanup"
        return 0
    fi
    
    log_info "Cleaning up old metrics..."
    
    # Cleanup database metrics
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would clean metrics older than $RETENTION_DAYS days from database"
    else
        # Connect to database and clean old metrics
        python3 << EOF
import os
import sys
import yaml
from datetime import datetime, timedelta
import psycopg2

# Load configuration
with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)

# Database connection
db_config = config['database']
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    database=db_config['name'],
    user=db_config['user'],
    password=db_config['password']
)

cursor = conn.cursor()

# Calculate cutoff date
cutoff_date = datetime.now() - timedelta(days=$RETENTION_DAYS)

# Clean old metrics
cursor.execute("""
    DELETE FROM monitoring.metrics 
    WHERE timestamp < %s
""", (cutoff_date,))

metrics_deleted = cursor.rowcount

# Clean old processing runs
cursor.execute("""
    DELETE FROM basemap.processing_runs 
    WHERE start_time < %s AND status IN ('completed', 'failed')
""", (cutoff_date,))

runs_deleted = cursor.rowcount

conn.commit()
cursor.close()
conn.close()

print(f"Deleted {metrics_deleted} old metrics records")
print(f"Deleted {runs_deleted} old processing run records")
EOF
    fi
    
    # Cleanup Prometheus metrics (if using external Prometheus)
    if [[ "$ENVIRONMENT" != "development" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would clean Prometheus metrics older than $RETENTION_DAYS days"
        else
            # Note: Prometheus cleanup is typically handled by retention settings
            log_info "Prometheus metrics cleanup is handled by retention settings"
        fi
    fi
    
    log_success "Metrics cleanup completed"
}

# Cleanup Docker resources
cleanup_docker_resources() {
    if [[ "$ENVIRONMENT" != "development" ]]; then
        return 0
    fi
    
    log_info "Cleaning up Docker resources..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would clean unused Docker resources"
        docker system df
    else
        # Remove unused containers, networks, images, and volumes
        docker system prune -af --volumes --filter "until=${RETENTION_DAYS}h"
        
        # Remove unused images
        docker image prune -af --filter "until=${RETENTION_DAYS}h"
        
        log_info "Docker resources cleaned"
        docker system df
    fi
    
    log_success "Docker cleanup completed"
}

# Generate cleanup report
generate_cleanup_report() {
    log_info "Generating cleanup report..."
    
    local report_file="cleanup_report_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$report_file" << EOF
Basemap Data Processing Pipeline Cleanup Report
Generated: $(date)
Environment: $ENVIRONMENT
Retention Period: $RETENTION_DAYS days
Dry Run: $DRY_RUN

Cleanup Summary:
- Logs: $([ "$CLEANUP_LOGS" == true ] && echo "✓" || echo "✗")
- Temporary Files: $([ "$CLEANUP_TEMP" == true ] && echo "✓" || echo "✗")
- Old Tiles: $([ "$CLEANUP_OLD_TILES" == true ] && echo "✓" || echo "✗")
- Metrics: $([ "$CLEANUP_METRICS" == true ] && echo "✓" || echo "✗")

Disk Usage Before Cleanup:
$(df -h)

System Information:
- Hostname: $(hostname)
- Kernel: $(uname -r)
- Uptime: $(uptime)

Docker Information (if applicable):
$(if command -v docker &> /dev/null; then docker system df; fi)

EOF
    
    log_info "Cleanup report saved to: $report_file"
}

# Main cleanup function
main() {
    log_info "Starting cleanup process..."
    
    # Generate initial report
    generate_cleanup_report
    
    # Run cleanup functions
    cleanup_logs
    cleanup_temp_files
    cleanup_old_tiles
    cleanup_old_metrics
    cleanup_docker_resources
    
    log_success "Cleanup process completed successfully!"
    
    # Show disk usage after cleanup
    log_info "Disk usage after cleanup:"
    df -h
}

# Run main function
main "$@"
