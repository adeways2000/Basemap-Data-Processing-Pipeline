"""
Basemap Data Processing Infrastructure
Terraform configuration for AWS resources supporting the basemap data pipeline.

This configuration demonstrates:
- Infrastructure as Code best practices
- AWS services integration (EMR, S3, RDS, CloudWatch)
- Security and access management
- Cost optimization strategies
- Scalability and high availability
- Monitoring and observability
"""

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }
  
  backend "s3" {
    # Backend configuration will be provided during terraform init
    # bucket = "mapbox-basemap-terraform-state"
    # key    = "basemap-processing/terraform.tfstate"
    # region = "us-west-2"
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "basemap-processing"
      Environment = var.environment
      ManagedBy   = "terraform"
      Team        = "basemap-team"
      CostCenter  = "engineering"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

# Random suffix for unique resource names
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
  
  # Common naming convention
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Resource tags
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Team        = "basemap-team"
  }
}

# VPC and Networking
module "vpc" {
  source = "./modules/vpc"
  
  name_prefix         = local.name_prefix
  vpc_cidr           = var.vpc_cidr
  availability_zones = slice(data.aws_availability_zones.available.names, 0, 3)
  
  # Enable DNS support for RDS and other services
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  # Enable VPC Flow Logs for security monitoring
  enable_flow_log = true
  
  tags = local.common_tags
}

# S3 Buckets for data storage
module "s3_storage" {
  source = "./modules/s3"
  
  name_prefix = local.name_prefix
  suffix      = random_id.suffix.hex
  
  # Bucket configuration
  buckets = {
    raw_data = {
      name        = "${local.name_prefix}-raw-data-${random_id.suffix.hex}"
      description = "Raw geospatial data storage"
      lifecycle_rules = [
        {
          id     = "raw_data_lifecycle"
          status = "Enabled"
          transitions = [
            {
              days          = 30
              storage_class = "STANDARD_IA"
            },
            {
              days          = 90
              storage_class = "GLACIER"
            },
            {
              days          = 365
              storage_class = "DEEP_ARCHIVE"
            }
          ]
        }
      ]
    }
    
    processed_data = {
      name        = "${local.name_prefix}-processed-data-${random_id.suffix.hex}"
      description = "Processed geospatial data storage"
      lifecycle_rules = [
        {
          id     = "processed_data_lifecycle"
          status = "Enabled"
          transitions = [
            {
              days          = 60
              storage_class = "STANDARD_IA"
            },
            {
              days          = 180
              storage_class = "GLACIER"
            }
          ]
        }
      ]
    }
    
    tiles = {
      name        = "${local.name_prefix}-tiles-${random_id.suffix.hex}"
      description = "Generated map tiles storage"
      cors_rules = [
        {
          allowed_headers = ["*"]
          allowed_methods = ["GET", "HEAD"]
          allowed_origins = ["*"]
          max_age_seconds = 3600
        }
      ]
    }
    
    logs = {
      name        = "${local.name_prefix}-logs-${random_id.suffix.hex}"
      description = "Application and infrastructure logs"
      lifecycle_rules = [
        {
          id     = "logs_lifecycle"
          status = "Enabled"
          expiration = {
            days = 90
          }
        }
      ]
    }
    
    terraform_state = {
      name        = "${local.name_prefix}-terraform-state-${random_id.suffix.hex}"
      description = "Terraform state storage"
      versioning = true
      encryption = true
    }
  }
  
  tags = local.common_tags
}

# RDS PostgreSQL with PostGIS for spatial database
module "rds_postgres" {
  source = "./modules/rds"
  
  name_prefix = local.name_prefix
  
  # Database configuration
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = var.rds_instance_class
  
  # Storage configuration
  allocated_storage     = var.rds_allocated_storage
  max_allocated_storage = var.rds_max_allocated_storage
  storage_type         = "gp3"
  storage_encrypted    = true
  
  # Database settings
  db_name  = var.db_name
  username = var.db_username
  
  # Network configuration
  vpc_id                = module.vpc.vpc_id
  subnet_ids           = module.vpc.private_subnet_ids
  vpc_security_group_ids = [aws_security_group.rds.id]
  
  # Backup and maintenance
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  # Performance and monitoring
  performance_insights_enabled = true
  monitoring_interval         = 60
  
  # High availability
  multi_az = var.environment == "production"
  
  tags = local.common_tags
}

# EMR cluster for Spark processing
module "emr_cluster" {
  source = "./modules/emr"
  
  name_prefix = local.name_prefix
  
  # Cluster configuration
  release_label = "emr-6.10.0"
  applications  = ["Spark", "Hadoop", "Hive", "Livy"]
  
  # Instance configuration
  master_instance_type = var.emr_master_instance_type
  core_instance_type   = var.emr_core_instance_type
  core_instance_count  = var.emr_core_instance_count
  
  # Auto scaling
  enable_auto_scaling = true
  min_capacity       = var.emr_min_capacity
  max_capacity       = var.emr_max_capacity
  
  # Network configuration
  subnet_id                = module.vpc.private_subnet_ids[0]
  additional_security_groups = [aws_security_group.emr.id]
  
  # S3 configuration
  log_uri = "s3://${module.s3_storage.bucket_names["logs"]}/emr-logs/"
  
  # Bootstrap actions
  bootstrap_actions = [
    {
      name = "Install Python Dependencies"
      path = "s3://${module.s3_storage.bucket_names["processed_data"]}/scripts/bootstrap.sh"
    }
  ]
  
  # Service roles
  service_role = aws_iam_role.emr_service_role.arn
  job_flow_role = aws_iam_instance_profile.emr_instance_profile.arn
  
  tags = local.common_tags
}

# ElastiCache Redis for caching
resource "aws_elasticache_subnet_group" "redis" {
  name       = "${local.name_prefix}-redis-subnet-group"
  subnet_ids = module.vpc.private_subnet_ids
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-redis-subnet-group"
  })
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "${local.name_prefix}-redis"
  description                = "Redis cluster for basemap processing cache"
  
  # Configuration
  node_type                  = var.redis_node_type
  port                       = 6379
  parameter_group_name       = "default.redis7"
  
  # Cluster configuration
  num_cache_clusters         = var.redis_num_nodes
  
  # Network configuration
  subnet_group_name          = aws_elasticache_subnet_group.redis.name
  security_group_ids         = [aws_security_group.redis.id]
  
  # Security
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = var.redis_auth_token
  
  # Backup
  snapshot_retention_limit   = 3
  snapshot_window           = "03:00-05:00"
  
  # Maintenance
  maintenance_window         = "sun:05:00-sun:07:00"
  
  tags = local.common_tags
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "application_logs" {
  name              = "/aws/basemap-processing/application"
  retention_in_days = 30
  
  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "emr_logs" {
  name              = "/aws/emr/basemap-processing"
  retention_in_days = 14
  
  tags = local.common_tags
}

# CloudWatch Alarms
resource "aws_cloudwatch_metric_alarm" "emr_cluster_health" {
  alarm_name          = "${local.name_prefix}-emr-cluster-health"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CoreNodesRunning"
  namespace           = "AWS/ElasticMapReduce"
  period              = "300"
  statistic           = "Average"
  threshold           = var.emr_core_instance_count
  alarm_description   = "This metric monitors EMR cluster health"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    JobFlowId = module.emr_cluster.cluster_id
  }
  
  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu_utilization" {
  alarm_name          = "${local.name_prefix}-rds-cpu-utilization"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "This metric monitors RDS CPU utilization"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    DBInstanceIdentifier = module.rds_postgres.db_instance_id
  }
  
  tags = local.common_tags
}

# SNS Topic for alerts
resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
  
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "email_alerts" {
  count     = length(var.alert_email_addresses)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email_addresses[count.index]
}

# Lambda function for data processing triggers
resource "aws_lambda_function" "data_processor_trigger" {
  filename         = "data_processor_trigger.zip"
  function_name    = "${local.name_prefix}-data-processor-trigger"
  role            = aws_iam_role.lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  runtime         = "python3.9"
  timeout         = 300
  
  environment {
    variables = {
      EMR_CLUSTER_ID = module.emr_cluster.cluster_id
      S3_BUCKET      = module.s3_storage.bucket_names["raw_data"]
      SNS_TOPIC_ARN  = aws_sns_topic.alerts.arn
    }
  }
  
  tags = local.common_tags
}

# S3 Event notification to trigger Lambda
resource "aws_s3_bucket_notification" "data_upload_notification" {
  bucket = module.s3_storage.bucket_names["raw_data"]
  
  lambda_function {
    lambda_function_arn = aws_lambda_function.data_processor_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "osm/"
    filter_suffix       = ".pbf"
  }
  
  depends_on = [aws_lambda_permission.s3_invoke_lambda]
}

resource "aws_lambda_permission" "s3_invoke_lambda" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_processor_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = module.s3_storage.bucket_arns["raw_data"]
}

# Athena for ad-hoc querying
resource "aws_athena_workgroup" "basemap_analytics" {
  name = "${local.name_prefix}-analytics"
  
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics        = true
    bytes_scanned_cutoff_per_query    = 1073741824  # 1GB
    
    result_configuration {
      output_location = "s3://${module.s3_storage.bucket_names["processed_data"]}/athena-results/"
      
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
  
  tags = local.common_tags
}

# Athena database
resource "aws_athena_database" "basemap_data" {
  name   = "basemap_data"
  bucket = module.s3_storage.bucket_names["processed_data"]
  
  encryption_configuration {
    encryption_option = "SSE_S3"
  }
}

# Glue Catalog for data discovery
resource "aws_glue_catalog_database" "basemap_catalog" {
  name        = "basemap_catalog"
  description = "Data catalog for basemap processing pipeline"
  
  target_database {
    catalog_id    = local.account_id
    database_name = aws_athena_database.basemap_data.name
  }
}

# Glue Crawler for automatic schema discovery
resource "aws_glue_crawler" "basemap_crawler" {
  database_name = aws_glue_catalog_database.basemap_catalog.name
  name          = "${local.name_prefix}-crawler"
  role          = aws_iam_role.glue_role.arn
  
  s3_target {
    path = "s3://${module.s3_storage.bucket_names["processed_data"]}/osm/"
  }
  
  s3_target {
    path = "s3://${module.s3_storage.bucket_names["processed_data"]}/poi/"
  }
  
  schedule = "cron(0 6 * * ? *)"  # Daily at 6 AM
  
  tags = local.common_tags
}

# Systems Manager Parameter Store for configuration
resource "aws_ssm_parameter" "db_host" {
  name  = "/${var.project_name}/${var.environment}/db/host"
  type  = "String"
  value = module.rds_postgres.db_instance_endpoint
  
  tags = local.common_tags
}

resource "aws_ssm_parameter" "db_name" {
  name  = "/${var.project_name}/${var.environment}/db/name"
  type  = "String"
  value = var.db_name
  
  tags = local.common_tags
}

resource "aws_ssm_parameter" "redis_endpoint" {
  name  = "/${var.project_name}/${var.environment}/redis/endpoint"
  type  = "String"
  value = aws_elasticache_replication_group.redis.primary_endpoint_address
  
  tags = local.common_tags
}

resource "aws_ssm_parameter" "s3_buckets" {
  name  = "/${var.project_name}/${var.environment}/s3/buckets"
  type  = "String"
  value = jsonencode(module.s3_storage.bucket_names)
  
  tags = local.common_tags
}
