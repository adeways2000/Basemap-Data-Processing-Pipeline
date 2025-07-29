# Terraform Variables for Basemap Processing Infrastructure
# Defines all configurable parameters for the AWS infrastructure

# General Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "basemap-processing"
  
  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "Project name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Environment name (development, staging, production)"
  type        = string
  default     = "development"
  
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be one of: development, staging, production."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
  
  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.aws_region))
    error_message = "AWS region must be a valid region identifier."
  }
}

# Network Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
  
  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "VPC CIDR must be a valid IPv4 CIDR block."
  }
}

# RDS Configuration
variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
  
  validation {
    condition = contains([
      "db.t3.micro", "db.t3.small", "db.t3.medium", "db.t3.large",
      "db.r5.large", "db.r5.xlarge", "db.r5.2xlarge", "db.r5.4xlarge"
    ], var.rds_instance_class)
    error_message = "RDS instance class must be a valid PostgreSQL-supported instance type."
  }
}

variable "rds_allocated_storage" {
  description = "Initial allocated storage for RDS instance (GB)"
  type        = number
  default     = 100
  
  validation {
    condition     = var.rds_allocated_storage >= 20 && var.rds_allocated_storage <= 65536
    error_message = "RDS allocated storage must be between 20 and 65536 GB."
  }
}

variable "rds_max_allocated_storage" {
  description = "Maximum allocated storage for RDS auto-scaling (GB)"
  type        = number
  default     = 1000
  
  validation {
    condition     = var.rds_max_allocated_storage >= var.rds_allocated_storage
    error_message = "RDS max allocated storage must be greater than or equal to allocated storage."
  }
}

variable "db_name" {
  description = "Name of the PostgreSQL database"
  type        = string
  default     = "basemap_db"
  
  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]*$", var.db_name))
    error_message = "Database name must start with a letter and contain only letters, numbers, and underscores."
  }
}

variable "db_username" {
  description = "Username for the PostgreSQL database"
  type        = string
  default     = "basemap_user"
  
  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]*$", var.db_username))
    error_message = "Database username must start with a letter and contain only letters, numbers, and underscores."
  }
}

# EMR Configuration
variable "emr_master_instance_type" {
  description = "EMR master node instance type"
  type        = string
  default     = "m5.xlarge"
  
  validation {
    condition = contains([
      "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
      "m5.8xlarge", "m5.12xlarge", "m5.16xlarge", "m5.24xlarge",
      "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
      "r5.large", "r5.xlarge", "r5.2xlarge", "r5.4xlarge"
    ], var.emr_master_instance_type)
    error_message = "EMR master instance type must be a valid EC2 instance type suitable for EMR."
  }
}

variable "emr_core_instance_type" {
  description = "EMR core node instance type"
  type        = string
  default     = "m5.xlarge"
  
  validation {
    condition = contains([
      "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
      "m5.8xlarge", "m5.12xlarge", "m5.16xlarge", "m5.24xlarge",
      "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
      "r5.large", "r5.xlarge", "r5.2xlarge", "r5.4xlarge"
    ], var.emr_core_instance_type)
    error_message = "EMR core instance type must be a valid EC2 instance type suitable for EMR."
  }
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 3
  
  validation {
    condition     = var.emr_core_instance_count >= 1 && var.emr_core_instance_count <= 50
    error_message = "EMR core instance count must be between 1 and 50."
  }
}

variable "emr_min_capacity" {
  description = "Minimum capacity for EMR auto scaling"
  type        = number
  default     = 1
  
  validation {
    condition     = var.emr_min_capacity >= 1 && var.emr_min_capacity <= var.emr_core_instance_count
    error_message = "EMR minimum capacity must be at least 1 and not exceed core instance count."
  }
}

variable "emr_max_capacity" {
  description = "Maximum capacity for EMR auto scaling"
  type        = number
  default     = 10
  
  validation {
    condition     = var.emr_max_capacity >= var.emr_core_instance_count && var.emr_max_capacity <= 100
    error_message = "EMR maximum capacity must be at least equal to core instance count and not exceed 100."
  }
}

# Redis Configuration
variable "redis_node_type" {
  description = "ElastiCache Redis node type"
  type        = string
  default     = "cache.t3.medium"
  
  validation {
    condition = contains([
      "cache.t3.micro", "cache.t3.small", "cache.t3.medium",
      "cache.m5.large", "cache.m5.xlarge", "cache.m5.2xlarge",
      "cache.r5.large", "cache.r5.xlarge", "cache.r5.2xlarge"
    ], var.redis_node_type)
    error_message = "Redis node type must be a valid ElastiCache node type."
  }
}

variable "redis_num_nodes" {
  description = "Number of Redis cache nodes"
  type        = number
  default     = 2
  
  validation {
    condition     = var.redis_num_nodes >= 1 && var.redis_num_nodes <= 6
    error_message = "Redis number of nodes must be between 1 and 6."
  }
}

variable "redis_auth_token" {
  description = "Auth token for Redis cluster"
  type        = string
  default     = ""
  sensitive   = true
  
  validation {
    condition     = var.redis_auth_token == "" || length(var.redis_auth_token) >= 16
    error_message = "Redis auth token must be at least 16 characters long if provided."
  }
}

# Monitoring and Alerting
variable "alert_email_addresses" {
  description = "List of email addresses for CloudWatch alerts"
  type        = list(string)
  default     = []
  
  validation {
    condition = alltrue([
      for email in var.alert_email_addresses : can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email))
    ])
    error_message = "All email addresses must be valid email format."
  }
}

variable "enable_detailed_monitoring" {
  description = "Enable detailed CloudWatch monitoring"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 30
  
  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653
    ], var.log_retention_days)
    error_message = "Log retention days must be a valid CloudWatch Logs retention value."
  }
}

# Cost Optimization
variable "enable_spot_instances" {
  description = "Enable spot instances for EMR core nodes"
  type        = bool
  default     = true
}

variable "spot_instance_bid_price" {
  description = "Maximum bid price for spot instances (USD per hour)"
  type        = string
  default     = "0.10"
  
  validation {
    condition     = can(tonumber(var.spot_instance_bid_price)) && tonumber(var.spot_instance_bid_price) > 0
    error_message = "Spot instance bid price must be a positive number."
  }
}

variable "enable_s3_lifecycle_policies" {
  description = "Enable S3 lifecycle policies for cost optimization"
  type        = bool
  default     = true
}

# Security Configuration
variable "enable_encryption_at_rest" {
  description = "Enable encryption at rest for all storage services"
  type        = bool
  default     = true
}

variable "enable_encryption_in_transit" {
  description = "Enable encryption in transit for all services"
  type        = bool
  default     = true
}

variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed to access resources"
  type        = list(string)
  default     = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
  
  validation {
    condition = alltrue([
      for cidr in var.allowed_cidr_blocks : can(cidrhost(cidr, 0))
    ])
    error_message = "All CIDR blocks must be valid IPv4 CIDR notation."
  }
}

variable "enable_vpc_flow_logs" {
  description = "Enable VPC Flow Logs for network monitoring"
  type        = bool
  default     = true
}

# Performance Configuration
variable "enable_performance_insights" {
  description = "Enable Performance Insights for RDS"
  type        = bool
  default     = true
}

variable "performance_insights_retention_period" {
  description = "Performance Insights retention period in days"
  type        = number
  default     = 7
  
  validation {
    condition     = contains([7, 731], var.performance_insights_retention_period)
    error_message = "Performance Insights retention period must be 7 or 731 days."
  }
}

# Backup Configuration
variable "rds_backup_retention_period" {
  description = "Number of days to retain RDS backups"
  type        = number
  default     = 7
  
  validation {
    condition     = var.rds_backup_retention_period >= 0 && var.rds_backup_retention_period <= 35
    error_message = "RDS backup retention period must be between 0 and 35 days."
  }
}

variable "rds_backup_window" {
  description = "Preferred backup window for RDS (UTC)"
  type        = string
  default     = "03:00-04:00"
  
  validation {
    condition     = can(regex("^([0-1][0-9]|2[0-3]):[0-5][0-9]-([0-1][0-9]|2[0-3]):[0-5][0-9]$", var.rds_backup_window))
    error_message = "RDS backup window must be in format HH:MM-HH:MM (24-hour UTC)."
  }
}

variable "rds_maintenance_window" {
  description = "Preferred maintenance window for RDS (UTC)"
  type        = string
  default     = "sun:04:00-sun:05:00"
  
  validation {
    condition = can(regex("^(mon|tue|wed|thu|fri|sat|sun):[0-2][0-9]:[0-5][0-9]-(mon|tue|wed|thu|fri|sat|sun):[0-2][0-9]:[0-5][0-9]$", var.rds_maintenance_window))
    error_message = "RDS maintenance window must be in format ddd:HH:MM-ddd:HH:MM."
  }
}

# Data Processing Configuration
variable "spark_executor_memory" {
  description = "Memory allocation for Spark executors"
  type        = string
  default     = "4g"
  
  validation {
    condition     = can(regex("^[0-9]+[gm]$", var.spark_executor_memory))
    error_message = "Spark executor memory must be in format like '4g' or '2048m'."
  }
}

variable "spark_executor_cores" {
  description = "Number of cores for Spark executors"
  type        = number
  default     = 2
  
  validation {
    condition     = var.spark_executor_cores >= 1 && var.spark_executor_cores <= 8
    error_message = "Spark executor cores must be between 1 and 8."
  }
}

variable "spark_driver_memory" {
  description = "Memory allocation for Spark driver"
  type        = string
  default     = "2g"
  
  validation {
    condition     = can(regex("^[0-9]+[gm]$", var.spark_driver_memory))
    error_message = "Spark driver memory must be in format like '2g' or '1024m'."
  }
}

# Feature Flags
variable "enable_athena_workgroup" {
  description = "Enable Athena workgroup for analytics"
  type        = bool
  default     = true
}

variable "enable_glue_crawler" {
  description = "Enable Glue crawler for data catalog"
  type        = bool
  default     = true
}

variable "enable_lambda_triggers" {
  description = "Enable Lambda functions for data processing triggers"
  type        = bool
  default     = true
}

variable "enable_auto_scaling" {
  description = "Enable auto scaling for EMR clusters"
  type        = bool
  default     = true
}

# Environment-specific overrides
variable "environment_config" {
  description = "Environment-specific configuration overrides"
  type = object({
    enable_multi_az           = optional(bool, false)
    enable_deletion_protection = optional(bool, false)
    enable_termination_protection = optional(bool, false)
    backup_retention_days     = optional(number, 7)
    monitoring_interval       = optional(number, 60)
  })
  default = {}
}

# Tags
variable "additional_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
  
  validation {
    condition = alltrue([
      for k, v in var.additional_tags : can(regex("^[a-zA-Z0-9+\\-=._:/@]+$", k)) && can(regex("^[a-zA-Z0-9+\\-=._:/@\\s]*$", v))
    ])
    error_message = "Tag keys and values must contain only valid characters."
  }
}
