# Security Configuration for Basemap Processing Infrastructure
# Defines security groups, IAM roles, and access policies

# Security Groups

# RDS Security Group
resource "aws_security_group" "rds" {
  name_prefix = "${local.name_prefix}-rds-"
  vpc_id      = module.vpc.vpc_id
  description = "Security group for RDS PostgreSQL instance"

  ingress {
    description = "PostgreSQL from EMR"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  ingress {
    description = "PostgreSQL from Lambda"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  ingress {
    description = "PostgreSQL from Airflow"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnet_cidrs
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-rds-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# EMR Security Group
resource "aws_security_group" "emr" {
  name_prefix = "${local.name_prefix}-emr-"
  vpc_id      = module.vpc.vpc_id
  description = "Security group for EMR cluster"

  # EMR Master node communication
  ingress {
    description = "EMR Master communication"
    from_port   = 8443
    to_port     = 8443
    protocol    = "tcp"
    self        = true
  }

  # Spark communication
  ingress {
    description = "Spark driver communication"
    from_port   = 7077
    to_port     = 7077
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Spark UI"
    from_port   = 4040
    to_port     = 4044
    protocol    = "tcp"
    self        = true
  }

  # Hadoop communication
  ingress {
    description = "Hadoop NameNode"
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Hadoop DataNode"
    from_port   = 50010
    to_port     = 50010
    protocol    = "tcp"
    self        = true
  }

  # YARN communication
  ingress {
    description = "YARN ResourceManager"
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    self        = true
  }

  # SSH access (if needed for debugging)
  ingress {
    description = "SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnet_cidrs
  }

  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-emr-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Redis Security Group
resource "aws_security_group" "redis" {
  name_prefix = "${local.name_prefix}-redis-"
  vpc_id      = module.vpc.vpc_id
  description = "Security group for ElastiCache Redis"

  ingress {
    description = "Redis from EMR"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  ingress {
    description = "Redis from Lambda"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  ingress {
    description = "Redis from Airflow"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnet_cidrs
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-redis-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Lambda Security Group
resource "aws_security_group" "lambda" {
  name_prefix = "${local.name_prefix}-lambda-"
  vpc_id      = module.vpc.vpc_id
  description = "Security group for Lambda functions"

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-lambda-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ALB Security Group (for Airflow UI)
resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  vpc_id      = module.vpc.vpc_id
  description = "Security group for Application Load Balancer"

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-alb-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# IAM Roles and Policies

# EMR Service Role
resource "aws_iam_role" "emr_service_role" {
  name = "${local.name_prefix}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "emr_service_role_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# EMR Instance Profile
resource "aws_iam_role" "emr_instance_role" {
  name = "${local.name_prefix}-emr-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "emr_instance_role_policy" {
  role       = aws_iam_role.emr_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Custom policy for EMR to access S3 buckets
resource "aws_iam_role_policy" "emr_s3_access" {
  name = "${local.name_prefix}-emr-s3-access"
  role = aws_iam_role.emr_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*",
          "arn:aws:s3:::${var.s3_bucket_name}-logs",
          "arn:aws:s3:::${var.s3_bucket_name}-logs/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets"
        ]
        Resource = "*"
      }
    ]
  })
}

# Custom policy for EMR to access RDS
resource "aws_iam_role_policy" "emr_rds_access" {
  name = "${local.name_prefix}-emr-rds-access"
  role = aws_iam_role.emr_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusters"
        ]
        Resource = "*"
      }
    ]
  })
}

# Custom policy for EMR to access Systems Manager
resource "aws_iam_role_policy" "emr_ssm_access" {
  name = "${local.name_prefix}-emr-ssm-access"
  role = aws_iam_role.emr_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath"
        ]
        Resource = "arn:aws:ssm:${local.region}:${local.account_id}:parameter/${var.project_name}/${var.environment}/*"
      }
    ]
  })
}

# Custom policy for EMR to access Secrets Manager
resource "aws_iam_role_policy" "emr_secrets_access" {
  name = "${local.name_prefix}-emr-secrets-access"
  role = aws_iam_role.emr_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.db_password.arn,
          aws_secretsmanager_secret.api_keys.arn
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_instance_profile" {
  name = "${local.name_prefix}-emr-instance-profile"
  role = aws_iam_role.emr_instance_role.name

  tags = local.common_tags
}

# Lambda Execution Role
resource "aws_iam_role" "lambda_role" {
  name = "${local.name_prefix}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Custom policy for Lambda to manage EMR
resource "aws_iam_role_policy" "lambda_emr_access" {
  name = "${local.name_prefix}-lambda-emr-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticmapreduce:AddJobFlowSteps",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:DescribeStep",
          "elasticmapreduce:ListClusters",
          "elasticmapreduce:ListSteps",
          "elasticmapreduce:RunJobFlow",
          "elasticmapreduce:TerminateJobFlows"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.emr_service_role.arn,
          aws_iam_role.emr_instance_role.arn
        ]
      }
    ]
  })
}

# Custom policy for Lambda to access S3
resource "aws_iam_role_policy" "lambda_s3_access" {
  name = "${local.name_prefix}-lambda-s3-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# Custom policy for Lambda to publish to SNS
resource "aws_iam_role_policy" "lambda_sns_access" {
  name = "${local.name_prefix}-lambda-sns-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

# Custom policy for Lambda to access CloudWatch
resource "aws_iam_role_policy" "lambda_cloudwatch_access" {
  name = "${local.name_prefix}-lambda-cloudwatch-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics"
        ]
        Resource = "*"
      }
    ]
  })
}

# Glue Service Role
resource "aws_iam_role" "glue_role" {
  name = "${local.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for Glue to access S3 buckets
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${local.name_prefix}-glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# Airflow Execution Role (for when running on ECS/EC2)
resource "aws_iam_role" "airflow_role" {
  name = "${local.name_prefix}-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["ec2.amazonaws.com", "ecs-tasks.amazonaws.com"]
        }
      }
    ]
  })

  tags = local.common_tags
}

# Custom policy for Airflow to manage EMR
resource "aws_iam_role_policy" "airflow_emr_access" {
  name = "${local.name_prefix}-airflow-emr-access"
  role = aws_iam_role.airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticmapreduce:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.emr_service_role.arn,
          aws_iam_role.emr_instance_role.arn
        ]
      }
    ]
  })
}

# Custom policy for Airflow to access S3
resource "aws_iam_role_policy" "airflow_s3_access" {
  name = "${local.name_prefix}-airflow-s3-access"
  role = aws_iam_role.airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*",
          "arn:aws:s3:::${var.s3_bucket_name}-logs",
          "arn:aws:s3:::${var.s3_bucket_name}-logs/*"
        ]
      }
    ]
  })
}

# Custom policy for Airflow to access RDS
resource "aws_iam_role_policy" "airflow_rds_access" {
  name = "${local.name_prefix}-airflow-rds-access"
  role = aws_iam_role.airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusters"
        ]
        Resource = "*"
      }
    ]
  })
}

# Custom policy for Airflow to access Systems Manager
resource "aws_iam_role_policy" "airflow_ssm_access" {
  name = "${local.name_prefix}-airflow-ssm-access"
  role = aws_iam_role.airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath"
        ]
        Resource = "arn:aws:ssm:${local.region}:${local.account_id}:parameter/${var.project_name}/${var.environment}/*"
      }
    ]
  })
}

# Custom policy for Airflow to publish to SNS
resource "aws_iam_role_policy" "airflow_sns_access" {
  name = "${local.name_prefix}-airflow-sns-access"
  role = aws_iam_role.airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

# Custom policy for Airflow to access Secrets Manager
resource "aws_iam_role_policy" "airflow_secrets_access" {
  name = "${local.name_prefix}-airflow-secrets-access"
  role = aws_iam_role.airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.db_password.arn,
          aws_secretsmanager_secret.api_keys.arn
        ]
      }
    ]
  })
}

# KMS Key for encryption
resource "aws_kms_key" "basemap_key" {
  description             = "KMS key for basemap processing encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for EMR"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.emr_instance_role.arn
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for Lambda"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.lambda_role.arn
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for RDS"
        Effect = "Allow"
        Principal = {
          Service = "rds.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-kms-key"
  })
}

resource "aws_kms_alias" "basemap_key_alias" {
  name          = "alias/${local.name_prefix}-key"
  target_key_id = aws_kms_key.basemap_key.key_id
}

# Secrets Manager for sensitive configuration
resource "aws_secretsmanager_secret" "db_password" {
  name                    = "${local.name_prefix}-db-password"
  description             = "Database password for basemap processing"
  recovery_window_in_days = 7
  kms_key_id             = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id = aws_secretsmanager_secret.db_password.id
  secret_string = jsonencode({
    username = var.db_username
    password = random_password.db_password.result
  })
}

resource "random_password" "db_password" {
  length  = 32
  special = true
}

# API Keys secret
resource "aws_secretsmanager_secret" "api_keys" {
  name                    = "${local.name_prefix}-api-keys"
  description             = "API keys for external services"
  recovery_window_in_days = 7
  kms_key_id             = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "api_keys" {
  secret_id = aws_secretsmanager_secret.api_keys.id
  secret_string = jsonencode({
    slack_webhook_url = var.slack_webhook_url
    mapbox_api_key   = var.mapbox_api_key
    osm_api_key      = var.osm_api_key
  })
}

# WAF for API protection
resource "aws_wafv2_web_acl" "basemap_waf" {
  name  = "${local.name_prefix}-waf"
  scope = "REGIONAL"

  default_action {
    allow {}
  }

  rule {
    name     = "RateLimitRule"
    priority = 1

    action {
      block {}
    }

    statement {
      rate_based_statement {
        limit              = 2000
        aggregate_key_type = "IP"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "RateLimitRule"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "AWSManagedRulesCommonRuleSet"
    priority = 2

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "CommonRuleSetMetric"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "AWSManagedRulesKnownBadInputsRuleSet"
    priority = 3

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesKnownBadInputsRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "KnownBadInputsRuleSetMetric"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "IPWhitelistRule"
    priority = 4

    action {
      allow {}
    }

    statement {
      ip_set_reference_statement {
        arn = aws_wafv2_ip_set.whitelist.arn
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "IPWhitelistRule"
      sampled_requests_enabled   = true
    }
  }

  tags = local.common_tags

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${local.name_prefix}-waf"
    sampled_requests_enabled   = true
  }
}

# IP Whitelist for WAF
resource "aws_wafv2_ip_set" "whitelist" {
  name               = "${local.name_prefix}-ip-whitelist"
  description        = "IP whitelist for basemap processing"
  scope              = "REGIONAL"
  ip_address_version = "IPV4"

  addresses = var.allowed_ip_addresses

  tags = local.common_tags
}

# CloudTrail for audit logging
resource "aws_cloudtrail" "basemap_trail" {
  name                          = "${local.name_prefix}-cloudtrail"
  s3_bucket_name               = aws_s3_bucket.cloudtrail_logs.bucket
  s3_key_prefix                = "cloudtrail-logs"
  include_global_service_events = true
  is_multi_region_trail        = true
  enable_logging               = true
  kms_key_id                   = aws_kms_key.basemap_key.arn

  event_selector {
    read_write_type                 = "All"
    include_management_events       = true
    exclude_management_event_sources = []

    data_resource {
      type   = "AWS::S3::Object"
      values = ["arn:aws:s3:::${var.s3_bucket_name}/*"]
    }

    data_resource {
      type   = "AWS::S3::Bucket"
      values = ["arn:aws:s3:::${var.s3_bucket_name}"]
    }
  }

  tags = local.common_tags
}

# S3 bucket for CloudTrail logs
resource "aws_s3_bucket" "cloudtrail_logs" {
  bucket        = "${var.s3_bucket_name}-cloudtrail-logs"
  force_destroy = true

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "cloudtrail_logs_versioning" {
  bucket = aws_s3_bucket.cloudtrail_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "cloudtrail_logs_encryption" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.basemap_key.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }
}

resource "aws_s3_bucket_public_access_block" "cloudtrail_logs_pab" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CloudTrail bucket policy
resource "aws_s3_bucket_policy" "cloudtrail_logs_policy" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AWSCloudTrailAclCheck"
        Effect = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action   = "s3:GetBucketAcl"
        Resource = aws_s3_bucket.cloudtrail_logs.arn
      },
      {
        Sid    = "AWSCloudTrailWrite"
        Effect = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.cloudtrail_logs.arn}/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      }
    ]
  })
}

# SNS Topic for security alerts
resource "aws_sns_topic" "security_alerts" {
  name              = "${local.name_prefix}-security-alerts"
  kms_master_key_id = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "security_alerts_email" {
  topic_arn = aws_sns_topic.security_alerts.arn
  protocol  = "email"
  endpoint  = var.security_alert_email
}

# CloudWatch Log Groups with encryption
resource "aws_cloudwatch_log_group" "emr_logs" {
  name              = "/aws/emr/${local.name_prefix}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${local.name_prefix}"
  retention_in_days = 14
  kms_key_id        = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "airflow_logs" {
  name              = "/aws/airflow/${local.name_prefix}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

# VPC Flow Logs
resource "aws_flow_log" "vpc_flow_log" {
  iam_role_arn    = aws_iam_role.flow_log_role.arn
  log_destination = aws_cloudwatch_log_group.vpc_flow_logs.arn
  traffic_type    = "ALL"
  vpc_id          = module.vpc.vpc_id

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "vpc_flow_logs" {
  name              = "/aws/vpc/flowlogs/${local.name_prefix}"
  retention_in_days = 14
  kms_key_id        = aws_kms_key.basemap_key.arn

  tags = local.common_tags
}

resource "aws_iam_role" "flow_log_role" {
  name = "${local.name_prefix}-flow-log-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "vpc-flow-logs.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "flow_log_policy" {
  name = "${local.name_prefix}-flow-log-policy"
  role = aws_iam_role.flow_log_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      }
    ]
  })
}

# Output security group IDs and role ARNs
output "security_group_ids" {
  description = "Security group IDs for different services"
  value = {
    rds    = aws_security_group.rds.id
    emr    = aws_security_group.emr.id
    redis  = aws_security_group.redis.id
    lambda = aws_security_group.lambda.id
    alb    = aws_security_group.alb.id
  }
}

output "iam_role_arns" {
  description = "IAM role ARNs for different services"
  value = {
    emr_service_role     = aws_iam_role.emr_service_role.arn
    emr_instance_role    = aws_iam_role.emr_instance_role.arn
    emr_instance_profile = aws_iam_instance_profile.emr_instance_profile.arn
    lambda_role          = aws_iam_role.lambda_role.arn
    glue_role           = aws_iam_role.glue_role.arn
    airflow_role        = aws_iam_role.airflow_role.arn
  }
}

output "kms_key_id" {
  description = "KMS key ID for encryption"
  value       = aws_kms_key.basemap_key.key_id
}

output "kms_key_arn" {
  description = "KMS key ARN for encryption"
  value       = aws_kms_key.basemap_key.arn
}

output "secrets_manager_arns" {
  description = "Secrets Manager secret ARNs"
  value = {
    db_password = aws_secretsmanager_secret.db_password.arn
    api_keys    = aws_secretsmanager_secret.api_keys.arn
  }
}

output "waf_web_acl_arn" {
  description = "WAF Web ACL ARN"
  value       = aws_wafv2_web_acl.basemap_waf.arn
}

output "cloudtrail_arn" {
  description = "CloudTrail ARN"
  value       = aws_cloudtrail.basemap_trail.arn
}

