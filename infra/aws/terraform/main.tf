terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {}

resource "aws_kms_key" "platform" {
  description             = "KMS key for the cloud data platform"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_s3_bucket" "lakehouse" {
  bucket_prefix = "${var.name_prefix}-lakehouse-"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_db_subnet_group" "platform" {
  name       = "${var.name_prefix}-db"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "platform_internal" {
  name        = "${var.name_prefix}-internal"
  description = "Internal platform service traffic"
  vpc_id      = var.vpc_id

  ingress {
    description = "Internal ingress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "postgres" {
  identifier             = "${var.name_prefix}-postgres"
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = "db.t4g.medium"
  allocated_storage      = 100
  storage_encrypted      = true
  kms_key_id             = aws_kms_key.platform.arn
  db_name                = "data_platform"
  username               = var.db_username
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.platform.name
  vpc_security_group_ids = [aws_security_group.platform_internal.id]
  skip_final_snapshot    = true
}

resource "aws_elasticache_subnet_group" "platform" {
  name       = "${var.name_prefix}-redis"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "${var.name_prefix}-redis"
  description                = "Redis cache for analytics APIs"
  engine                     = "redis"
  node_type                  = "cache.t4g.small"
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  subnet_group_name          = aws_elasticache_subnet_group.platform.name
  security_group_ids         = [aws_security_group.platform_internal.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
}

resource "aws_msk_cluster" "kafka" {
  cluster_name           = "${var.name_prefix}-msk"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m7g.large"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.platform_internal.id]

    storage_info {
      ebs_storage_info {
        volume_size = 250
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.platform.arn
  }
}

resource "aws_ecs_cluster" "platform" {
  name = "${var.name_prefix}-ecs"
}

resource "aws_cloudwatch_log_group" "services" {
  name              = "/aws/ecs/${var.name_prefix}"
  retention_in_days = 30
}

