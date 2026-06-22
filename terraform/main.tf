terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Buckets (medallion layers) ─────────────────────────────────────────────

locals {
  layers = ["raw", "bronze", "silver", "gold"]
}

resource "aws_s3_bucket" "nih_trials" {
  for_each = toset(local.layers)
  bucket   = "nih-trials-${each.key}-${var.environment}"

  tags = {
    Project     = "nih-clinical-trials-pipeline"
    Layer       = each.key
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "nih_trials" {
  for_each = aws_s3_bucket.nih_trials
  bucket   = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "nih_trials" {
  for_each = aws_s3_bucket.nih_trials
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.nih_trials.arn
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.nih_trials["raw"].id

  rule {
    id     = "archive-raw-after-90-days"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ── KMS Key ───────────────────────────────────────────────────────────────────

resource "aws_kms_key" "nih_trials" {
  description             = "KMS key for NIH Clinical Trials pipeline encryption"
  deletion_window_in_days = 10

  tags = {
    Project = "nih-clinical-trials-pipeline"
  }
}

resource "aws_kms_alias" "nih_trials" {
  name          = "alias/nih-trials-${var.environment}"
  target_key_id = aws_kms_key.nih_trials.key_id
}

# ── AWS Glue ──────────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "nih_trials" {
  name        = "nih_trials_${var.environment}"
  description = "Glue catalog for NIH Clinical Trials lakehouse layers"
}

resource "aws_iam_role" "glue_role" {
  name = "nih-trials-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        for bucket in aws_s3_bucket.nih_trials : "${bucket.arn}/*"
      ]
    }]
  })
}

# ── Amazon Redshift ───────────────────────────────────────────────────────────

resource "aws_redshift_cluster" "nih_trials" {
  cluster_identifier        = "nih-trials-${var.environment}"
  database_name             = "nih_trials"
  master_username           = var.redshift_username
  master_password           = var.redshift_password
  node_type                 = var.redshift_node_type
  number_of_nodes           = var.redshift_node_count
  cluster_type              = var.redshift_node_count > 1 ? "multi-node" : "single-node"
  encrypted                 = true
  kms_key_id                = aws_kms_key.nih_trials.arn
  skip_final_snapshot       = var.environment != "prod"
  final_snapshot_identifier = "nih-trials-final-${var.environment}"

  tags = {
    Project     = "nih-clinical-trials-pipeline"
    Environment = var.environment
  }
}
