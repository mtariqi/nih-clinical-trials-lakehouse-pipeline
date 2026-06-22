variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "redshift_username" {
  description = "Redshift master username"
  type        = string
  sensitive   = true
}

variable "redshift_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_node_count" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 1
}
