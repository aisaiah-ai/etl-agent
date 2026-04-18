variable "project_name" {
  description = "Project name used in resource naming"
  type        = string
  default     = "etl-agent"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "s3_bucket_arns" {
  description = "List of S3 bucket ARNs the roles can access"
  type        = list(string)
  default     = []
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption operations"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
