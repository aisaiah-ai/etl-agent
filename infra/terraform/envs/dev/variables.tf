variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "etl-agent"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "VPC ID for network resources"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Redshift and other network resources"
  type        = list(string)
}

variable "redshift_admin_password" {
  description = "Admin password for Redshift Serverless"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email address for alert notifications"
  type        = string
  default     = ""
}

# Redshift JDBC connection for Glue
variable "create_redshift_connection" {
  description = "Whether to create a Glue JDBC connection to Redshift"
  type        = bool
  default     = false
}

variable "connection_availability_zone" {
  description = "Availability zone for the Glue Connection"
  type        = string
  default     = "us-east-1a"
}

# Lambda ARNs — provided after Lambda deployment
variable "discover_lambda_arn" {
  description = "ARN of the Discover Lambda function"
  type        = string
  default     = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-dev-discover"
}

variable "translate_lambda_arn" {
  description = "ARN of the Translate Lambda function"
  type        = string
  default     = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-dev-translate"
}

variable "deploy_lambda_arn" {
  description = "ARN of the Deploy Lambda function"
  type        = string
  default     = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-dev-deploy"
}

variable "verify_lambda_arn" {
  description = "ARN of the Verify Lambda function"
  type        = string
  default     = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-dev-verify"
}
