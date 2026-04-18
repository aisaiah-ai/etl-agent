variable "job_name" {
  description = "Name of the Glue job"
  type        = string
}

variable "script_location" {
  description = "S3 path to the Glue job script"
  type        = string
}

variable "role_arn" {
  description = "IAM role ARN for the Glue job"
  type        = string
}

variable "worker_type" {
  description = "Glue worker type (G.1X, G.2X, G.4X, G.8X, G.025X)"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"
}

variable "timeout" {
  description = "Job timeout in minutes"
  type        = number
  default     = 120
}

variable "max_retries" {
  description = "Maximum number of retries on failure"
  type        = number
  default     = 1
}

variable "max_concurrent_runs" {
  description = "Maximum concurrent runs of the job"
  type        = number
  default     = 1
}

variable "artifacts_bucket" {
  description = "S3 bucket name for temporary files and Spark UI logs"
  type        = string
  default     = ""
}

variable "extra_arguments" {
  description = "Additional default arguments for the Glue job"
  type        = map(string)
  default     = {}
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

# -- Redshift JDBC Connection --------------------------------------------------

variable "create_redshift_connection" {
  description = "Whether to create a Glue Connection for Redshift JDBC"
  type        = bool
  default     = false
}

variable "connection_name" {
  description = "Name of the Glue Connection for Redshift"
  type        = string
  default     = "etl-agent-redshift"
}

variable "redshift_jdbc_url" {
  description = "JDBC URL for the Redshift cluster/workgroup (e.g. jdbc:redshift://host:5439/db)"
  type        = string
  default     = ""
}

variable "redshift_username" {
  description = "Username for the Redshift JDBC connection"
  type        = string
  default     = ""
}

variable "redshift_password" {
  description = "Password for the Redshift JDBC connection"
  type        = string
  default     = ""
  sensitive   = true
}

variable "connection_availability_zone" {
  description = "Availability zone for the Glue Connection"
  type        = string
  default     = ""
}

variable "connection_security_group_ids" {
  description = "Security group IDs for the Glue Connection VPC endpoint"
  type        = list(string)
  default     = []
}

variable "connection_subnet_id" {
  description = "Subnet ID for the Glue Connection VPC endpoint"
  type        = string
  default     = ""
}
