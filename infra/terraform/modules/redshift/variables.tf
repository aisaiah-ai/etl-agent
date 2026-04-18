variable "namespace_name" {
  description = "Redshift Serverless namespace name"
  type        = string
}

variable "workgroup_name" {
  description = "Redshift Serverless workgroup name"
  type        = string
}

variable "database_name" {
  description = "Default database name"
  type        = string
  default     = "etl_agent_db"
}

variable "admin_username" {
  description = "Admin username for the Redshift namespace"
  type        = string
  default     = "admin"
}

variable "admin_user_password" {
  description = "Admin user password for the Redshift namespace"
  type        = string
  sensitive   = true
}

variable "base_capacity" {
  description = "Base RPU capacity for the workgroup"
  type        = number
  default     = 8
}

variable "max_capacity" {
  description = "Maximum RPU capacity for the workgroup"
  type        = number
  default     = 32
}

variable "subnet_ids" {
  description = "Subnet IDs for the workgroup"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID for the security group"
  type        = string
}

variable "data_bucket_name" {
  description = "S3 data bucket name for Redshift access"
  type        = string
  default     = ""
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to connect to Redshift"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "publicly_accessible" {
  description = "Whether the workgroup is publicly accessible"
  type        = bool
  default     = false
}

variable "existing_security_group_id" {
  description = "Use an existing security group instead of creating one. Leave empty to create a new SG."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
