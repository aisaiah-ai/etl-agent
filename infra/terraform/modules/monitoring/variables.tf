variable "project_name" {
  description = "Project name"
  type        = string
  default     = "etl-agent"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "alert_email" {
  description = "Email address for alert notifications (empty to skip subscription)"
  type        = string
  default     = ""
}

variable "glue_job_name" {
  description = "Glue job name for the failure alarm"
  type        = string
  default     = ""
}

variable "state_machine_arn" {
  description = "Step Functions state machine ARN for alarms"
  type        = string
  default     = ""
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name for alarms"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
