variable "state_machine_name" {
  description = "Name of the Step Functions state machine"
  type        = string
}

variable "discover_lambda_arn" {
  description = "ARN of the Discover Lambda function"
  type        = string
}

variable "translate_lambda_arn" {
  description = "ARN of the Translate Lambda function"
  type        = string
}

variable "deploy_lambda_arn" {
  description = "ARN of the Deploy Lambda function"
  type        = string
}

variable "verify_lambda_arn" {
  description = "ARN of the Verify Lambda function"
  type        = string
}

variable "lambda_arns" {
  description = "List of all Lambda ARNs invoked by the state machine"
  type        = list(string)
}

variable "dlq_sns_topic_arn" {
  description = "SNS topic ARN for dead-letter / error notifications"
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "log_level" {
  description = "Step Functions logging level (OFF, ALL, ERROR, FATAL)"
  type        = string
  default     = "ERROR"
}

variable "enable_xray_tracing" {
  description = "Enable X-Ray tracing for the state machine"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
