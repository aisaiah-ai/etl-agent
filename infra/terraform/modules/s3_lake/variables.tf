variable "data_bucket_name" {
  description = "Name of the data lake S3 bucket"
  type        = string
}

variable "artifacts_bucket_name" {
  description = "Name of the artifacts S3 bucket"
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for bucket encryption (empty string for SSE-S3)"
  type        = string
  default     = ""
}

variable "force_destroy" {
  description = "Allow destroying non-empty buckets"
  type        = bool
  default     = false
}

variable "data_ia_transition_days" {
  description = "Days before transitioning data to Standard-IA"
  type        = number
  default     = 90
}

variable "data_glacier_transition_days" {
  description = "Days before transitioning data to Glacier"
  type        = number
  default     = 365
}

variable "data_expiration_days" {
  description = "Days before data expires"
  type        = number
  default     = 730
}

variable "temp_expiration_days" {
  description = "Days before temp files expire in artifacts bucket"
  type        = number
  default     = 7
}

variable "noncurrent_version_expiration_days" {
  description = "Days before noncurrent versions expire"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
