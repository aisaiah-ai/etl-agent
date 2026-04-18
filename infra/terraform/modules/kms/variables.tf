variable "alias_name" {
  description = "KMS key alias name (without 'alias/' prefix)"
  type        = string
}

variable "description" {
  description = "Description of the KMS key"
  type        = string
  default     = "ETL Agent encryption key"
}

variable "deletion_window_in_days" {
  description = "Waiting period before KMS key deletion"
  type        = number
  default     = 14
}

variable "enable_key_rotation" {
  description = "Enable automatic key rotation"
  type        = bool
  default     = true
}

variable "key_administrators" {
  description = "List of IAM ARNs that can administer the key"
  type        = list(string)
  default     = []
}

variable "key_users" {
  description = "List of IAM ARNs that can use the key for encrypt/decrypt"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
