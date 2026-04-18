variable "database_name" {
  description = "Name of the Glue catalog database"
  type        = string
}

variable "description" {
  description = "Description of the Glue catalog database"
  type        = string
  default     = "ETL Agent managed Glue catalog database"
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
