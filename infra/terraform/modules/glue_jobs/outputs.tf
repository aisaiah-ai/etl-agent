output "job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.this.name
}

output "job_arn" {
  description = "ARN of the Glue job"
  value       = aws_glue_job.this.arn
}

output "connection_name" {
  description = "Name of the Glue Redshift connection (if created)"
  value       = var.create_redshift_connection ? aws_glue_connection.redshift[0].name : ""
}
