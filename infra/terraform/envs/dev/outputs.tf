# S3
output "data_bucket_name" {
  description = "Data lake S3 bucket name"
  value       = module.s3_lake.data_bucket_name
}

output "artifacts_bucket_name" {
  description = "Artifacts S3 bucket name"
  value       = module.s3_lake.artifacts_bucket_name
}

# KMS
output "kms_key_arn" {
  description = "KMS key ARN"
  value       = module.kms.key_arn
}

# IAM
output "glue_execution_role_arn" {
  description = "Glue job execution role ARN"
  value       = module.iam.glue_execution_role_arn
}

output "discovery_agent_role_arn" {
  description = "Discovery agent role ARN"
  value       = module.iam.discovery_agent_role_arn
}

output "step_functions_role_arn" {
  description = "Step Functions execution role ARN"
  value       = module.iam.step_functions_role_arn
}

# Glue
output "glue_catalog_database" {
  description = "Glue catalog database name"
  value       = module.glue_catalog.database_name
}

output "glue_job_transform_name" {
  description = "Glue transform job name"
  value       = module.glue_job_transform.job_name
}

# Redshift
output "redshift_namespace_id" {
  description = "Redshift Serverless namespace ID"
  value       = module.redshift.namespace_id
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = module.redshift.workgroup_endpoint
}

output "redshift_role_arn" {
  description = "Redshift IAM role ARN"
  value       = module.redshift.role_arn
}

# Step Functions
output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = module.step_functions.state_machine_arn
}

# Monitoring
output "sns_topic_arn" {
  description = "Alerts SNS topic ARN"
  value       = module.monitoring.sns_topic_arn
}
