output "namespace_id" {
  description = "ID of the Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.this.id
}

output "workgroup_endpoint" {
  description = "Endpoint of the Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.this.endpoint
}

output "role_arn" {
  description = "ARN of the Redshift IAM role"
  value       = aws_iam_role.redshift.arn
}

output "security_group_id" {
  description = "ID of the Redshift security group"
  value       = local.security_group_id
}
