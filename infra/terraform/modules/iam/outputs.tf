output "glue_execution_role_arn" {
  description = "ARN of the Glue job execution role"
  value       = aws_iam_role.glue_execution.arn
}

output "glue_execution_role_name" {
  description = "Name of the Glue job execution role"
  value       = aws_iam_role.glue_execution.name
}

output "step_functions_role_arn" {
  description = "ARN of the Step Functions execution role"
  value       = aws_iam_role.step_functions_execution.arn
}

output "discovery_agent_role_arn" {
  description = "ARN of the discovery agent role"
  value       = aws_iam_role.discovery_agent.arn
}

output "discovery_agent_role_name" {
  description = "Name of the discovery agent role"
  value       = aws_iam_role.discovery_agent.name
}
