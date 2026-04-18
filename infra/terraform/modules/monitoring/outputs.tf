output "sns_topic_arn" {
  description = "ARN of the alerts SNS topic"
  value       = aws_sns_topic.alerts.arn
}

output "etl_pipeline_log_group" {
  description = "Name of the ETL pipeline CloudWatch log group"
  value       = aws_cloudwatch_log_group.etl_pipeline.name
}

output "discovery_agent_log_group" {
  description = "Name of the discovery agent CloudWatch log group"
  value       = aws_cloudwatch_log_group.discovery_agent.name
}
