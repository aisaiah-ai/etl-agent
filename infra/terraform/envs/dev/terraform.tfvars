aws_region   = "us-east-1"
project_name = "etl-agent"
environment  = "dev"

# Network — ies-dev VPC with RDS private subnets (multi-AZ)
vpc_id = "vpc-0e711aa7f465f0692"
subnet_ids = [
  "subnet-06d2edfa1115308ee", # ies-dev-rds-private-us-east-1a
  "subnet-03e98e5d190e3e93b", # ies-dev-rds-private-us-east-1b
  "subnet-0e2621ff4238c5d41", # ies-dev-rds-private-us-east-1c
]

# Alerting
alert_email = "adajao@iesabroad.org"

# Lambda ARNs — update after deploying Lambda functions
discover_lambda_arn  = "arn:aws:lambda:us-east-1:533267300255:function:etl-agent-dev-discover"
translate_lambda_arn = "arn:aws:lambda:us-east-1:533267300255:function:etl-agent-dev-translate"
deploy_lambda_arn    = "arn:aws:lambda:us-east-1:533267300255:function:etl-agent-dev-deploy"
verify_lambda_arn    = "arn:aws:lambda:us-east-1:533267300255:function:etl-agent-dev-verify"
