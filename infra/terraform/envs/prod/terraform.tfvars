aws_region   = "us-east-1"
project_name = "etl-agent"
environment  = "prod"

# Network — replace with your VPC/subnet IDs
vpc_id     = "vpc-xxxxxxxxxxxxxxxxx"
subnet_ids = ["subnet-xxxxxxxxxxxxxxxxx", "subnet-yyyyyyyyyyyyyyyyy", "subnet-zzzzzzzzzzzzzzzzz"]

# Alerting
alert_email = ""

# Lambda ARNs — update after deploying Lambda functions
discover_lambda_arn  = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-prod-discover"
translate_lambda_arn = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-prod-translate"
deploy_lambda_arn    = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-prod-deploy"
verify_lambda_arn    = "arn:aws:lambda:us-east-1:000000000000:function:etl-agent-prod-verify"
