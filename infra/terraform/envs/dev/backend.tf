# Using local backend for initial bootstrap.
# Switch to S3 backend after state bucket is created:
#   bucket         = "etl-agent-terraform-state-dev"
#   key            = "dev/terraform.tfstate"
#   region         = "us-east-1"
#   dynamodb_table = "etl-agent-terraform-locks-dev"
#   encrypt        = true
terraform {
  backend "local" {}
}
