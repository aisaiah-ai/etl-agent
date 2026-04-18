terraform {
  backend "s3" {
    bucket         = "etl-agent-terraform-state-prod"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "etl-agent-terraform-locks-prod"
    encrypt        = true
  }
}
