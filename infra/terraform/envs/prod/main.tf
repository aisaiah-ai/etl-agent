locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ------------------------------------------------------------------------------
# KMS
# ------------------------------------------------------------------------------
module "kms" {
  source = "../../modules/kms"

  alias_name         = "${var.project_name}-${var.environment}"
  description        = "ETL Agent encryption key (${var.environment})"
  key_administrators = []
  key_users          = []

  tags = local.tags
}

# ------------------------------------------------------------------------------
# S3 Data Lake
# ------------------------------------------------------------------------------
module "s3_lake" {
  source = "../../modules/s3_lake"

  data_bucket_name      = "${var.project_name}-data-${var.environment}"
  artifacts_bucket_name = "${var.project_name}-artifacts-${var.environment}"
  kms_key_arn           = module.kms.key_arn
  force_destroy         = false

  data_ia_transition_days            = 90
  data_glacier_transition_days       = 365
  data_expiration_days               = 730
  temp_expiration_days               = 7
  noncurrent_version_expiration_days = 30

  tags = local.tags
}

# ------------------------------------------------------------------------------
# IAM Roles
# ------------------------------------------------------------------------------
module "iam" {
  source = "../../modules/iam"

  project_name = var.project_name
  environment  = var.environment
  kms_key_arn  = module.kms.key_arn

  s3_bucket_arns = [
    module.s3_lake.data_bucket_arn,
    module.s3_lake.artifacts_bucket_arn,
  ]

  tags = local.tags
}

# ------------------------------------------------------------------------------
# Glue Catalog
# ------------------------------------------------------------------------------
module "glue_catalog" {
  source = "../../modules/glue_catalog"

  database_name = "${var.project_name}_${var.environment}"
  description   = "ETL Agent Glue catalog database (${var.environment})"

  tags = local.tags
}

# ------------------------------------------------------------------------------
# Glue Job — ETL Transform
# ------------------------------------------------------------------------------
module "glue_job_transform" {
  source = "../../modules/glue_jobs"

  job_name         = "${var.project_name}-${var.environment}-transform"
  script_location  = "s3://${module.s3_lake.artifacts_bucket_name}/glue-scripts/transform.py"
  role_arn         = module.iam.glue_execution_role_arn
  artifacts_bucket = module.s3_lake.artifacts_bucket_name

  worker_type        = "G.2X"
  number_of_workers  = 5
  timeout            = 240
  max_retries        = 2
  log_retention_days = 30

  extra_arguments = {
    "--catalog_database" = module.glue_catalog.database_name
    "--data_bucket"      = module.s3_lake.data_bucket_name
  }

  # Redshift JDBC connection
  create_redshift_connection    = var.create_redshift_connection
  connection_name               = "${var.project_name}-${var.environment}-redshift"
  redshift_jdbc_url             = "jdbc:redshift://${module.redshift.workgroup_endpoint[0].address}:5439/etl_agent_db"
  redshift_username             = "admin"
  redshift_password             = var.redshift_admin_password
  connection_availability_zone  = var.connection_availability_zone
  connection_security_group_ids = [module.redshift.security_group_id]
  connection_subnet_id          = var.subnet_ids[0]

  tags = local.tags
}

# ------------------------------------------------------------------------------
# Redshift Serverless
# ------------------------------------------------------------------------------
module "redshift" {
  source = "../../modules/redshift"

  namespace_name      = "${var.project_name}-${var.environment}"
  workgroup_name      = "${var.project_name}-${var.environment}-wg"
  database_name       = "etl_agent_db"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password
  data_bucket_name    = module.s3_lake.data_bucket_name

  base_capacity = 32
  max_capacity  = 128

  vpc_id     = var.vpc_id
  subnet_ids = var.subnet_ids

  publicly_accessible = false
  allowed_cidr_blocks = ["10.0.0.0/8"]

  tags = local.tags
}

# ------------------------------------------------------------------------------
# Step Functions
# ------------------------------------------------------------------------------
module "step_functions" {
  source = "../../modules/step_functions"

  state_machine_name = "${var.project_name}-${var.environment}-pipeline"

  discover_lambda_arn  = var.discover_lambda_arn
  translate_lambda_arn = var.translate_lambda_arn
  deploy_lambda_arn    = var.deploy_lambda_arn
  verify_lambda_arn    = var.verify_lambda_arn

  lambda_arns = [
    var.discover_lambda_arn,
    var.translate_lambda_arn,
    var.deploy_lambda_arn,
    var.verify_lambda_arn,
  ]

  dlq_sns_topic_arn   = module.monitoring.sns_topic_arn
  log_retention_days  = 90
  log_level           = "ERROR"
  enable_xray_tracing = true

  tags = local.tags
}

# ------------------------------------------------------------------------------
# Monitoring
# ------------------------------------------------------------------------------
module "monitoring" {
  source = "../../modules/monitoring"

  project_name       = var.project_name
  environment        = var.environment
  alert_email        = var.alert_email
  log_retention_days = 90

  glue_job_name           = module.glue_job_transform.job_name
  state_machine_arn       = module.step_functions.state_machine_arn
  redshift_workgroup_name = "${var.project_name}-${var.environment}-wg"

  tags = local.tags
}
