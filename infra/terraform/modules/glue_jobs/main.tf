# ------------------------------------------------------------------------------
# Glue Connection (JDBC to Redshift)
# ------------------------------------------------------------------------------
resource "aws_glue_connection" "redshift" {
  count = var.create_redshift_connection ? 1 : 0

  name            = var.connection_name
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = var.redshift_jdbc_url
    USERNAME            = var.redshift_username
    PASSWORD            = var.redshift_password
  }

  physical_connection_requirements {
    availability_zone      = var.connection_availability_zone
    security_group_id_list = var.connection_security_group_ids
    subnet_id              = var.connection_subnet_id
  }

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Glue Job
# ------------------------------------------------------------------------------
resource "aws_glue_job" "this" {
  name     = var.job_name
  role_arn = var.role_arn

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout
  max_retries       = var.max_retries

  command {
    name            = "glueetl"
    script_location = var.script_location
    python_version  = "3"
  }

  default_arguments = merge(
    {
      "--enable-metrics"                   = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-continuous-log-filter"     = "true"
      "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.job_name}"
      "--job-bookmark-option"              = "job-bookmark-enable"
      "--TempDir"                          = "s3://${var.artifacts_bucket}/glue-temp/${var.job_name}/"
      "--enable-spark-ui"                  = "true"
      "--spark-event-logs-path"            = "s3://${var.artifacts_bucket}/spark-ui-logs/${var.job_name}/"
      "--enable-glue-datacatalog"          = "true"
    },
    var.extra_arguments,
  )

  connections = var.create_redshift_connection ? [aws_glue_connection.redshift[0].name] : []

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.job_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}
