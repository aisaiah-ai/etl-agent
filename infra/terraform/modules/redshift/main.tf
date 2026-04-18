data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ------------------------------------------------------------------------------
# IAM Role for Redshift Serverless
# ------------------------------------------------------------------------------
resource "aws_iam_role" "redshift" {
  name = "${var.namespace_name}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "redshift_s3_glue" {
  name = "${var.namespace_name}-s3-glue-access"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          "arn:aws:s3:::${var.data_bucket_name}",
          "arn:aws:s3:::${var.data_bucket_name}/*",
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
        ]
      },
    ]
  })
}

# ------------------------------------------------------------------------------
# Security Group — use existing if provided, otherwise create new
# ------------------------------------------------------------------------------
resource "aws_security_group" "redshift" {
  count = var.existing_security_group_id == "" ? 1 : 0

  name        = "${var.namespace_name}-redshift-sg"
  description = "Security group for Redshift Serverless workgroup"
  vpc_id      = var.vpc_id

  ingress {
    description = "Redshift port"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.namespace_name}-redshift-sg"
  })
}

locals {
  security_group_id = var.existing_security_group_id != "" ? var.existing_security_group_id : aws_security_group.redshift[0].id
}

# ------------------------------------------------------------------------------
# Redshift Serverless
# ------------------------------------------------------------------------------
resource "aws_redshiftserverless_namespace" "this" {
  namespace_name       = var.namespace_name
  db_name              = var.database_name
  admin_username       = var.admin_username
  admin_user_password  = var.admin_user_password
  iam_roles            = [aws_iam_role.redshift.arn]
  default_iam_role_arn = aws_iam_role.redshift.arn

  log_exports = ["userlog", "connectionlog", "useractivitylog"]

  tags = var.tags
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = var.workgroup_name

  base_capacity = var.base_capacity
  max_capacity  = var.max_capacity

  subnet_ids         = var.subnet_ids
  security_group_ids = [local.security_group_id]

  publicly_accessible = var.publicly_accessible

  tags = var.tags
}
