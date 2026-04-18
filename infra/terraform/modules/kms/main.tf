data "aws_caller_identity" "current" {}

resource "aws_kms_key" "this" {
  description             = var.description
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = var.enable_key_rotation

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid    = "RootAccountFullAccess"
          Effect = "Allow"
          Principal = {
            AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
          }
          Action   = "kms:*"
          Resource = "*"
        },
      ],
      length(var.key_administrators) > 0 ? [
        {
          Sid    = "AllowKeyAdministration"
          Effect = "Allow"
          Principal = {
            AWS = var.key_administrators
          }
          Action = [
            "kms:Create*",
            "kms:Describe*",
            "kms:Enable*",
            "kms:List*",
            "kms:Put*",
            "kms:Update*",
            "kms:Revoke*",
            "kms:Disable*",
            "kms:Get*",
            "kms:Delete*",
            "kms:TagResource",
            "kms:UntagResource",
            "kms:ScheduleKeyDeletion",
            "kms:CancelKeyDeletion",
          ]
          Resource = "*"
        },
      ] : [],
      length(var.key_users) > 0 ? [
        {
          Sid    = "AllowServiceAccess"
          Effect = "Allow"
          Principal = {
            AWS = var.key_users
          }
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:DescribeKey",
          ]
          Resource = "*"
        },
      ] : [],
      [
        {
          Sid    = "AllowGlueService"
          Effect = "Allow"
          Principal = {
            Service = "glue.amazonaws.com"
          }
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:GenerateDataKey",
          ]
          Resource = "*"
        },
        {
          Sid    = "AllowS3Service"
          Effect = "Allow"
          Principal = {
            Service = "s3.amazonaws.com"
          }
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:GenerateDataKey",
          ]
          Resource = "*"
        },
      ],
    )
  })

  tags = var.tags
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.alias_name}"
  target_key_id = aws_kms_key.this.key_id
}
