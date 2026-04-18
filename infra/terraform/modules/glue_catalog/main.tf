resource "aws_glue_catalog_database" "this" {
  name        = var.database_name
  description = var.description

  tags = var.tags
}
