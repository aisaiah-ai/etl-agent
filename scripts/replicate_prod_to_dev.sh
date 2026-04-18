#!/usr/bin/env bash
# Replicate prod Glue catalog data to dev
# Architecture:
#   Prod S3 (ies-devteam-prd) → Dev S3 (etl-agent-data-dev) → Dev Glue Catalog → Dev Redshift External Schema
#
# Prerequisites:
#   - Both prod and dev credentials must be active
#   - prod profile in ~/.aws/credentials (source docs/aws_mfa_prod.sh)
#   - default profile for dev (source docs/aws_mfa_dev.sh)
#
# Usage:
#   bash scripts/replicate_prod_to_dev.sh

set -euo pipefail

# --- Configuration ---
PROD_PROFILE="prod"
PROD_BUCKET="ies-devteam-prd"
DEV_REGION="us-east-1"
DEV_BUCKET="etl-agent-data-dev"
DEV_GLUE_DB="financial_hist_ext"
DEV_REDSHIFT_WG="etl-agent-dev-wg"
DEV_REDSHIFT_DB="etl_agent_db"
DEV_EXTERNAL_SCHEMA="financial_hist_ext"
DEV_VIEW_SCHEMA="financial_hist"
MAX_FILES=5  # Limit files per table for dev subset

# Tables to replicate: "prod_glue_db|table_name|s3_prefix"
TABLES=(
  "financial_hist_ext|historic_all_financial_transactions|financial_hist_ext/historic_all_financial_transactions"
  "financial_hist_ext|fx_rates_hist|financial_hist_ext/fx_rates_hist"
  "financial_hist_ext|plutus_budget_enrollment|financial_hist_ext/plutus_budget_enrollment"
  "financial_hist_ext|plutus_budget_entry|financial_hist_ext/plutus_budget_entry"
  "financial_hist_ext|plutus_budget_revision|financial_hist_ext/plutus_budget_revision"
  "financial_hist_ext|plutus_budget_revision_currency_fx|financial_hist_ext/plutus_budget_revision_currency_fx"
  "financial_hist_ext|chronus_student_term_admit|financial_hist_ext/chronus_student_term_admit"
  "financial_hist_ext|chronus_gl_program|financial_hist_ext/chronus_gl_program"
  "financial_hist_ext|chronus_great_plains_db|financial_hist_ext/chronus_great_plains_db"
)

# View mappings: "external_table|view_name"
# These create financial_hist.<view_name> AS SELECT * FROM financial_hist_ext.<external_table>
VIEWS=(
  "historic_all_financial_transactions|historic_all_financial_transactions"
  "fx_rates_hist|fx_rates_hist"
  "plutus_budget_enrollment|forecastenrollment"
  "plutus_budget_revision_currency_fx|fx_book_rates"
  "chronus_gl_program|program_groupings"
  "chronus_student_term_admit|ministudentdetailsandstatushistory"
  "chronus_great_plains_db|bookdisplaytypeindex"
  "plutus_budget_entry|forecastenrollmenttotals"
  "plutus_budget_revision|slicer_fxbookrates"
)

# Helper: run SQL on dev Redshift and wait for completion
run_redshift_sql() {
  local sql="$1"
  local stmt_id
  stmt_id=$(aws redshift-data execute-statement \
    --workgroup-name "$DEV_REDSHIFT_WG" \
    --database "$DEV_REDSHIFT_DB" \
    --sql "$sql" \
    --query "Id" --output text 2>&1)

  if [[ "$stmt_id" == *"rror"* ]]; then
    echo "    ERROR submitting: $stmt_id"
    return 1
  fi

  local status="SUBMITTED"
  while [[ "$status" == "SUBMITTED" || "$status" == "PICKED" || "$status" == "STARTED" ]]; do
    sleep 2
    status=$(aws redshift-data describe-statement --id "$stmt_id" --query "Status" --output text 2>/dev/null)
  done

  if [[ "$status" != "FINISHED" ]]; then
    local err
    err=$(aws redshift-data describe-statement --id "$stmt_id" --query "Error" --output text 2>/dev/null)
    echo "    FAILED ($status): $err"
    return 1
  fi
  return 0
}

echo "============================================"
echo "  Prod → Dev Data Replication"
echo "============================================"

# --- Step 1: Copy S3 data from prod to dev ---
echo ""
echo "Step 1: Copying S3 data from prod to dev..."
echo "  Source: s3://$PROD_BUCKET"
echo "  Dest:   s3://$DEV_BUCKET/financial_hist/"

for entry in "${TABLES[@]}"; do
  IFS='|' read -r prod_db table_name s3_prefix <<< "$entry"

  echo ""
  echo "  Copying: $table_name"
  echo "    From: s3://$PROD_BUCKET/$s3_prefix/"
  echo "    To:   s3://$DEV_BUCKET/financial_hist/$table_name/"

  # List files from prod, take subset
  prod_files=$(aws s3 ls "s3://$PROD_BUCKET/$s3_prefix/" --profile "$PROD_PROFILE" 2>/dev/null | head -$MAX_FILES | awk '{print $NF}') || true

  if [ -z "$prod_files" ]; then
    echo "    WARNING: No files found in prod, skipping."
    continue
  fi

  file_count=0
  while IFS= read -r file; do
    [ -z "$file" ] && continue
    aws s3 cp "s3://$PROD_BUCKET/$s3_prefix/$file" "/tmp/etl_replicate_$file" --profile "$PROD_PROFILE" --quiet 2>/dev/null
    aws s3 cp "/tmp/etl_replicate_$file" "s3://$DEV_BUCKET/financial_hist/$table_name/$file" --quiet 2>/dev/null
    rm -f "/tmp/etl_replicate_$file"
    file_count=$((file_count + 1))
  done <<< "$prod_files"
  echo "    Copied $file_count files."
done

# --- Step 2: Create Glue database in dev ---
echo ""
echo "Step 2: Creating Glue database in dev: $DEV_GLUE_DB"
aws glue create-database --database-input "{
  \"Name\": \"$DEV_GLUE_DB\",
  \"Description\": \"Dev replica of prod financial_hist_ext tables\",
  \"LocationUri\": \"s3://$DEV_BUCKET/financial_hist/\"
}" 2>/dev/null || echo "  Database already exists."

# --- Step 3: Register tables in dev Glue catalog ---
echo ""
echo "Step 3: Registering tables in dev Glue catalog..."

for entry in "${TABLES[@]}"; do
  IFS='|' read -r prod_db table_name s3_prefix <<< "$entry"

  echo "  Registering: $table_name"

  # Get table definition from prod
  table_def=$(aws glue get-table \
    --database-name "$prod_db" \
    --name "$table_name" \
    --profile "$PROD_PROFILE" \
    --query "Table.{Name:Name,StorageDescriptor:StorageDescriptor,PartitionKeys:PartitionKeys,TableType:TableType,Parameters:Parameters}" \
    --output json 2>/dev/null) || true

  if [ -z "$table_def" ] || [ "$table_def" = "null" ]; then
    echo "    WARNING: Table not found in prod ($prod_db.$table_name), skipping."
    continue
  fi

  # Update S3 location to dev bucket
  updated_def=$(echo "$table_def" | jq --arg loc "s3://$DEV_BUCKET/financial_hist/$table_name/" \
    '.StorageDescriptor.Location = $loc')

  # Build table input
  table_input=$(echo "$updated_def" | jq '{
    Name: .Name,
    StorageDescriptor: .StorageDescriptor,
    PartitionKeys: (.PartitionKeys // []),
    TableType: (.TableType // "EXTERNAL_TABLE"),
    Parameters: (.Parameters // {})
  }')

  # Delete and recreate
  aws glue delete-table --database-name "$DEV_GLUE_DB" --name "$table_name" 2>/dev/null || true
  aws glue create-table --database-name "$DEV_GLUE_DB" --table-input "$table_input" 2>/dev/null
  echo "    Done."
done

# --- Step 4: Create external schema and views in Redshift ---
echo ""
echo "Step 4: Creating external schema and views in Redshift..."

REDSHIFT_ROLE=$(aws iam get-role --role-name etl-agent-dev-redshift-role --query "Role.Arn" --output text 2>/dev/null)
echo "  Redshift IAM Role: $REDSHIFT_ROLE"

# Drop and recreate external schema (clean state)
echo "  Creating external schema: $DEV_EXTERNAL_SCHEMA → Glue $DEV_GLUE_DB"
run_redshift_sql "CREATE EXTERNAL SCHEMA IF NOT EXISTS $DEV_EXTERNAL_SCHEMA FROM DATA CATALOG DATABASE '$DEV_GLUE_DB' IAM_ROLE '$REDSHIFT_ROLE' REGION '$DEV_REGION';"

# Create the view schema (drop existing tables first since we're replacing with views)
echo "  Dropping old tables in $DEV_VIEW_SCHEMA (replacing with views)..."
for entry in "${VIEWS[@]}"; do
  IFS='|' read -r ext_table view_name <<< "$entry"
  run_redshift_sql "DROP TABLE IF EXISTS $DEV_VIEW_SCHEMA.$view_name CASCADE;" || true
done

echo "  Creating view schema: $DEV_VIEW_SCHEMA"
run_redshift_sql "CREATE SCHEMA IF NOT EXISTS $DEV_VIEW_SCHEMA;"

# Create simple SELECT * views
echo "  Creating views..."
for entry in "${VIEWS[@]}"; do
  IFS='|' read -r ext_table view_name <<< "$entry"
  echo "    $DEV_VIEW_SCHEMA.$view_name → SELECT * FROM $DEV_EXTERNAL_SCHEMA.$ext_table"
  run_redshift_sql "CREATE OR REPLACE VIEW $DEV_VIEW_SCHEMA.$view_name AS SELECT * FROM $DEV_EXTERNAL_SCHEMA.$ext_table WITH NO SCHEMA BINDING;" || true
done

echo ""
echo "============================================"
echo "  Replication complete!"
echo "============================================"
echo ""
echo "Dev Redshift: $DEV_REDSHIFT_WG / $DEV_REDSHIFT_DB"
echo "Glue catalog: $DEV_GLUE_DB (same name as prod)"
echo "External schema: $DEV_EXTERNAL_SCHEMA"
echo "View schema: $DEV_VIEW_SCHEMA (simple SELECT * views)"
echo ""
echo "Test:"
echo "  aws redshift-data execute-statement \\"
echo "    --workgroup-name $DEV_REDSHIFT_WG --database $DEV_REDSHIFT_DB \\"
echo "    --sql \"SELECT * FROM $DEV_VIEW_SCHEMA.historic_all_financial_transactions LIMIT 10;\""
