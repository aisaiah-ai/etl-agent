#!/usr/bin/env bash
# Create view-named alias tables in the Glue catalog.
# Each alias points to the same S3 data as the source table.
#
# Usage:
#   bash scripts/create_view_aliases.sh [profile]
#
# Example:
#   bash scripts/create_view_aliases.sh prod

set -euo pipefail

PROFILE="${1:-default}"
GLUE_DB="financial_hist_ext"
REGION="us-east-1"

# source_table|alias_name
# Only entries where the names differ
ALIASES=(
  "plutus_budget_enrollment|forecastenrollment"
  "plutus_budget_revision_currency_fx|fx_book_rates"
  "chronus_gl_program|program_groupings"
  "chronus_student_term_admit|ministudentdetailsandstatushistory"
  "chronus_great_plains_db|bookdisplaytypeindex"
  "plutus_budget_entry|forecastenrollmenttotals"
  "plutus_budget_revision|slicer_fxbookrates"
)

echo "============================================"
echo "  Create View-Named Aliases in Glue Catalog"
echo "  Database: $GLUE_DB  Profile: $PROFILE"
echo "============================================"
echo ""

for entry in "${ALIASES[@]}"; do
  IFS='|' read -r src alias_name <<< "$entry"

  echo "  $src → $alias_name"

  # Get source table definition
  table_json=$(aws glue get-table \
    --database-name "$GLUE_DB" \
    --name "$src" \
    --profile "$PROFILE" \
    --region "$REGION" \
    --query "Table.{StorageDescriptor:StorageDescriptor,PartitionKeys:PartitionKeys,TableType:TableType,Parameters:Parameters}" \
    --output json 2>/dev/null) || {
      echo "    SKIP: source table $src not found"
      continue
    }

  if [ -z "$table_json" ] || [ "$table_json" = "null" ]; then
    echo "    SKIP: source table $src returned null"
    continue
  fi

  # Build table input with alias name
  table_input=$(echo "$table_json" | jq --arg name "$alias_name" '{
    Name: $name,
    StorageDescriptor: .StorageDescriptor,
    PartitionKeys: (.PartitionKeys // []),
    TableType: (.TableType // "EXTERNAL_TABLE"),
    Parameters: ((.Parameters // {}) + {"alias_source": $name})
  }')

  # Create or update (NEVER delete prod tables)
  if aws glue get-table --database-name "$GLUE_DB" --name "$alias_name" \
      --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
    aws glue update-table \
      --database-name "$GLUE_DB" \
      --table-input "$table_input" \
      --profile "$PROFILE" \
      --region "$REGION" 2>/dev/null
    echo "    Updated"
    continue
  fi

  aws glue create-table \
    --database-name "$GLUE_DB" \
    --table-input "$table_input" \
    --profile "$PROFILE" \
    --region "$REGION" 2>/dev/null

  echo "    OK"
done

echo ""
echo "Done. Listing all tables in $GLUE_DB:"
aws glue get-tables \
  --database-name "$GLUE_DB" \
  --profile "$PROFILE" \
  --region "$REGION" \
  --query "TableList[].Name" \
  --output table
