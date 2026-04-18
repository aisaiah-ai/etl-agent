#!/usr/bin/env bash
# seed_localstack.sh — Runs inside LocalStack on startup to create Glue catalog and S3 buckets.
set -euo pipefail

echo "=== Seeding LocalStack ==="

# Create S3 buckets
awslocal s3 mb s3://etl-agent-data-local 2>/dev/null || true
awslocal s3 mb s3://etl-agent-artifacts-local 2>/dev/null || true

# Create Glue catalog database
awslocal glue create-database --database-input '{"Name": "etl_agent_local", "Description": "Local dev Glue catalog"}' 2>/dev/null || true

# Create Glue tables matching the source tables in local PostgreSQL
awslocal glue create-table --database-name etl_agent_local --table-input '{
  "Name": "patients",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "patient_id", "Type": "string"},
      {"Name": "first_name", "Type": "string"},
      {"Name": "last_name", "Type": "string"},
      {"Name": "date_of_birth", "Type": "date"},
      {"Name": "gender", "Type": "string"},
      {"Name": "zip_code", "Type": "string"},
      {"Name": "state", "Type": "string"},
      {"Name": "phone", "Type": "string"},
      {"Name": "email", "Type": "string"},
      {"Name": "created_at", "Type": "timestamp"}
    ],
    "Location": "s3://etl-agent-data-local/patients/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  },
  "TableType": "EXTERNAL_TABLE"
}' 2>/dev/null || true

awslocal glue create-table --database-name etl_agent_local --table-input '{
  "Name": "claims",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "claim_id", "Type": "string"},
      {"Name": "patient_id", "Type": "string"},
      {"Name": "provider_id", "Type": "string"},
      {"Name": "claim_type", "Type": "string"},
      {"Name": "service_date", "Type": "date"},
      {"Name": "diagnosis_code", "Type": "string"},
      {"Name": "procedure_code", "Type": "string"},
      {"Name": "charged_amount", "Type": "decimal(12,2)"},
      {"Name": "paid_amount", "Type": "decimal(12,2)"},
      {"Name": "status", "Type": "string"},
      {"Name": "submitted_at", "Type": "timestamp"}
    ],
    "Location": "s3://etl-agent-data-local/claims/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  },
  "TableType": "EXTERNAL_TABLE"
}' 2>/dev/null || true

awslocal glue create-table --database-name etl_agent_local --table-input '{
  "Name": "encounters",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "encounter_id", "Type": "string"},
      {"Name": "patient_id", "Type": "string"},
      {"Name": "provider_id", "Type": "string"},
      {"Name": "encounter_type", "Type": "string"},
      {"Name": "encounter_date", "Type": "date"},
      {"Name": "discharge_date", "Type": "date"},
      {"Name": "facility_id", "Type": "string"},
      {"Name": "primary_dx", "Type": "string"},
      {"Name": "drg_code", "Type": "string"},
      {"Name": "los_days", "Type": "int"}
    ],
    "Location": "s3://etl-agent-data-local/encounters/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  },
  "TableType": "EXTERNAL_TABLE"
}' 2>/dev/null || true

awslocal glue create-table --database-name etl_agent_local --table-input '{
  "Name": "providers",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "provider_id", "Type": "string"},
      {"Name": "provider_name", "Type": "string"},
      {"Name": "specialty", "Type": "string"},
      {"Name": "npi", "Type": "string"},
      {"Name": "facility_id", "Type": "string"},
      {"Name": "state", "Type": "string"}
    ],
    "Location": "s3://etl-agent-data-local/providers/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  },
  "TableType": "EXTERNAL_TABLE"
}' 2>/dev/null || true

awslocal glue create-table --database-name etl_agent_local --table-input '{
  "Name": "payer_plans",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "plan_id", "Type": "string"},
      {"Name": "plan_name", "Type": "string"},
      {"Name": "payer_name", "Type": "string"},
      {"Name": "plan_type", "Type": "string"},
      {"Name": "effective_date", "Type": "date"},
      {"Name": "termination_date", "Type": "date"}
    ],
    "Location": "s3://etl-agent-data-local/payer_plans/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  },
  "TableType": "EXTERNAL_TABLE"
}' 2>/dev/null || true

echo "=== LocalStack seeded successfully ==="
echo "  - S3 buckets: etl-agent-data-local, etl-agent-artifacts-local"
echo "  - Glue database: etl_agent_local"
echo "  - Glue tables: patients, claims, encounters, providers, payer_plans"
