#!/usr/bin/env bash
# Create Parquet tables uat.sf_course_offerings and uat.moodle_courses
# in the Glue catalog backed by S3 Parquet files.
#
# Prerequisites: source docs/aws_mfa_dev.sh
#
# Usage: bash scripts/load_uat_parquet.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

REGION="us-east-1"
DATA_BUCKET="etl-agent-data-dev"
GLUE_DB="uat"
S3_PREFIX="uat"

SF_JSON="${PROJECT_ROOT}/output/sf_all_course_offerings.json"
MOODLE_JSON="${PROJECT_ROOT}/output/moodle_all_courses_cache.json"

echo "=== Create Parquet Tables in Glue Catalog ==="
echo "  Database:  $GLUE_DB"
echo "  S3:        s3://${DATA_BUCKET}/${S3_PREFIX}/"
echo ""

# Verify credentials
echo "Checking AWS credentials..."
ACCT=$(aws sts get-caller-identity --query "Account" --output text) || {
    echo "ERROR: AWS credentials expired. Run: source docs/aws_mfa_dev.sh"
    exit 1
}
echo "  Account: $ACCT"

# ── Convert JSON → Parquet ───────────────────────────────────────────────
echo ""
echo "Step 1: Converting JSON to Parquet..."

python3 -c "
import json, sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print('ERROR: pyarrow not installed. Run: pip install pyarrow', file=sys.stderr)
    sys.exit(1)

# SF Course Offerings
print('  Converting sf_course_offerings...')
sf = json.load(open('${SF_JSON}'))
sf_table = pa.table({
    'sf_id':  pa.array([r['Id'] for r in sf], type=pa.string()),
    'name':   pa.array([r['Name'] for r in sf], type=pa.string()),
})
pq.write_table(sf_table, '/tmp/sf_course_offerings.parquet')
print(f'    {len(sf)} rows written')

# Moodle Courses
print('  Converting moodle_courses...')
mc = json.load(open('${MOODLE_JSON}'))
mc_table = pa.table({
    'moodle_id':  pa.array([r['id'] for r in mc], type=pa.int64()),
    'shortname':  pa.array([r.get('shortname', '') for r in mc], type=pa.string()),
    'fullname':   pa.array([r.get('fullname', '') for r in mc], type=pa.string()),
    'idnumber':   pa.array([r.get('idnumber', '') for r in mc], type=pa.string()),
})
pq.write_table(mc_table, '/tmp/moodle_courses.parquet')
print(f'    {len(mc)} rows written')
print('  Done.')
"

# ── Upload Parquet to S3 ────────────────────────────────────────────────
echo ""
echo "Step 2: Uploading Parquet files to S3..."
aws s3 cp /tmp/sf_course_offerings.parquet "s3://${DATA_BUCKET}/${S3_PREFIX}/sf_course_offerings/data.parquet"
aws s3 cp /tmp/moodle_courses.parquet "s3://${DATA_BUCKET}/${S3_PREFIX}/moodle_courses/data.parquet"
echo "  Done."

# ── Create Glue database ────────────────────────────────────────────────
echo ""
echo "Step 3: Creating Glue database: ${GLUE_DB}..."
aws glue create-database \
    --database-input "{\"Name\": \"${GLUE_DB}\", \"Description\": \"UAT comparison tables\"}" \
    --region "$REGION" 2>/dev/null || echo "  (already exists)"
echo "  Done."

# ── Create sf_course_offerings table ────────────────────────────────────
echo ""
echo "Step 4: Creating table ${GLUE_DB}.sf_course_offerings..."
aws glue delete-table --database-name "$GLUE_DB" --name "sf_course_offerings" --region "$REGION" 2>/dev/null || true
aws glue create-table \
    --database-name "$GLUE_DB" \
    --region "$REGION" \
    --table-input "{
        \"Name\": \"sf_course_offerings\",
        \"Description\": \"Salesforce CourseOffering records (Id, Name)\",
        \"StorageDescriptor\": {
            \"Columns\": [
                {\"Name\": \"sf_id\", \"Type\": \"string\", \"Comment\": \"SF CourseOffering.Id\"},
                {\"Name\": \"name\", \"Type\": \"string\", \"Comment\": \"SF CourseOffering.Name\"}
            ],
            \"Location\": \"s3://${DATA_BUCKET}/${S3_PREFIX}/sf_course_offerings/\",
            \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
            \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
            \"SerdeInfo\": {
                \"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"
            }
        },
        \"TableType\": \"EXTERNAL_TABLE\",
        \"Parameters\": {
            \"classification\": \"parquet\"
        }
    }"
echo "  Done."

# ── Create moodle_courses table ─────────────────────────────────────────
echo ""
echo "Step 5: Creating table ${GLUE_DB}.moodle_courses..."
aws glue delete-table --database-name "$GLUE_DB" --name "moodle_courses" --region "$REGION" 2>/dev/null || true
aws glue create-table \
    --database-name "$GLUE_DB" \
    --region "$REGION" \
    --table-input "{
        \"Name\": \"moodle_courses\",
        \"Description\": \"Moodle courses (id, shortname, fullname, idnumber)\",
        \"StorageDescriptor\": {
            \"Columns\": [
                {\"Name\": \"moodle_id\", \"Type\": \"bigint\", \"Comment\": \"Moodle course ID\"},
                {\"Name\": \"shortname\", \"Type\": \"string\", \"Comment\": \"Moodle course shortname\"},
                {\"Name\": \"fullname\", \"Type\": \"string\", \"Comment\": \"Moodle course fullname\"},
                {\"Name\": \"idnumber\", \"Type\": \"string\", \"Comment\": \"Moodle course idnumber (SF link)\"}
            ],
            \"Location\": \"s3://${DATA_BUCKET}/${S3_PREFIX}/moodle_courses/\",
            \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
            \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
            \"SerdeInfo\": {
                \"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"
            }
        },
        \"TableType\": \"EXTERNAL_TABLE\",
        \"Parameters\": {
            \"classification\": \"parquet\"
        }
    }"
echo "  Done."

# ── Verify ──────────────────────────────────────────────────────────────
echo ""
echo "=== Verify ==="
echo "Tables in ${GLUE_DB}:"
aws glue get-tables --database-name "$GLUE_DB" --region "$REGION" \
    --query "TableList[].{Name:Name, Columns:StorageDescriptor.Columns[].Name, Location:StorageDescriptor.Location}" \
    --output table

echo ""
echo "=== Done! ==="
echo ""
echo "Query via Redshift Spectrum (create external schema first):"
echo "  CREATE EXTERNAL SCHEMA uat FROM DATA CATALOG"
echo "    DATABASE '${GLUE_DB}' IAM_ROLE '<your-redshift-role-arn>';"
echo ""
echo "  SELECT * FROM uat.sf_course_offerings LIMIT 10;"
echo "  SELECT * FROM uat.moodle_courses LIMIT 10;"
echo ""
echo "Or query via Athena directly against the ${GLUE_DB} database."
