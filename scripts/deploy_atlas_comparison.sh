#!/usr/bin/env bash
# Deploy Atlas ID comparison: upload data to S3, register Glue tables, run job.
#
# Prerequisites: source docs/aws_mfa_dev.sh
#
# Usage: bash scripts/deploy_atlas_comparison.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

REGION="us-east-1"
ACCOUNT_ID="533267300255"
DATA_BUCKET="etl-agent-data-dev"
GLUE_DB="uat"
S3_DATA_PREFIX="uat"
JOB_NAME="atlas-id-comparison"
SCRIPT_BUCKET="aws-glue-assets-${ACCOUNT_ID}-${REGION}"
SCRIPT_KEY="scripts/${JOB_NAME}.py"
LOCAL_SCRIPT="${PROJECT_ROOT}/pipelines/glue_jobs/atlas_id_comparison.py"
OUTPUT_PREFIX="atlas_comparison/"

echo "=== Atlas ID Comparison — Glue Dev ==="
echo "  Database:    $GLUE_DB"
echo "  Data bucket: $DATA_BUCKET"
echo ""

# Verify credentials
echo "Checking AWS credentials..."
ACCT=$(aws sts get-caller-identity --query "Account" --output text) || {
    echo "ERROR: AWS credentials expired. Run: source docs/aws_mfa_dev.sh"
    exit 1
}
echo "  Account: $ACCT"

# ── Step 1: Upload Parquet data to S3 ───────────────────────────────────
echo ""
echo "Step 1: Uploading Parquet data to S3..."
aws s3 cp "${PROJECT_ROOT}/output/sf_course_offerings_uat.parquet" \
    "s3://${DATA_BUCKET}/${S3_DATA_PREFIX}/sf_course_offerings_uat/data.parquet"
aws s3 cp "${PROJECT_ROOT}/output/moodle_courses.parquet" \
    "s3://${DATA_BUCKET}/${S3_DATA_PREFIX}/moodle_courses/data.parquet"
echo "  Done."

# ── Step 2: Create Glue database ────────────────────────────────────────
echo ""
echo "Step 2: Creating Glue database: ${GLUE_DB}..."
aws glue create-database \
    --database-input "{\"Name\": \"${GLUE_DB}\", \"Description\": \"UAT comparison tables\"}" \
    --region "$REGION" 2>/dev/null || echo "  (already exists)"

# ── Step 3: Create sf_course_offerings_uat table ────────────────────────
echo ""
echo "Step 3: Creating table ${GLUE_DB}.sf_course_offerings_uat..."
aws glue delete-table --database-name "$GLUE_DB" --name "sf_course_offerings_uat" --region "$REGION" 2>/dev/null || true
aws glue create-table \
    --database-name "$GLUE_DB" \
    --region "$REGION" \
    --table-input "{
        \"Name\": \"sf_course_offerings_uat\",
        \"Description\": \"SF CourseOffering with Atlas ID (HCG_External_ID__c)\",
        \"StorageDescriptor\": {
            \"Columns\": [
                {\"Name\": \"sf_id\", \"Type\": \"string\"},
                {\"Name\": \"name\", \"Type\": \"string\"},
                {\"Name\": \"learning_course_id\", \"Type\": \"string\"},
                {\"Name\": \"learning_course_name\", \"Type\": \"string\"},
                {\"Name\": \"atlas_id\", \"Type\": \"string\"}
            ],
            \"Location\": \"s3://${DATA_BUCKET}/${S3_DATA_PREFIX}/sf_course_offerings_uat/\",
            \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
            \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
            \"SerdeInfo\": {
                \"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"
            }
        },
        \"TableType\": \"EXTERNAL_TABLE\",
        \"Parameters\": {\"classification\": \"parquet\"}
    }"
echo "  Done."

# ── Step 4: Create moodle_courses table ─────────────────────────────────
echo ""
echo "Step 4: Creating table ${GLUE_DB}.moodle_courses..."
aws glue delete-table --database-name "$GLUE_DB" --name "moodle_courses" --region "$REGION" 2>/dev/null || true
aws glue create-table \
    --database-name "$GLUE_DB" \
    --region "$REGION" \
    --table-input "{
        \"Name\": \"moodle_courses\",
        \"Description\": \"Moodle courses with Atlas ID in shortname\",
        \"StorageDescriptor\": {
            \"Columns\": [
                {\"Name\": \"moodle_id\", \"Type\": \"bigint\"},
                {\"Name\": \"shortname\", \"Type\": \"string\"},
                {\"Name\": \"fullname\", \"Type\": \"string\"},
                {\"Name\": \"idnumber\", \"Type\": \"string\"}
            ],
            \"Location\": \"s3://${DATA_BUCKET}/${S3_DATA_PREFIX}/moodle_courses/\",
            \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
            \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
            \"SerdeInfo\": {
                \"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"
            }
        },
        \"TableType\": \"EXTERNAL_TABLE\",
        \"Parameters\": {\"classification\": \"parquet\"}
    }"
echo "  Done."

# ── Step 5: Verify tables ──────────────────────────────────────────────
echo ""
echo "Step 5: Verify tables..."
aws glue get-tables --database-name "$GLUE_DB" --region "$REGION" \
    --query "TableList[].{Name:Name, Location:StorageDescriptor.Location}" \
    --output table

# ── Step 6: Upload Glue job script ──────────────────────────────────────
echo ""
echo "Step 6: Uploading Glue job script..."
if ! aws s3 ls "s3://${SCRIPT_BUCKET}" > /dev/null 2>&1; then
    echo "  Creating script bucket: ${SCRIPT_BUCKET}"
    aws s3 mb "s3://${SCRIPT_BUCKET}" --region "$REGION"
fi
aws s3 cp "$LOCAL_SCRIPT" "s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}"
echo "  Done."

# ── Step 7: Find Glue role ──────────────────────────────────────────────
echo ""
echo "Step 7: Looking up Glue role..."
GLUE_ROLE=$(aws glue get-jobs --max-results 5 --query "Jobs[0].Role" --output text 2>/dev/null || true)
if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    GLUE_ROLE=$(aws iam list-roles \
        --query "Roles[?contains(RoleName, 'Glue') || contains(RoleName, 'glue')].Arn | [0]" \
        --output text 2>/dev/null || true)
fi
if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    echo "ERROR: No Glue role found."
    printf "Role ARN: " && read -r GLUE_ROLE
fi
echo "  Role: $GLUE_ROLE"

# ── Step 8: Create/update Glue job ──────────────────────────────────────
echo ""
echo "Step 8: Creating/updating Glue job: ${JOB_NAME}..."

EXISTING=$(aws glue get-job --job-name "$JOB_NAME" 2>/dev/null && echo "yes" || echo "no")

JOB_CONFIG="{
    \"Role\": \"${GLUE_ROLE}\",
    \"Command\": {
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}\",
        \"PythonVersion\": \"3\"
    },
    \"GlueVersion\": \"4.0\",
    \"WorkerType\": \"G.1X\",
    \"NumberOfWorkers\": 2,
    \"Timeout\": 30,
    \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--database\": \"${GLUE_DB}\",
        \"--output_bucket\": \"${DATA_BUCKET}\",
        \"--output_prefix\": \"${OUTPUT_PREFIX}\"
    }
}"

if [ "$EXISTING" = "yes" ]; then
    echo "  Updating..."
    aws glue update-job --job-name "$JOB_NAME" --job-update "$JOB_CONFIG" > /dev/null
else
    echo "  Creating..."
    aws glue create-job \
        --name "$JOB_NAME" \
        --role "$GLUE_ROLE" \
        --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}\", \"PythonVersion\": \"3\"}" \
        --glue-version "4.0" --worker-type "G.1X" --number-of-workers 2 --timeout 30 \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--enable-metrics\": \"true\",
            \"--database\": \"${GLUE_DB}\",
            \"--output_bucket\": \"${DATA_BUCKET}\",
            \"--output_prefix\": \"${OUTPUT_PREFIX}\"
        }" > /dev/null
fi
echo "  Done."

# ── Step 9: Start job ──────────────────────────────────────────────────
echo ""
echo "Step 9: Starting job run..."
RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --query "JobRunId" --output text)
echo "  Run ID: $RUN_ID"

# ── Step 10: Wait for completion ────────────────────────────────────────
echo ""
echo "Step 10: Waiting for job..."
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name "$JOB_NAME" --run-id "$RUN_ID" \
        --query "JobRun.JobRunState" --output text 2>/dev/null)

    echo "  $(date +%H:%M:%S) $STATUS"

    case "$STATUS" in
        SUCCEEDED)
            echo ""
            echo "Job completed!"
            echo ""
            echo "Results:"
            echo "  aws s3 ls s3://${DATA_BUCKET}/${OUTPUT_PREFIX} --recursive"
            echo ""
            echo "Download:"
            echo "  aws s3 cp s3://${DATA_BUCKET}/${OUTPUT_PREFIX} output/atlas_comparison/ --recursive"
            echo ""
            echo "Logs:"
            echo "  aws logs tail /aws-glue/jobs/output --since 15m"
            break
            ;;
        FAILED|ERROR|TIMEOUT)
            echo ""
            echo "Job FAILED!"
            aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
                --query "JobRun.ErrorMessage" --output text
            echo ""
            echo "  aws logs tail /aws-glue/jobs/error --since 15m"
            exit 1
            ;;
        STOPPED)
            echo "Job stopped."
            exit 1
            ;;
        *)
            sleep 20
            ;;
    esac
done
