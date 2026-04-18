#!/usr/bin/env bash
# Deploy and run the materialize_qsreport Glue job in prod.
# Creates the historic_financial_qsreport table in Glue catalog
# by joining all 9 source tables.
#
# Prerequisites:
#   - Prod credentials active (source docs/aws_mfa_prod.sh)
#   - View aliases created (bash scripts/create_view_aliases.sh prod)
#
# Usage:
#   bash scripts/deploy_qsreport.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PROFILE="prod"
REGION="us-east-1"
ACCOUNT_ID="442594162630"
JOB_NAME="materialize-qsreport"
SCRIPT_BUCKET="aws-glue-assets-${ACCOUNT_ID}-${REGION}"
SCRIPT_KEY="scripts/${JOB_NAME}.py"
LOCAL_SCRIPT="${PROJECT_ROOT}/pipelines/glue_jobs/materialize_qsreport.py"

# Output config
GLUE_DB="financial_hist_ext"
OUTPUT_BUCKET="ies-devteam-prd"
OUTPUT_PREFIX="financial_hist_ext"

echo "=== Materialize historic_financial_qsreport ==="
echo "  Glue DB:  $GLUE_DB"
echo "  Output:   s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}/"
echo ""

# ── Find Glue role ────────────────────────────────────────────────────────
echo "Step 1: Looking up Glue role..."
GLUE_ROLE=$(aws glue get-jobs \
    --profile "$PROFILE" \
    --max-results 1 \
    --query "Jobs[0].Role" \
    --output text 2>/dev/null || true)

if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    GLUE_ROLE=$(aws iam list-roles \
        --profile "$PROFILE" \
        --query "Roles[?contains(RoleName,'Glue')].Arn | [0]" \
        --output text 2>/dev/null || true)
fi

if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    echo "ERROR: Could not find Glue role."
    printf "Role ARN: " && read -r GLUE_ROLE
fi
echo "  Role: $GLUE_ROLE"

# ── Upload script ─────────────────────────────────────────────────────────
echo ""
echo "Step 2: Uploading script..."
aws s3 cp "$LOCAL_SCRIPT" "s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}" --profile "$PROFILE"
echo "  Done."

# ── Create/update Glue job ────────────────────────────────────────────────
echo ""
echo "Step 3: Creating/updating Glue job: ${JOB_NAME}..."

EXISTING=$(aws glue get-job --job-name "$JOB_NAME" --profile "$PROFILE" 2>/dev/null && echo "yes" || echo "no")

JOB_CONFIG="{
    \"Role\": \"${GLUE_ROLE}\",
    \"Command\": {
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}\",
        \"PythonVersion\": \"3\"
    },
    \"GlueVersion\": \"4.0\",
    \"WorkerType\": \"G.1X\",
    \"NumberOfWorkers\": 10,
    \"Timeout\": 240,
    \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--enable-spark-ui\": \"true\",
        \"--enable-glue-datacatalog\": \"true\",
        \"--spark-event-logs-path\": \"s3://${SCRIPT_BUCKET}/sparkHistoryLogs/\",
        \"--glue_database\": \"${GLUE_DB}\",
        \"--output_bucket\": \"${OUTPUT_BUCKET}\",
        \"--output_prefix\": \"${OUTPUT_PREFIX}\"
    }
}"

if [ "$EXISTING" = "yes" ]; then
    echo "  Job exists, updating..."
    aws glue update-job \
        --job-name "$JOB_NAME" \
        --profile "$PROFILE" \
        --job-update "$JOB_CONFIG"
else
    echo "  Creating new job..."
    aws glue create-job \
        --name "$JOB_NAME" \
        --profile "$PROFILE" \
        --role "$GLUE_ROLE" \
        --command "{
            \"Name\": \"glueetl\",
            \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}\",
            \"PythonVersion\": \"3\"
        }" \
        --glue-version "4.0" \
        --worker-type "G.1X" \
        --number-of-workers 10 \
        --timeout 240 \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--enable-metrics\": \"true\",
            \"--enable-spark-ui\": \"true\",
            \"--enable-glue-datacatalog\": \"true\",
            \"--spark-event-logs-path\": \"s3://${SCRIPT_BUCKET}/sparkHistoryLogs/\",
            \"--glue_database\": \"${GLUE_DB}\",
            \"--output_bucket\": \"${OUTPUT_BUCKET}\",
            \"--output_prefix\": \"${OUTPUT_PREFIX}\"
        }"
fi
echo "  Done."

# ── Start job run ─────────────────────────────────────────────────────────
echo ""
echo "Step 4: Starting job run..."
RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --profile "$PROFILE" \
    --query "JobRunId" \
    --output text)

echo "  Job run started: $RUN_ID"

# ── Wait for completion ───────────────────────────────────────────────────
echo ""
echo "Step 5: Waiting for job to complete..."
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name "$JOB_NAME" \
        --run-id "$RUN_ID" \
        --profile "$PROFILE" \
        --query "JobRun.JobRunState" \
        --output text 2>/dev/null)

    echo "  $(date +%H:%M:%S) Status: $STATUS"

    case "$STATUS" in
        SUCCEEDED)
            echo ""
            echo "Job completed successfully!"
            echo ""
            echo "Verify table:"
            echo "  aws glue get-table --database-name $GLUE_DB --name historic_financial_qsreport --profile $PROFILE --query 'Table.{Name:Name,Columns:StorageDescriptor.NumberOfBuckets,Location:StorageDescriptor.Location}'"
            echo ""
            echo "Preview data:"
            echo "  aws athena start-query-execution --query-string 'SELECT * FROM $GLUE_DB.historic_financial_qsreport LIMIT 10' --profile $PROFILE"
            echo ""
            echo "View logs:"
            echo "  aws logs tail /aws-glue/jobs/output --profile $PROFILE --since 15m"
            break
            ;;
        FAILED|ERROR|TIMEOUT)
            echo ""
            echo "Job FAILED!"
            ERROR_MSG=$(aws glue get-job-run \
                --job-name "$JOB_NAME" \
                --run-id "$RUN_ID" \
                --profile "$PROFILE" \
                --query "JobRun.ErrorMessage" \
                --output text 2>/dev/null)
            echo "  Error: $ERROR_MSG"
            echo ""
            echo "Check logs:"
            echo "  aws logs tail /aws-glue/jobs/error --profile $PROFILE --since 15m"
            exit 1
            ;;
        STOPPED)
            echo "Job was stopped."
            exit 1
            ;;
        *)
            sleep 15
            ;;
    esac
done
