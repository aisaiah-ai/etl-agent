#!/usr/bin/env bash
# Deploy course_comparison.py as a Glue job in the dev account.
#
# Uploads SF + Moodle JSON data to S3, deploys Glue job, and runs it.
# Reads cached data (no live SF/Moodle connectivity needed).
#
# Prerequisites:
#   - Dev credentials active (source docs/aws_mfa_dev.sh)
#
# Usage:
#   bash scripts/deploy_course_comparison.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

REGION="us-east-1"
ACCOUNT_ID="533267300255"
JOB_NAME="course-comparison"
DATA_BUCKET="etl-agent-data-dev"
SCRIPT_BUCKET="aws-glue-assets-${ACCOUNT_ID}-${REGION}"
SCRIPT_KEY="scripts/${JOB_NAME}.py"
LOCAL_SCRIPT="${PROJECT_ROOT}/pipelines/glue_jobs/course_comparison.py"
DATA_PREFIX="course_comparison/"
OUTPUT_PREFIX="course_comparison/results/"

SF_JSON="${PROJECT_ROOT}/output/sf_all_course_offerings.json"
MOODLE_JSON="${PROJECT_ROOT}/output/moodle_all_courses_cache.json"

echo "=== Course Comparison (Glue Dev) ==="
echo "  Job:          $JOB_NAME"
echo "  Data bucket:  $DATA_BUCKET"
echo "  SF offerings: $(python3 -c "import json; print(len(json.load(open('$SF_JSON'))))")"
echo "  Moodle courses: $(python3 -c "import json; print(len(json.load(open('$MOODLE_JSON'))))")"
echo ""

# Verify credentials
echo "Checking AWS credentials..."
aws sts get-caller-identity --query "Account" --output text || {
    echo "ERROR: AWS credentials expired. Run: source docs/aws_mfa_dev.sh"
    exit 1
}

# ── Upload data to S3 ───────────────────────────────────────────────────
echo ""
echo "Step 1: Uploading data to S3..."
aws s3 cp "$SF_JSON" "s3://${DATA_BUCKET}/${DATA_PREFIX}sf_all_course_offerings.json"
aws s3 cp "$MOODLE_JSON" "s3://${DATA_BUCKET}/${DATA_PREFIX}moodle_all_courses_cache.json"
echo "  Done."

# ── Upload script to S3 ─────────────────────────────────────────────────
echo ""
echo "Step 2: Uploading Glue script..."

# Ensure the assets bucket exists
if ! aws s3 ls "s3://${SCRIPT_BUCKET}" > /dev/null 2>&1; then
    echo "  Creating script bucket: ${SCRIPT_BUCKET}"
    aws s3 mb "s3://${SCRIPT_BUCKET}" --region "$REGION"
fi

aws s3 cp "$LOCAL_SCRIPT" "s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}"
echo "  Done."

# ── Find Glue role ───────────────────────────────────────────────────────
echo ""
echo "Step 3: Looking up Glue job role..."
GLUE_ROLE=$(aws glue get-jobs \
    --max-results 5 \
    --query "Jobs[0].Role" \
    --output text 2>/dev/null || true)

if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    # Try IAM role search
    GLUE_ROLE=$(aws iam list-roles \
        --query "Roles[?contains(RoleName, 'Glue') || contains(RoleName, 'glue')].Arn | [0]" \
        --output text 2>/dev/null || true)
fi

if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    echo "ERROR: Could not find a Glue service role."
    printf "Role ARN: " && read -r GLUE_ROLE
fi
echo "  Role: $GLUE_ROLE"

# ── Create/update Glue job ──────────────────────────────────────────────
echo ""
echo "Step 4: Creating/updating Glue job: ${JOB_NAME}..."

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
    \"Timeout\": 60,
    \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--data_bucket\": \"${DATA_BUCKET}\",
        \"--data_prefix\": \"${DATA_PREFIX}\",
        \"--output_prefix\": \"${OUTPUT_PREFIX}\"
    }
}"

if [ "$EXISTING" = "yes" ]; then
    echo "  Job exists, updating..."
    aws glue update-job \
        --job-name "$JOB_NAME" \
        --job-update "$JOB_CONFIG"
else
    echo "  Creating new job..."
    aws glue create-job \
        --name "$JOB_NAME" \
        --role "$GLUE_ROLE" \
        --command "{
            \"Name\": \"glueetl\",
            \"ScriptLocation\": \"s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}\",
            \"PythonVersion\": \"3\"
        }" \
        --glue-version "4.0" \
        --worker-type "G.1X" \
        --number-of-workers 2 \
        --timeout 60 \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--enable-metrics\": \"true\",
            \"--data_bucket\": \"${DATA_BUCKET}\",
            \"--data_prefix\": \"${DATA_PREFIX}\",
            \"--output_prefix\": \"${OUTPUT_PREFIX}\"
        }"
fi
echo "  Done."

# ── Start job run ────────────────────────────────────────────────────────
echo ""
echo "Step 5: Starting job run..."
RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --query "JobRunId" \
    --output text)

echo "  Job run started: $RUN_ID"

# ── Wait for completion ─────────────────────────────────────────────────
echo ""
echo "Step 6: Waiting for job to complete..."
echo "  (1790 SF × 28524 Moodle — may take a while)"
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name "$JOB_NAME" \
        --run-id "$RUN_ID" \
        --query "JobRun.JobRunState" \
        --output text 2>/dev/null)

    echo "  $(date +%H:%M:%S) Status: $STATUS"

    case "$STATUS" in
        SUCCEEDED)
            echo ""
            echo "Job completed successfully!"
            echo ""
            echo "Results at:"
            echo "  aws s3 ls s3://${DATA_BUCKET}/${OUTPUT_PREFIX} --recursive"
            echo ""
            echo "Download CSV:"
            echo "  aws s3 cp s3://${DATA_BUCKET}/${OUTPUT_PREFIX} output/glue_comparison/ --recursive"
            echo ""
            echo "View logs:"
            echo "  aws logs tail /aws-glue/jobs/output --since 30m"
            break
            ;;
        FAILED|ERROR|TIMEOUT)
            echo ""
            echo "Job FAILED!"
            ERROR_MSG=$(aws glue get-job-run \
                --job-name "$JOB_NAME" \
                --run-id "$RUN_ID" \
                --query "JobRun.ErrorMessage" \
                --output text 2>/dev/null)
            echo "  Error: $ERROR_MSG"
            echo ""
            echo "Check logs:"
            echo "  aws logs tail /aws-glue/jobs/error --since 30m"
            exit 1
            ;;
        STOPPED)
            echo "Job was stopped."
            exit 1
            ;;
        *)
            sleep 30
            ;;
    esac
done
