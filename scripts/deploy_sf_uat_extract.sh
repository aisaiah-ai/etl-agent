#!/usr/bin/env bash
# Deploy sf_uat_extract.py — extract SF UAT CourseOfferings with HCG_Format__c,
# register in Glue catalog, and match to Moodle Atlas ID.
#
# Prerequisites:
#   - Prod credentials active (source docs/aws_mfa_prod.sh)
#
# Usage:
#   bash scripts/deploy_sf_uat_extract.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PROFILE="prod"
REGION="us-east-1"
ACCOUNT_ID="442594162630"
JOB_NAME="sf-uat-extract"
SCRIPT_BUCKET="aws-glue-assets-${ACCOUNT_ID}-${REGION}"
SCRIPT_KEY="scripts/${JOB_NAME}.py"
LOCAL_SCRIPT="${PROJECT_ROOT}/pipelines/glue_jobs/sf_uat_extract.py"
OUTPUT_BUCKET="ies-devteam-prd"
OUTPUT_PREFIX="uat/sf_uat_extract/"
GLUE_DB="uat"

echo "=== SF UAT Extract + HCG_Format__c Atlas Match ==="
echo "  Job:      $JOB_NAME"
echo "  Bucket:   $OUTPUT_BUCKET"
echo "  Prefix:   $OUTPUT_PREFIX"
echo "  Glue DB:  $GLUE_DB"
echo ""

# Verify credentials
echo "Checking AWS credentials..."
CALLER=$(aws sts get-caller-identity --profile "$PROFILE" --query "Account" --output text 2>/dev/null) || {
    echo "ERROR: AWS credentials expired. Run: source docs/aws_mfa_prod.sh"
    exit 1
}
echo "  Account: $CALLER"

# ── Find Glue role ─────────────────────────────────────────────────────
echo ""
echo "Step 1: Looking up Glue job role..."
GLUE_ROLE=$(aws glue get-job \
    --job-name "POC_02_getting_sf_objects" \
    --profile "$PROFILE" \
    --query "Job.Role" \
    --output text 2>/dev/null || true)

if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    GLUE_ROLE=$(aws glue get-jobs \
        --profile "$PROFILE" \
        --max-results 1 \
        --query "Jobs[0].Role" \
        --output text 2>/dev/null || true)
fi

if [ -z "$GLUE_ROLE" ] || [ "$GLUE_ROLE" = "None" ]; then
    echo "ERROR: Could not find an existing Glue job role."
    printf "Role ARN: " && read -r GLUE_ROLE
fi
echo "  Role: $GLUE_ROLE"

# ── Upload script ──────────────────────────────────────────────────────
echo ""
echo "Step 2: Uploading script to s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}..."
aws s3 cp "$LOCAL_SCRIPT" "s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}" --profile "$PROFILE"
echo "  Done."

# ── Upload Moodle data (for matching) ──────────────────────────────────
echo ""
echo "Step 3: Uploading Moodle course cache for matching..."
MOODLE_JSON="${PROJECT_ROOT}/output/moodle_all_courses_cache.json"
if [ -f "$MOODLE_JSON" ]; then
    aws s3 cp "$MOODLE_JSON" "s3://${OUTPUT_BUCKET}/moodle_data/moodle_all_courses.json" --profile "$PROFILE"
    echo "  Uploaded $(python3 -c "import json; print(len(json.load(open('$MOODLE_JSON'))))" 2>/dev/null || echo '?') courses"
else
    echo "  WARNING: $MOODLE_JSON not found — matching step will try Glue catalog"
fi

# ── Create/update Glue job ─────────────────────────────────────────────
echo ""
echo "Step 4: Creating/updating Glue job: ${JOB_NAME}..."

EXISTING=$(aws glue get-job --job-name "$JOB_NAME" --profile "$PROFILE" 2>&1 > /dev/null && echo "yes" || echo "no")

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
    \"Connections\": {
        \"Connections\": [\"Sandbox Salesforce Connection\"]
    },
    \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--output_bucket\": \"${OUTPUT_BUCKET}\",
        \"--output_prefix\": \"${OUTPUT_PREFIX}\",
        \"--database\": \"${GLUE_DB}\"
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
        --number-of-workers 2 \
        --timeout 30 \
        --connections "{\"Connections\": [\"Sandbox Salesforce Connection\"]}" \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--enable-metrics\": \"true\",
            \"--output_bucket\": \"${OUTPUT_BUCKET}\",
            \"--output_prefix\": \"${OUTPUT_PREFIX}\",
            \"--database\": \"${GLUE_DB}\"
        }"
fi
echo "  Done."

# ── Start job run ──────────────────────────────────────────────────────
echo ""
echo "Step 5: Starting job run..."
RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --profile "$PROFILE" \
    --query "JobRunId" \
    --output text)

echo "  Job run started: $RUN_ID"

# ── Wait for completion ────────────────────────────────────────────────
echo ""
echo "Step 6: Waiting for job to complete..."
echo "  (Extracts SF UAT → S3 → Glue catalog → Atlas ID match)"
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
            echo "Results:"
            echo "  Parquet:       s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}sf_course_offerings_uat/"
            echo "  CSV:           s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}sf_course_offerings_uat_csv/"
            echo "  Atlas match:   s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}hcg_format_atlas_match/"
            echo "  Moodle report: s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}moodle_match_report/"
            echo ""
            echo "Download results:"
            echo "  aws s3 cp s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}hcg_format_atlas_match/ output/ --recursive --profile $PROFILE"
            echo "  aws s3 cp s3://${OUTPUT_BUCKET}/${OUTPUT_PREFIX}moodle_match_report/ output/ --recursive --profile $PROFILE"
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
            sleep 20
            ;;
    esac
done
