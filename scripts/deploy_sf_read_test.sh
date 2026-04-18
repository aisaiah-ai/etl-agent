#!/usr/bin/env bash
# Deploy sf_read_test.py as a Glue job in the prod account (442594162630)
# where the Salesforce connection exists.
#
# Now also audits Moodle sandbox data (users + courses with idnumber status).
#
# Prerequisites:
#   - Prod credentials active (source docs/aws_mfa_prod.sh)
#
# Usage:
#   bash scripts/deploy_sf_read_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PROFILE="prod"
REGION="us-east-1"
ACCOUNT_ID="442594162630"
JOB_NAME="sf-moodle-read-test"
SCRIPT_BUCKET="aws-glue-assets-${ACCOUNT_ID}-${REGION}"
SCRIPT_KEY="scripts/${JOB_NAME}.py"
LOCAL_SCRIPT="${PROJECT_ROOT}/pipelines/sf_moodle_sync/glue_jobs/sf_read_test.py"

# Moodle sandbox config
MOODLE_URL="https://ies-sbox.unhosting.site"
MOODLE_TOKEN="${MOODLE_TOKEN:-41c6b3ad321ff6a69f04814d66362a3b}"

echo "=== SF + Moodle Discovery Test ==="
echo "  Moodle: $MOODLE_URL"
echo "  SF:     Sandbox Salesforce Connection"
echo ""

# Find the Glue service role used by existing jobs
echo "Looking up existing Glue job role..."
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

echo "Using role: $GLUE_ROLE"

# Step 1: Upload script to S3
echo ""
echo "Step 1: Uploading script to s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}..."
aws s3 cp "$LOCAL_SCRIPT" "s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}" --profile "$PROFILE"
echo "  Done."

# Step 2: Create or update the Glue job
echo ""
echo "Step 2: Creating Glue job: ${JOB_NAME}..."

# Check if job already exists
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
    \"NumberOfWorkers\": 2,
    \"Timeout\": 30,
    \"Connections\": {
        \"Connections\": [\"Sandbox Salesforce Connection\"]
    },
    \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--additional-python-modules\": \"requests\",
        \"--moodle_url\": \"${MOODLE_URL}\",
        \"--moodle_token\": \"${MOODLE_TOKEN}\"
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
            \"--additional-python-modules\": \"requests\",
            \"--moodle_url\": \"${MOODLE_URL}\",
            \"--moodle_token\": \"${MOODLE_TOKEN}\"
        }"
fi
echo "  Done."

# Step 3: Start the job
echo ""
echo "Step 3: Starting job run..."
RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --profile "$PROFILE" \
    --query "JobRunId" \
    --output text)

echo "  Job run started: $RUN_ID"

# Step 4: Wait for completion
echo ""
echo "Step 4: Waiting for job to complete..."
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name "$JOB_NAME" \
        --run-id "$RUN_ID" \
        --profile "$PROFILE" \
        --query "JobRun.JobRunState" \
        --output text 2>/dev/null)

    echo "  Status: $STATUS"

    case "$STATUS" in
        SUCCEEDED)
            echo ""
            echo "Job completed successfully!"
            echo ""
            echo "View logs:"
            echo "  aws logs tail /aws-glue/jobs/output --profile $PROFILE --since 10m"
            break
            ;;
        FAILED|ERROR|TIMEOUT)
            echo ""
            echo "Job failed!"
            ERROR_MSG=$(aws glue get-job-run \
                --job-name "$JOB_NAME" \
                --run-id "$RUN_ID" \
                --profile "$PROFILE" \
                --query "JobRun.ErrorMessage" \
                --output text 2>/dev/null)
            echo "  Error: $ERROR_MSG"
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
