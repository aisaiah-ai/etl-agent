#!/usr/bin/env bash
# Deploy sync_test_job.py as a Glue job in the prod account (442594162630)
# where the Salesforce connection exists.
#
# This job runs: AUDIT → SYNC → VERIFY against real SF + Moodle sandbox.
#
# Prerequisites:
#   - Prod credentials active (source docs/aws_mfa_prod.sh)
#
# Usage:
#   # Deploy + run in audit mode (read-only, safe)
#   bash scripts/deploy_sync_test.sh
#
#   # Deploy + run in sync mode (will update Moodle)
#   bash scripts/deploy_sync_test.sh sync
#
#   # Deploy + run sync with force (overwrite conflicts)
#   bash scripts/deploy_sync_test.sh sync --force

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PROFILE="prod"
REGION="us-east-1"
ACCOUNT_ID="442594162630"
JOB_NAME="sf-moodle-sync-test"
SCRIPT_BUCKET="aws-glue-assets-${ACCOUNT_ID}-${REGION}"
SCRIPT_KEY="scripts/${JOB_NAME}.py"
LOCAL_SCRIPT="${PROJECT_ROOT}/pipelines/sf_moodle_sync/glue_jobs/sync_test_job.py"

# Configurable params
MODE="${1:-audit}"  # audit | sync | verify
FORCE="false"
if [ "$2" = "--force" ]; then
    FORCE="true"
fi

# Moodle sandbox config
MOODLE_URL="https://ies-sbox.unhosting.site"
MOODLE_TOKEN="${MOODLE_TOKEN:-41c6b3ad321ff6a69f04814d66362a3b}"
SF_CONNECTION="Sandbox Salesforce Connection"

echo "=== SF-Moodle Sync Test Deployment ==="
echo "  Mode:   $MODE"
echo "  Force:  $FORCE"
echo "  Moodle: $MOODLE_URL"
echo "  SF:     $SF_CONNECTION"
echo ""

# ── Bundle extra-py-files ─────────────────────────────────────────────────
# The sync job imports from pipelines.sf_moodle_sync.*
# We need to zip up the package and upload as --extra-py-files

EXTRA_PY_ZIP="/tmp/sf_moodle_sync_lib.zip"
echo "Step 0: Bundling pipelines.sf_moodle_sync as extra-py-files..."
cd "$PROJECT_ROOT"
zip -r "$EXTRA_PY_ZIP" \
    pipelines/__init__.py \
    pipelines/sf_moodle_sync/__init__.py \
    pipelines/sf_moodle_sync/moodle_client.py \
    pipelines/sf_moodle_sync/course_sync.py \
    pipelines/sf_moodle_sync/user_sync.py \
    pipelines/sf_moodle_sync/verify_sync.py \
    -x "*.pyc" "__pycache__/*"
echo "  Created $EXTRA_PY_ZIP"

aws s3 cp "$EXTRA_PY_ZIP" "s3://${SCRIPT_BUCKET}/extra-py-files/sf_moodle_sync_lib.zip" --profile "$PROFILE"
echo "  Uploaded to S3"

# ── Find Glue role ────────────────────────────────────────────────────────
echo ""
echo "Step 1: Looking up Glue job role..."
GLUE_ROLE=$(aws glue get-job \
    --job-name "sf-moodle-read-test" \
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

# ── Upload script ─────────────────────────────────────────────────────────
echo ""
echo "Step 2: Uploading script..."
aws s3 cp "$LOCAL_SCRIPT" "s3://${SCRIPT_BUCKET}/${SCRIPT_KEY}" --profile "$PROFILE"
echo "  Done."

# ── Create/update Glue job ────────────────────────────────────────────────
echo ""
echo "Step 3: Creating/updating Glue job: ${JOB_NAME}..."

EXTRA_PY_S3="s3://${SCRIPT_BUCKET}/extra-py-files/sf_moodle_sync_lib.zip"

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
        \"Connections\": [\"${SF_CONNECTION}\"]
    },
    \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--extra-py-files\": \"${EXTRA_PY_S3}\",
        \"--additional-python-modules\": \"requests\",
        \"--sf_connection_name\": \"${SF_CONNECTION}\",
        \"--moodle_url\": \"${MOODLE_URL}\",
        \"--moodle_token\": \"${MOODLE_TOKEN}\",
        \"--mode\": \"${MODE}\",
        \"--entity\": \"both\",
        \"--force\": \"${FORCE}\",
        \"--dry_run\": \"false\",
        \"--output_bucket\": \"none\"
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
        --connections "{\"Connections\": [\"${SF_CONNECTION}\"]}" \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--enable-metrics\": \"true\",
            \"--extra-py-files\": \"${EXTRA_PY_S3}\",
            \"--additional-python-modules\": \"requests\",
            \"--sf_connection_name\": \"${SF_CONNECTION}\",
            \"--moodle_url\": \"${MOODLE_URL}\",
            \"--moodle_token\": \"${MOODLE_TOKEN}\",
            \"--mode\": \"${MODE}\",
            \"--entity\": \"both\",
            \"--force\": \"${FORCE}\",
            \"--dry_run\": \"false\",
            \"--output_bucket\": \"none\"
        }"
fi
echo "  Done."

# ── Start job run (override mode/force from CLI) ─────────────────────────
echo ""
echo "Step 4: Starting job run (mode=${MODE}, force=${FORCE})..."
RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --profile "$PROFILE" \
    --arguments "{
        \"--mode\": \"${MODE}\",
        \"--force\": \"${FORCE}\"
    }" \
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

    echo "  Status: $STATUS"

    case "$STATUS" in
        SUCCEEDED)
            echo ""
            echo "Job completed successfully!"
            echo ""
            echo "View logs:"
            echo "  aws logs tail /aws-glue/jobs/output --profile $PROFILE --since 10m"
            echo ""
            echo "View errors (if any):"
            echo "  aws logs tail /aws-glue/jobs/error --profile $PROFILE --since 10m"
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
            echo "  aws logs tail /aws-glue/jobs/error --profile $PROFILE --since 10m"
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
