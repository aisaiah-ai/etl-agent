#!/usr/bin/env bash
# Clean up old test Glue jobs after the production pipeline is verified.
#
# Deletes:
#   - sf-moodle-read-test   (discovery/exploration job)
#   - sf-moodle-sync-test   (test sync job)
#
# Keeps:
#   - sf-moodle-sync        (production pipeline)
#   - materialize-qsreport  (QS report job)
#
# Prerequisites:
#   - Prod credentials active (source docs/aws_mfa_prod.sh)
#
# Usage:
#   bash scripts/cleanup_glue_test_jobs.sh           # Dry run (list only)
#   bash scripts/cleanup_glue_test_jobs.sh --delete   # Actually delete

set -euo pipefail

PROFILE="prod"
REGION="us-east-1"

TEST_JOBS=(
    "sf-moodle-read-test"
    "sf-moodle-sync-test"
)

MODE="${1:---dry-run}"

echo "=== Glue Test Job Cleanup ==="
echo ""

# List all current jobs
echo "Current Glue jobs:"
aws glue get-jobs \
    --profile "$PROFILE" \
    --region "$REGION" \
    --query 'Jobs[].{Name: Name, Created: CreatedOn, Modified: LastModifiedOn}' \
    --output table 2>/dev/null || echo "  (could not list jobs — check AWS credentials)"
echo ""

if [ "$MODE" = "--delete" ]; then
    for JOB in "${TEST_JOBS[@]}"; do
        echo "Deleting: $JOB ..."
        if aws glue delete-job --job-name "$JOB" --profile "$PROFILE" --region "$REGION" 2>/dev/null; then
            echo "  Deleted."
        else
            echo "  Not found or already deleted."
        fi
    done

    # Also clean up S3 scripts
    SCRIPT_BUCKET="aws-glue-assets-442594162630-${REGION}"
    for JOB in "${TEST_JOBS[@]}"; do
        echo "Removing S3 script: s3://${SCRIPT_BUCKET}/scripts/${JOB}.py ..."
        aws s3 rm "s3://${SCRIPT_BUCKET}/scripts/${JOB}.py" --profile "$PROFILE" 2>/dev/null || true
    done

    echo ""
    echo "Cleanup complete. Remaining jobs:"
    aws glue get-jobs \
        --profile "$PROFILE" \
        --region "$REGION" \
        --query 'Jobs[].Name' \
        --output table 2>/dev/null || true
else
    echo "DRY RUN — would delete:"
    for JOB in "${TEST_JOBS[@]}"; do
        echo "  - $JOB"
    done
    echo ""
    echo "Run with --delete to actually remove them."
fi
