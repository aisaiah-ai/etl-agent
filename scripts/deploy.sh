#!/usr/bin/env bash
# deploy.sh — Package and deploy ETL Agent code to an AWS environment.
# Usage: ./scripts/deploy.sh <dev|prod>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

ENV="${1:-}"
if [[ -z "$ENV" ]] || [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
    echo "Usage: $0 <dev|prod>"
    exit 1
fi

echo "=== ETL Agent — Deploy to ${ENV} ==="

# ── Configuration ────────────────────────────────────────────────────────────
ARTIFACTS_BUCKET="${ETL_ARTIFACTS_BUCKET:-etl-agent-artifacts-${ENV}}"
ARTIFACT_VERSION="$(git -C "${PROJECT_ROOT}" rev-parse --short HEAD 2>/dev/null || echo 'local')-$(date +%Y%m%d%H%M%S)"

echo "Artifacts bucket : ${ARTIFACTS_BUCKET}"
echo "Artifact version : ${ARTIFACT_VERSION}"
echo ""

# ── Package code ─────────────────────────────────────────────────────────────
echo "[1/3] Packaging code..."
WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

cd "${PROJECT_ROOT}"
zip -r "${WORK_DIR}/pipelines.zip" pipelines/ -x '*.pyc' '*__pycache__/*' '*.DS_Store' >/dev/null
zip -r "${WORK_DIR}/agents.zip" agents/ -x '*.pyc' '*__pycache__/*' '*.DS_Store' >/dev/null
echo "  Packaged pipelines.zip and agents.zip"

# ── Upload to S3 ─────────────────────────────────────────────────────────────
echo "[2/3] Uploading to s3://${ARTIFACTS_BUCKET}/${ENV}/..."
aws s3 cp "${WORK_DIR}/pipelines.zip" \
    "s3://${ARTIFACTS_BUCKET}/${ENV}/pipelines/${ARTIFACT_VERSION}/pipelines.zip"
aws s3 cp "${WORK_DIR}/agents.zip" \
    "s3://${ARTIFACTS_BUCKET}/${ENV}/agents/${ARTIFACT_VERSION}/agents.zip"
echo "${ARTIFACT_VERSION}" | aws s3 cp - \
    "s3://${ARTIFACTS_BUCKET}/${ENV}/latest-version.txt"

# ── Update Glue jobs ─────────────────────────────────────────────────────────
echo "[3/3] Updating Glue jobs..."
SCRIPT_LOCATION="s3://${ARTIFACTS_BUCKET}/${ENV}/pipelines/${ARTIFACT_VERSION}/pipelines.zip"

# List existing Glue jobs with the project prefix and update each one
GLUE_JOBS=$(aws glue get-jobs --query "Jobs[?starts_with(Name, 'etl-agent-')].Name" --output text 2>/dev/null || true)
if [[ -n "$GLUE_JOBS" ]]; then
    for JOB_NAME in $GLUE_JOBS; do
        echo "  Updating ${JOB_NAME}..."
        aws glue update-job \
            --job-name "${JOB_NAME}" \
            --job-update "{\"Command\":{\"ScriptLocation\":\"${SCRIPT_LOCATION}\"}}" \
            >/dev/null
    done
else
    echo "  No existing Glue jobs found with 'etl-agent-' prefix."
fi

echo ""
echo "=== Deployment Complete ==="
echo "Version: ${ARTIFACT_VERSION}"
echo "Env:     ${ENV}"
