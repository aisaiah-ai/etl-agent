#!/usr/bin/env bash
# bootstrap.sh — One-time setup for Terraform remote state backend.
# Creates the S3 bucket and DynamoDB table used for state locking.
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="etl-agent"
STATE_BUCKET="${PROJECT_NAME}-terraform-state"
LOCK_TABLE="${PROJECT_NAME}-terraform-locks"

echo "=== ETL Agent — Bootstrap Terraform Backend ==="
echo "Region       : ${AWS_REGION}"
echo "State bucket : ${STATE_BUCKET}"
echo "Lock table   : ${LOCK_TABLE}"
echo ""

# ── Create S3 bucket for Terraform state ─────────────────────────────────────
echo "[1/2] Creating S3 state bucket..."
if aws s3api head-bucket --bucket "${STATE_BUCKET}" 2>/dev/null; then
    echo "  Bucket ${STATE_BUCKET} already exists — skipping."
else
    if [[ "$AWS_REGION" == "us-east-1" ]]; then
        aws s3api create-bucket \
            --bucket "${STATE_BUCKET}" \
            --region "${AWS_REGION}"
    else
        aws s3api create-bucket \
            --bucket "${STATE_BUCKET}" \
            --region "${AWS_REGION}" \
            --create-bucket-configuration LocationConstraint="${AWS_REGION}"
    fi

    aws s3api put-bucket-versioning \
        --bucket "${STATE_BUCKET}" \
        --versioning-configuration Status=Enabled

    aws s3api put-bucket-encryption \
        --bucket "${STATE_BUCKET}" \
        --server-side-encryption-configuration \
        '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms"},"BucketKeyEnabled":true}]}'

    aws s3api put-public-access-block \
        --bucket "${STATE_BUCKET}" \
        --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

    echo "  Created and configured ${STATE_BUCKET}."
fi

# ── Create DynamoDB table for state locking ──────────────────────────────────
echo "[2/2] Creating DynamoDB lock table..."
if aws dynamodb describe-table --table-name "${LOCK_TABLE}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "  Table ${LOCK_TABLE} already exists — skipping."
else
    aws dynamodb create-table \
        --table-name "${LOCK_TABLE}" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region "${AWS_REGION}" \
        >/dev/null

    echo "  Created ${LOCK_TABLE}."
fi

echo ""
echo "=== Bootstrap Complete ==="
echo "Add this to your Terraform backend configuration:"
echo ""
echo '  terraform {'
echo '    backend "s3" {'
echo "      bucket         = \"${STATE_BUCKET}\""
echo "      key            = \"terraform.tfstate\""
echo "      region         = \"${AWS_REGION}\""
echo "      dynamodb_table = \"${LOCK_TABLE}\""
echo '      encrypt        = true'
echo '    }'
echo '  }'
