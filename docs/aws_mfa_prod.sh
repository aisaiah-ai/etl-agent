#!/bin/bash
# AWS MFA Session Token Helper — PROD (442594162630)
# Usage:  source docs/aws_mfa_prod.sh
#
# Prerequisites:
#   - brew install jq awscli
#   - ~/.aws/credentials must have a [master-profile-prod] profile with your
#     long-lived access key/secret key for the prod account
#
# Writes temporary credentials to the [prod] profile AND exports them.

# NOTE: Do NOT use "set -euo pipefail" — when sourced, it kills the parent shell on any error.

# --- Configuration ---
_MFA_ARN="arn:aws:iam::442594162630:mfa/prod_amdpmx14_agent"
_SOURCE_PROFILE="master-profile-prod"
_TARGET_PROFILE="prod"
_DURATION="129600"

# --- Prompt for MFA token ---
printf "MFA token (prod): "
read -r _mfa_token

if [[ -z "$_mfa_token" ]]; then
    echo "ERROR: MFA token cannot be empty." >&2
    return 1 2>/dev/null || exit 1
fi

# --- Get session token ---
echo "Requesting session token (profile: $_SOURCE_PROFILE, account: 442594162630)..."

_sts_response=$(aws sts get-session-token \
    --profile "$_SOURCE_PROFILE" \
    --duration-seconds "$_DURATION" \
    --serial-number "$_MFA_ARN" \
    --token-code "$_mfa_token" 2>&1)

if ! echo "$_sts_response" | jq -e .Credentials > /dev/null 2>&1; then
    echo "ERROR: Failed to get session token." >&2
    echo "$_sts_response" >&2
    return 1 2>/dev/null || exit 1
fi

# --- Parse credentials ---
AWS_ACCESS_KEY_ID=$(echo "$_sts_response" | jq -r '.Credentials.AccessKeyId')
AWS_SECRET_ACCESS_KEY=$(echo "$_sts_response" | jq -r '.Credentials.SecretAccessKey')
AWS_SESSION_TOKEN=$(echo "$_sts_response" | jq -r '.Credentials.SessionToken')
_EXPIRATION=$(echo "$_sts_response" | jq -r '.Credentials.Expiration')

# --- Export for current shell ---
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

# --- Write to [prod] profile (does NOT overwrite [default]) ---
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID" --profile "$_TARGET_PROFILE"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY" --profile "$_TARGET_PROFILE"
aws configure set aws_session_token "$AWS_SESSION_TOKEN" --profile "$_TARGET_PROFILE"
aws configure set region "us-east-1" --profile "$_TARGET_PROFILE"

# --- Summary ---
echo ""
echo "AWS MFA session activated (PROD)."
echo "  Account:       442594162630"
echo "  Access Key ID: ${AWS_ACCESS_KEY_ID:0:8}...${AWS_ACCESS_KEY_ID: -4}"
echo "  Expires:       $_EXPIRATION"
echo "  Profile:       $_TARGET_PROFILE (written to ~/.aws/credentials)"
echo ""
echo "Usage: aws redshift describe-clusters --profile prod"
echo "   or: export AWS_PROFILE=prod"

# --- Cleanup temp vars ---
unset _MFA_ARN _SOURCE_PROFILE _TARGET_PROFILE _DURATION _mfa_token _sts_response _EXPIRATION
