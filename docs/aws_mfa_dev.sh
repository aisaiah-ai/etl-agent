#!/bin/bash
# AWS MFA Session Token Helper — DEV (533267300255)
# Usage:  source docs/aws_mfa_dev.sh
#
# Prerequisites:
#   - brew install jq awscli
#   - ~/.aws/credentials must have a [master-profile-dev] profile with your
#     long-lived access key/secret key
#
# Writes temporary credentials to the [default] profile AND exports them.

# NOTE: Do NOT use "set -euo pipefail" — when sourced, it kills the parent shell on any error.

# --- Configuration ---
_MFA_ARN="arn:aws:iam::533267300255:mfa/dev_amdpmx14"
_SOURCE_PROFILE="master-profile-dev"
_DURATION="129600"

# --- Prompt for MFA token ---
printf "MFA token (dev): "
read -r _mfa_token

if [[ -z "$_mfa_token" ]]; then
    echo "ERROR: MFA token cannot be empty." >&2
    return 1 2>/dev/null || exit 1
fi

# --- Get session token ---
echo "Requesting session token (profile: $_SOURCE_PROFILE, duration: ${_DURATION}s)..."

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

# --- Write to [default] profile for CLI commands in other terminals ---
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
aws configure set aws_session_token "$AWS_SESSION_TOKEN"

# --- Summary (no secrets printed) ---
echo ""
echo "AWS MFA session activated (DEV)."
echo "  Access Key ID: ${AWS_ACCESS_KEY_ID:0:8}...${AWS_ACCESS_KEY_ID: -4}"
echo "  Expires:       $_EXPIRATION"
echo "  Profile:       default (written to ~/.aws/credentials)"
echo ""
echo "Tip: run 'aws sts get-caller-identity' to verify."

# --- Cleanup temp vars ---
unset _MFA_ARN _SOURCE_PROFILE _DURATION _mfa_token _sts_response _EXPIRATION
