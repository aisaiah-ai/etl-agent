#!/usr/bin/env bash
# teardown.sh — Clean up local output directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${PROJECT_ROOT}/output"

if [[ -d "$OUTPUT_DIR" ]]; then
    rm -rf "$OUTPUT_DIR"
    echo "Removed ${OUTPUT_DIR}/"
else
    echo "Nothing to clean up — ${OUTPUT_DIR}/ does not exist."
fi
