#!/usr/bin/env bash
# Load SF course offerings and Moodle courses into dev Redshift (uat schema)
# for comparison/matching queries.
#
# Prerequisites: source docs/aws_mfa_dev.sh
#
# Usage: bash scripts/load_course_comparison.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

WORKGROUP="etl-agent-dev-wg"
DATABASE="etl_agent_db"
SCHEMA="uat"
REGION="us-east-1"

# Verify credentials
echo "Checking AWS credentials..."
aws sts get-caller-identity --query "Account" --output text || {
    echo "ERROR: AWS credentials expired. Run: source docs/aws_mfa_dev.sh"
    exit 1
}

run_sql() {
    local sql="$1"
    local desc="${2:-SQL}"
    echo "  Running: $desc"
    STMT_ID=$(aws redshift-data execute-statement \
        --workgroup-name "$WORKGROUP" \
        --database "$DATABASE" \
        --sql "$sql" \
        --region "$REGION" \
        --query "Id" \
        --output text)

    # Wait for completion
    while true; do
        STATUS=$(aws redshift-data describe-statement \
            --id "$STMT_ID" \
            --region "$REGION" \
            --query "Status" \
            --output text)
        case "$STATUS" in
            FINISHED) break ;;
            FAILED|ABORTED)
                ERROR=$(aws redshift-data describe-statement \
                    --id "$STMT_ID" \
                    --region "$REGION" \
                    --query "Error" \
                    --output text)
                echo "  FAILED: $ERROR"
                return 1
                ;;
            *) sleep 2 ;;
        esac
    done
    echo "  Done."
}

# ── Create schema ────────────────────────────────────────────────────────
echo ""
echo "=== Creating schema ${SCHEMA} ==="
run_sql "CREATE SCHEMA IF NOT EXISTS ${SCHEMA};" "Create schema"

# ── SF Course Offerings ──────────────────────────────────────────────────
echo ""
echo "=== Loading SF Course Offerings (sf_course_offerings) ==="
run_sql "DROP TABLE IF EXISTS ${SCHEMA}.sf_course_offerings;" "Drop old table"

run_sql "
CREATE TABLE ${SCHEMA}.sf_course_offerings (
    sf_id       VARCHAR(18)  NOT NULL,
    name        VARCHAR(500) NOT NULL
);
" "Create sf_course_offerings"

# Build INSERT batches from JSON (100 rows per INSERT)
echo "  Generating INSERT statements..."
BATCH_FILE=$(mktemp)
python3 -c "
import json, sys

offerings = json.load(open('${PROJECT_ROOT}/output/sf_all_course_offerings.json'))
batch_size = 100
for i in range(0, len(offerings), batch_size):
    batch = offerings[i:i+batch_size]
    values = []
    for o in batch:
        sf_id = o['Id'].replace(\"'\", \"''\")
        name = o['Name'].replace(\"'\", \"''\")
        values.append(f\"('{sf_id}', '{name}')\")
    sql = f\"INSERT INTO ${SCHEMA}.sf_course_offerings (sf_id, name) VALUES {', '.join(values)};\"
    print(sql)
" > "$BATCH_FILE"

TOTAL_BATCHES=$(wc -l < "$BATCH_FILE" | tr -d ' ')
echo "  ${TOTAL_BATCHES} batches to insert..."

BATCH_NUM=0
while IFS= read -r sql; do
    BATCH_NUM=$((BATCH_NUM + 1))
    run_sql "$sql" "SF batch ${BATCH_NUM}/${TOTAL_BATCHES}"
done < "$BATCH_FILE"
rm -f "$BATCH_FILE"

# ── Moodle Courses ───────────────────────────────────────────────────────
echo ""
echo "=== Loading Moodle Courses (moodle_courses) ==="
run_sql "DROP TABLE IF EXISTS ${SCHEMA}.moodle_courses;" "Drop old table"

run_sql "
CREATE TABLE ${SCHEMA}.moodle_courses (
    moodle_id   INT          NOT NULL,
    shortname   VARCHAR(500) NOT NULL,
    fullname    VARCHAR(500) NOT NULL,
    idnumber    VARCHAR(100)
);
" "Create moodle_courses"

# Build INSERT batches from JSON (100 rows per INSERT)
echo "  Generating INSERT statements..."
BATCH_FILE=$(mktemp)
python3 -c "
import json, sys

courses = json.load(open('${PROJECT_ROOT}/output/moodle_all_courses_cache.json'))
batch_size = 100
for i in range(0, len(courses), batch_size):
    batch = courses[i:i+batch_size]
    values = []
    for c in batch:
        moodle_id = c['id']
        shortname = c.get('shortname', '').replace(\"'\", \"''\")
        fullname = c.get('fullname', '').replace(\"'\", \"''\")
        idnumber = c.get('idnumber', '').replace(\"'\", \"''\")
        values.append(f\"({moodle_id}, '{shortname}', '{fullname}', '{idnumber}')\")
    sql = f\"INSERT INTO ${SCHEMA}.moodle_courses (moodle_id, shortname, fullname, idnumber) VALUES {', '.join(values)};\"
    print(sql)
" > "$BATCH_FILE"

TOTAL_BATCHES=$(wc -l < "$BATCH_FILE" | tr -d ' ')
echo "  ${TOTAL_BATCHES} batches to insert..."

BATCH_NUM=0
while IFS= read -r sql; do
    BATCH_NUM=$((BATCH_NUM + 1))
    if (( BATCH_NUM % 25 == 0 )); then
        echo "  Moodle batch ${BATCH_NUM}/${TOTAL_BATCHES}..."
    fi
    run_sql "$sql" "Moodle batch ${BATCH_NUM}/${TOTAL_BATCHES}" > /dev/null 2>&1 || {
        echo "  FAILED at batch ${BATCH_NUM}"
        # Re-run with output for debugging
        run_sql "$sql" "Moodle batch ${BATCH_NUM}/${TOTAL_BATCHES} (retry)"
    }
done < "$BATCH_FILE"
rm -f "$BATCH_FILE"

# ── Verify ───────────────────────────────────────────────────────────────
echo ""
echo "=== Verifying ==="
run_sql "SELECT COUNT(*) FROM ${SCHEMA}.sf_course_offerings;" "Count SF offerings"
aws redshift-data get-statement-result --id "$STMT_ID" --region "$REGION" \
    --query "Records[0][0].longValue" --output text | xargs -I{} echo "  sf_course_offerings: {} rows"

run_sql "SELECT COUNT(*) FROM ${SCHEMA}.moodle_courses;" "Count Moodle courses"
aws redshift-data get-statement-result --id "$STMT_ID" --region "$REGION" \
    --query "Records[0][0].longValue" --output text | xargs -I{} echo "  moodle_courses: {} rows"

echo ""
echo "=== Done! ==="
echo ""
echo "Sample queries:"
echo "  -- All SF offerings"
echo "  SELECT * FROM uat.sf_course_offerings LIMIT 10;"
echo ""
echo "  -- All Moodle courses"
echo "  SELECT * FROM uat.moodle_courses LIMIT 10;"
echo ""
echo "  -- Fuzzy comparison (exact name match)"
echo "  SELECT s.sf_id, s.name AS sf_name, m.moodle_id, m.shortname, m.fullname"
echo "  FROM uat.sf_course_offerings s"
echo "  JOIN uat.moodle_courses m ON LOWER(s.name) = LOWER(m.shortname)"
echo "     OR LOWER(s.name) = LOWER(m.fullname);"
