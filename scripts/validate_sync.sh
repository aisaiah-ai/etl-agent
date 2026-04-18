#!/usr/bin/env bash
# Validate SF→Moodle sync results.
#
# Captures Moodle state and compares against SF source data to produce
# a pass/fail report showing: SF Data → Moodle Before → Moodle After.
#
# Usage:
#   # Full validation (capture before, run sync, capture after, report)
#   bash scripts/validate_sync.sh
#
#   # Just capture current state + compare against SF data
#   bash scripts/validate_sync.sh --check-only
#
# Prerequisites:
#   - MOODLE_URL and MOODLE_TOKEN env vars (or defaults to sandbox)
#   - SF source data in output/ (sf_accounts.json, sf_courses.json, etc.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

export MOODLE_URL="${MOODLE_URL:-https://ies-sbox.unhosting.site}"
export MOODLE_TOKEN="${MOODLE_TOKEN:-41c6b3ad321ff6a69f04814d66362a3b}"

TIMESTAMP=$(date +%Y-%m-%d_%H%M%S)
OUTPUT_DIR="output/validation_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

MODE="${1:-full}"

echo "=== SF→Moodle Sync Validation ==="
echo "  Moodle:  $MOODLE_URL"
echo "  Output:  $OUTPUT_DIR"
echo "  Mode:    $MODE"
echo ""

# ── Capture Moodle state ─────────────────────────────────────────────────

capture_state() {
    local label="$1"
    local outfile="${OUTPUT_DIR}/${label}.json"

    echo "Capturing Moodle state ($label)..."

    python3 -c "
import json, sys
sys.path.insert(0, '.')
from pipelines.sf_moodle_sync.moodle_client import MoodleClient

moodle = MoodleClient()

# Users
user_ids = [1396920, 1396921, 1396922, 1396923, 1396924, 1396925, 1396751]
users = []
for uid in user_ids:
    result = moodle.get_users_by_field(field='id', values=[str(uid)])
    if result:
        u = result[0]
        users.append({
            'id': u['id'],
            'username': u.get('username', ''),
            'name': f\"{u.get('firstname', '')} {u.get('lastname', '')}\",
            'email': u.get('email', ''),
            'idnumber': u.get('idnumber', ''),
        })

# Courses
course_ids = [68412, 69213, 69214, 69215, 69216]
courses = []
for cid in course_ids:
    result = moodle.search_courses_by_field('id', str(cid))
    if result:
        c = result[0]
        courses.append({
            'id': c['id'],
            'shortname': c.get('shortname', ''),
            'fullname': c.get('fullname', ''),
            'idnumber': c.get('idnumber', ''),
        })

state = {'users': users, 'courses': courses}
with open('$outfile', 'w') as f:
    json.dump(state, f, indent=2)
print(f'  Saved to $outfile')
" 2>&1
}

# ── Generate report ──────────────────────────────────────────────────────

generate_report() {
    echo "Generating validation report..."

    python3 -c "
import json

sf_accounts_file = 'output/sync_test_2026_03_16/sf_accounts.json'
sf_courses_file = 'output/sync_test_2026_03_16_retest/sf_courses_full.json'
before_file = '${OUTPUT_DIR}/before.json'
after_file = '${OUTPUT_DIR}/after.json'
report_file = '${OUTPUT_DIR}/REPORT.md'

# Load data
try:
    sf_accounts = json.load(open(sf_accounts_file))
except FileNotFoundError:
    sf_accounts = []

try:
    sf_courses = json.load(open(sf_courses_file))
except FileNotFoundError:
    sf_courses = []

try:
    before = json.load(open(before_file))
except FileNotFoundError:
    before = {'users': [], 'courses': []}

after = json.load(open(after_file))

# Index
before_users = {u['id']: u for u in before.get('users', [])}
after_users = {u['id']: u for u in after.get('users', [])}
before_courses = {c['id']: c for c in before.get('courses', [])}
after_courses = {c['id']: c for c in after.get('courses', [])}

# Build SF lookups
sf_by_email = {}
for a in sf_accounts:
    email = (a.get('PersonEmail') or a.get('Email') or '').strip().lower()
    if email:
        sf_by_email[email] = a

sf_by_course_name = {}
for c in sf_courses:
    sf_by_course_name[c['Name'].lower().strip()] = c

lines = []
lines.append('# SF→Moodle Sync Validation Report')
lines.append('')
lines.append(f'Generated: \$(date)')
lines.append(f'Moodle: ${MOODLE_URL}')
lines.append('')

# ── User validation ──
lines.append('## Users')
lines.append('')
lines.append('| Moodle ID | User | SF AccountId | Before | After | Result |')
lines.append('|---|---|---|---|---|---|')

user_pass = 0
user_fail = 0
user_total = 0

for uid in sorted(after_users.keys()):
    au = after_users[uid]
    bu = before_users.get(uid, {})
    email = au.get('email', '').lower()
    sf = sf_by_email.get(email, {})
    sf_id = sf.get('Id', '')
    before_idn = bu.get('idnumber', '(unknown)')
    after_idn = au.get('idnumber', '')

    if not sf_id:
        # Not in SF test data
        continue

    user_total += 1
    if after_idn == sf_id:
        result = 'PASS'
        user_pass += 1
    elif not sf_id:
        result = 'N/A'
    else:
        result = 'FAIL'
        user_fail += 1

    before_display = before_idn if before_idn else '(empty)'
    after_display = after_idn if after_idn else '(empty)'

    lines.append(f'| {uid} | {au[\"name\"]} | {sf_id} | {before_display} | {after_display} | **{result}** |')

lines.append(f'')
lines.append(f'**Users: {user_pass}/{user_total} PASS**')
lines.append('')

# ── Course validation ──
lines.append('## Courses')
lines.append('')
lines.append('| Moodle ID | Course | SF OfferingId | Before | After | Result |')
lines.append('|---|---|---|---|---|---|')

course_pass = 0
course_fail = 0
course_total = 0

for cid in sorted(after_courses.keys()):
    ac = after_courses[cid]
    bc = before_courses.get(cid, {})
    shortname = ac.get('shortname', '')
    sf = sf_by_course_name.get(shortname.lower().strip(), {})
    sf_id = sf.get('Id', '')
    before_idn = bc.get('idnumber', '(unknown)')
    after_idn = ac.get('idnumber', '')

    if not sf_id:
        # Not in SF test data — show but don't score
        before_display = before_idn if before_idn else '(empty)'
        after_display = after_idn if after_idn else '(empty)'
        lines.append(f'| {cid} | {shortname} | (not in SF data) | {before_display} | {after_display} | N/A |')
        continue

    course_total += 1
    # Special case: duplicate idnumber (expected to fail for 69213)
    if after_idn == sf_id:
        result = 'PASS'
        course_pass += 1
    else:
        result = 'FAIL'
        course_fail += 1

    before_display = before_idn if before_idn else '(empty)'
    after_display = after_idn if after_idn else '(empty)'
    lines.append(f'| {cid} | {shortname} | {sf_id} | {before_display} | {after_display} | **{result}** |')

lines.append(f'')
lines.append(f'**Courses: {course_pass}/{course_total} PASS**')
lines.append('')

# ── Summary ──
total_pass = user_pass + course_pass
total = user_total + course_total
lines.append('## Summary')
lines.append('')
lines.append(f'| Component | Pass | Total | Result |')
lines.append(f'|---|---|---|---|')
lines.append(f'| Users | {user_pass} | {user_total} | {\"PASS\" if user_fail == 0 else \"FAIL\"} |')
lines.append(f'| Courses | {course_pass} | {course_total} | {\"PASS\" if course_fail == 0 else \"FAIL\"} |')
lines.append(f'| **Total** | **{total_pass}** | **{total}** | **{\"PASS\" if user_fail + course_fail == 0 else \"FAIL\"}** |')

report = '\n'.join(lines)
with open(report_file, 'w') as f:
    f.write(report)

print(report)
print(f'\nReport saved to {report_file}')
" 2>&1
}

# ── Main ──────────────────────────────────────────────────────────────────

case "$MODE" in
    full)
        # 1. Capture before
        capture_state "before"

        # 2. Run sync
        echo ""
        echo "Running sync (users + courses, with force)..."
        python3 -m pipelines.sf_moodle_sync.verify_sync sync users \
            --sf-accounts output/sync_test_2026_03_16/sf_accounts.json --force \
            --output-dir "$OUTPUT_DIR" 2>&1 | tail -5
        python3 -m pipelines.sf_moodle_sync.verify_sync sync courses \
            --sf-courses output/sync_test_2026_03_16_retest/sf_courses_full.json --force \
            --output-dir "$OUTPUT_DIR" 2>&1 | tail -5
        echo ""

        # 3. Capture after
        capture_state "after"

        # 4. Report
        echo ""
        generate_report
        ;;

    --check-only)
        capture_state "after"
        echo ""
        generate_report
        ;;

    *)
        echo "Usage: bash scripts/validate_sync.sh [full|--check-only]"
        exit 1
        ;;
esac
