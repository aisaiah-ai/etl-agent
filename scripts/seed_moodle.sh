#!/usr/bin/env bash
# Seed Moodle sandbox with test data for SF→Moodle sync testing.
#
# Creates test users and courses in Moodle with known initial states
# (empty, pre-set, conflict) to exercise all sync scenarios.
#
# Usage:
#   bash scripts/seed_moodle.sh           # Seed sandbox
#   bash scripts/seed_moodle.sh --reset   # Reset to original seed state
#   bash scripts/seed_moodle.sh --status  # Show current state

set -euo pipefail

MOODLE_URL="${MOODLE_URL:-https://ies-sbox.unhosting.site}"
MOODLE_TOKEN="${MOODLE_TOKEN:-41c6b3ad321ff6a69f04814d66362a3b}"
API="${MOODLE_URL}/webservice/rest/server.php"

call() {
    curl -k -L -s "$API" -d "wstoken=${MOODLE_TOKEN}&moodlewsrestformat=json&$1"
}

MODE="${1:---status}"

# ── Known test user/course IDs ────────────────────────────────────────────
# These IDs are assigned by Moodle on creation. If the sandbox is reset,
# you'll need to re-run --seed and update these IDs.

USER_IDS=(1396920 1396921 1396922 1396923 1396924 1396925 1396751)
COURSE_IDS=(69213 69214 69215 69216 68412)

# ── Show current state ────────────────────────────────────────────────────

show_status() {
    echo "=== Moodle Sandbox State ==="
    echo ""
    echo "--- Users ---"
    local user_params=""
    for i in "${!USER_IDS[@]}"; do
        user_params="${user_params}&values[${i}]=${USER_IDS[$i]}"
    done
    call "wsfunction=core_user_get_users_by_field&field=id${user_params}" | python3 -c "
import sys, json
users = json.load(sys.stdin)
print(f'  {\"ID\":>10s}  {\"Username\":30s}  {\"Name\":25s}  {\"idnumber\"}')
print(f'  {\"---\":>10s}  {\"---\":30s}  {\"---\":25s}  {\"---\"}')
for u in sorted(users, key=lambda x: x['id']):
    idn = u.get('idnumber','') or '(empty)'
    name = f\"{u['firstname']} {u['lastname']}\"
    print(f'  {u[\"id\"]:>10d}  {u[\"username\"]:30s}  {name:25s}  {idn}')
"
    echo ""
    echo "--- Courses ---"
    for ID in "${COURSE_IDS[@]}"; do
        call "wsfunction=core_course_get_courses_by_field&field=id&value=$ID" | python3 -c "
import sys, json
c = json.load(sys.stdin)['courses'][0]
idn = c.get('idnumber','') or '(empty)'
print(f'  {c[\"id\"]:>10d}  {c[\"shortname\"]:40s}  {idn}')
"
    done
}

# ── Reset to original seed state ─────────────────────────────────────────

reset_state() {
    echo "=== Resetting to original seed state ==="
    echo ""

    echo "Resetting users..."
    call "wsfunction=core_user_update_users&\
users[0][id]=1396920&users[0][idnumber]=&\
users[1][id]=1396921&users[1][idnumber]=&\
users[2][id]=1396922&users[2][idnumber]=OLD_LEGACY_999&\
users[3][id]=1396923&users[3][idnumber]=&\
users[4][id]=1396924&users[4][idnumber]=&\
users[5][id]=1396925&users[5][idnumber]="
    echo "  Done."

    echo "Resetting courses..."
    call "wsfunction=core_course_update_courses&\
courses[0][id]=69213&courses[0][idnumber]=&\
courses[1][id]=69214&courses[1][idnumber]=&\
courses[2][id]=69215&courses[2][idnumber]=&\
courses[3][id]=69216&courses[3][idnumber]=WRONG_OLD_ID_123"
    echo "  Done."
    echo ""

    # Note: Ferris (1396751) and -SP 289-1 (68412) are NOT reset — they're
    # pre-existing records that should always keep their original values.
    echo "Not reset (pre-existing):"
    echo "  User  1396751 Ferris Bueller  idnumber=001Ov00000q40GXIAY"
    echo "  Course 68412 -SP 289-1        idnumber=0P0Ov0000000EnlKAE"
    echo ""

    show_status
}

# ── Seed (create users + courses) ────────────────────────────────────────

seed_data() {
    echo "=== Seeding Moodle sandbox ==="
    echo ""
    echo "NOTE: This creates NEW users and courses. Only run once on a fresh sandbox."
    echo "      If users/courses already exist, use --reset instead."
    echo ""

    echo "Creating users..."
    RESULT=$(call "wsfunction=core_user_create_users&\
users[0][username]=mfaraday&users[0][password]=TestPass123!&users[0][firstname]=Michael&users[0][lastname]=Faraday&users[0][email]=mfaraday@bustaff.edu&\
users[1][username]=mcurie&users[1][password]=TestPass123!&users[1][firstname]=Marie&users[1][lastname]=Curie&users[1][email]=mcurie@bustaff.edu&\
users[2][username]=alovelace&users[2][password]=TestPass123!&users[2][firstname]=Ada&users[2][lastname]=Lovelace&users[2][email]=alovelace@bustaff.edu&users[2][idnumber]=OLD_LEGACY_999&\
users[3][username]=paul.atreides&users[3][password]=TestPass123!&users[3][firstname]=Paul&users[3][lastname]=Atreides&users[3][email]=paul.atreides@caladan.com&\
users[4][username]=tom.brady&users[4][password]=TestPass123!&users[4][firstname]=Tom&users[4][lastname]=Brady&users[4][email]=tom.brady@michigan.edu&\
users[5][username]=john.four.smith&users[5][password]=TestPass123!&users[5][firstname]=John Four&users[5][lastname]=Smith&users[5][email]=john.four.smith@gmail.com")
    echo "  $RESULT"

    echo ""
    echo "Creating courses..."
    RESULT=$(call "wsfunction=core_course_create_courses&\
courses[0][fullname]=AH 221 - Art in the Prado (Test)&courses[0][shortname]=AH 221 - Art in the Prado&courses[0][categoryid]=1&\
courses[1][fullname]=ECON 301 - European Economics&courses[1][shortname]=ECON 301 - European Economics&courses[1][categoryid]=1&\
courses[2][fullname]=HIST 210 - Modern Spain&courses[2][shortname]=HIST 210 - Modern Spain&courses[2][categoryid]=1&\
courses[3][fullname]=PHIL 101 - Ethics&courses[3][shortname]=PHIL 101 - Ethics&courses[3][categoryid]=1&courses[3][idnumber]=WRONG_OLD_ID_123")
    echo "  $RESULT"

    echo ""
    echo "IMPORTANT: Update USER_IDS and COURSE_IDS in this script with the IDs"
    echo "           returned above, then commit the change."
    echo ""

    show_status
}

# ── Cleanup (delete test users + courses) ────────────────────────────────

cleanup_data() {
    echo "=== Cleaning up Moodle sandbox test data ==="
    echo ""
    echo "This will DELETE the test users and courses created by --seed."
    echo "Pre-existing records (Ferris, -SP 289-1) will NOT be deleted."
    echo ""
    printf "Continue? (y/N) " && read -r CONFIRM
    if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
        echo "Aborted."
        exit 0
    fi

    echo ""
    echo "Deleting test users..."
    # Only delete users WE created (not Ferris = 1396751)
    call "wsfunction=core_user_delete_users&\
userids[0]=1396920&\
userids[1]=1396921&\
userids[2]=1396922&\
userids[3]=1396923&\
userids[4]=1396924&\
userids[5]=1396925"
    echo "  Done (6 users deleted)."

    echo ""
    echo "Deleting test courses..."
    # Only delete courses WE created (not 68412 = -SP 289-1)
    call "wsfunction=core_course_delete_courses&\
courseids[0]=69213&\
courseids[1]=69214&\
courseids[2]=69215&\
courseids[3]=69216"
    echo "  Done (4 courses deleted)."

    echo ""
    echo "Remaining (pre-existing, NOT deleted):"
    echo "  User  1396751  Ferris Bueller"
    echo "  Course 68412   -SP 289-1 (Madrid - Spring 2026)"
}

# ── Main ─────────────────────────────────────────────────────────────────

case "$MODE" in
    --status)   show_status ;;
    --reset)    reset_state ;;
    --seed)     seed_data ;;
    --cleanup)  cleanup_data ;;
    *)
        echo "Usage: bash scripts/seed_moodle.sh [--status|--reset|--seed|--cleanup]"
        echo ""
        echo "  --status   Show current Moodle state (default)"
        echo "  --reset    Reset users/courses to original seed values"
        echo "  --seed     Create test users/courses (first time only)"
        echo "  --cleanup  Delete test users/courses from sandbox"
        exit 1
        ;;
esac
