"""AWS Glue job — Discover SF + Moodle data for sync testing.

Reads SF objects (CourseOffering, Account, Participants) and audits
Moodle sandbox to check what data exists and what's missing IDs.

Glue job parameters:
    --JOB_NAME       Glue job name
    --moodle_url     Moodle site URL (optional, for Moodle audit)
    --moodle_token   Moodle web-service token (optional)
"""

import json
import sys
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "moodle_url", "moodle_token"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SF_CONNECTION = "Sandbox Salesforce Connection"
MOODLE_URL = args.get("moodle_url", "")
MOODLE_TOKEN = args.get("moodle_token", "")


def read_sf(entity, query=None):
    opts = {
        "connectionName": SF_CONNECTION,
        "ENTITY_NAME": entity,
        "API_VERSION": "v60.0",
    }
    if query:
        opts["QUERY"] = query
    try:
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="salesforce",
            connection_options=opts,
        ).toDF()
        return df
    except Exception as e:
        print(f"  FAILED ({entity}): {e}")
        return None


def section(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


# ══════════════════════════════════════════════════════════════════════════
#  PART 1: SALESFORCE DISCOVERY
# ══════════════════════════════════════════════════════════════════════════

section("SF DISCOVERY")

# ── 1. Course Offerings (basic) ──────────────────────────────────────────
section("1. Course Offerings — Basic (Id, Name, HCG_External_ID__c)")
start = time.time()
df = read_sf(
    "CourseOffering",
    query="SELECT Id, Name, HCG_External_ID__c FROM CourseOffering LIMIT 20",
)
if df is not None:
    print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
    df.show(20, truncate=False)
else:
    print("  Trying without SOQL...")
    df = read_sf("CourseOffering")
    if df is not None:
        print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
        print(f"  Columns: {df.columns}")
        df.show(5, truncate=False)

# ── 2. Course Offerings — Faculty fields ────────────────────────────────
section("2. Course Offerings — Faculty Fields (PrimaryFacultyId, FacultyId)")
start = time.time()
# Try standard EDA field first
df = read_sf(
    "CourseOffering",
    query=(
        "SELECT Id, Name, FacultyId, HCG_External_ID__c "
        "FROM CourseOffering LIMIT 20"
    ),
)
if df is not None:
    print(f"  Records (with FacultyId): {df.count()} ({time.time() - start:.1f}s)")
    df.show(20, truncate=False)
else:
    print("  FacultyId not available, trying all fields to discover schema...")
    df = read_sf("CourseOffering")
    if df is not None:
        print(f"  ALL CourseOffering columns ({len(df.columns)}):")
        for col in sorted(df.columns):
            print(f"    - {col}")
        # Show sample to see which fields have faculty-related data
        faculty_cols = [
            c for c in df.columns
            if any(kw in c.lower() for kw in ["faculty", "instructor", "primary", "participant"])
        ]
        if faculty_cols:
            print(f"\n  Faculty-related columns: {faculty_cols}")
            df.select(["Id", "Name"] + faculty_cols).show(10, truncate=False)
        else:
            print("  No faculty-related columns found in CourseOffering")
            df.show(5, truncate=False)

# ── 3. Course Offering with Outside Instructor ─────────────────────────
section("3. Course Offerings — Outside Instructor (HCG custom fields)")
start = time.time()
df = read_sf(
    "CourseOffering",
    query=(
        "SELECT Id, Name, HCG_External_ID__c, HCG_Outside_Instructor__c "
        "FROM CourseOffering LIMIT 20"
    ),
)
if df is not None:
    print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
    df.show(20, truncate=False)
else:
    print("  HCG_Outside_Instructor__c not available on CourseOffering")

# ── 4. CourseOfferingParticipant (standard EDA object) ──────────────────
section("4. CourseOfferingParticipant (standard EDA object)")
start = time.time()
df = read_sf(
    "hed__Course_Offering_Schedule__c",
    query=(
        "SELECT Id FROM hed__Course_Offering_Schedule__c LIMIT 1"
    ),
)
# Try the standard EDA Course Enrollment object (links contacts to offerings)
df = read_sf(
    "hed__Course_Enrollment__c",
    query=(
        "SELECT Id, Name, hed__Contact__c, hed__Course_Offering__c, hed__Role__c "
        "FROM hed__Course_Enrollment__c LIMIT 20"
    ),
)
if df is not None:
    print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
    print(f"  Columns: {df.columns}")
    df.show(20, truncate=False)

    # Check for faculty-role records
    faculty_df = df.filter(
        df["hed__Role__c"].isin(["Faculty", "Instructor", "Teacher", "Primary Instructor"])
    )
    print(f"\n  Faculty-role records: {faculty_df.count()}")
    if faculty_df.count() > 0:
        faculty_df.show(20, truncate=False)
else:
    print("  hed__Course_Enrollment__c not found, trying HCG custom...")
    # Try HCG custom participant object
    df = read_sf(
        "HCG_Course_Offering_Participant__c",
        query=(
            "SELECT Id, Name FROM HCG_Course_Offering_Participant__c LIMIT 20"
        ),
    )
    if df is not None:
        print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
        print(f"  Columns: {df.columns}")
        df.show(20, truncate=False)
    else:
        print("  HCG_Course_Offering_Participant__c also not found")

# ── 5. Session Participants ─────────────────────────────────────────────
section("5. Session Participants (HCG_Session_Participant__c)")
start = time.time()
df = read_sf(
    "HCG_Session_Participant__c",
    query=(
        "SELECT Id, Name, HCG_Student__c, HCG_External_ID__c "
        "FROM HCG_Session_Participant__c LIMIT 20"
    ),
)
if df is not None:
    print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
    print(f"  Columns: {df.columns}")
    df.show(20, truncate=False)
else:
    print("  Trying all fields...")
    df = read_sf("HCG_Session_Participant__c")
    if df is not None:
        print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
        print(f"  Columns: {df.columns}")
        df.printSchema()
        df.show(5, truncate=False)

# ── 6. Person Accounts (students + faculty) ────────────────────────────
section("6. Person Accounts (IsPersonAccount = true)")
start = time.time()
df = read_sf(
    "Account",
    query=(
        "SELECT Id, Name, PersonEmail FROM Account "
        "WHERE IsPersonAccount = true LIMIT 30"
    ),
)
if df is not None:
    total = df.count()
    no_email = df.filter(df["PersonEmail"].isNull()).count()
    print(f"  Records: {total} ({time.time() - start:.1f}s)")
    print(f"  Missing PersonEmail: {no_email}")
    df.show(30, truncate=False)
else:
    print("  Trying basic Account read...")
    df = read_sf("Account", query="SELECT Id, Name, PersonEmail FROM Account LIMIT 20")
    if df is not None:
        print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
        df.show(20, truncate=False)

# ── 7. Contacts (faculty may be Contacts, not Person Accounts) ─────────
section("7. Contacts — check if faculty are here instead of Accounts")
start = time.time()
df = read_sf(
    "Contact",
    query=(
        "SELECT Id, Name, Email, AccountId "
        "FROM Contact LIMIT 20"
    ),
)
if df is not None:
    print(f"  Records: {df.count()} ({time.time() - start:.1f}s)")
    df.show(20, truncate=False)
else:
    print("  Contact read failed")


# ══════════════════════════════════════════════════════════════════════════
#  PART 2: MOODLE SANDBOX AUDIT
# ══════════════════════════════════════════════════════════════════════════

if MOODLE_URL and MOODLE_TOKEN:
    import requests

    section("MOODLE SANDBOX AUDIT")

    def moodle_call(wsfunction, **params):
        """Call Moodle REST API."""
        payload = {
            "wstoken": MOODLE_TOKEN,
            "wsfunction": wsfunction,
            "moodlewsrestformat": "json",
            **params,
        }
        resp = requests.post(
            f"{MOODLE_URL}/webservice/rest/server.php",
            data=payload,
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        if isinstance(result, dict) and result.get("exception"):
            print(f"  Moodle error: {result.get('message', result.get('exception'))}")
            return None
        return result

    # ── 8. Moodle Courses ───────────────────────────────────────────────
    section("8. Moodle Courses — all courses with idnumber status")
    courses = moodle_call("core_course_get_courses")
    if courses:
        print(f"  Total courses: {len(courses)}")
        empty_id = [c for c in courses if not c.get("idnumber")]
        has_id = [c for c in courses if c.get("idnumber")]
        print(f"  With idnumber set:    {len(has_id)}")
        print(f"  With idnumber empty:  {len(empty_id)}")
        print()
        print("  Courses WITH idnumber:")
        for c in has_id:
            print(f"    #{c['id']}  shortname={c.get('shortname',''):<40s}  "
                  f"idnumber={c.get('idnumber','')}")
        print()
        print("  Courses WITHOUT idnumber (candidates for sync):")
        for c in empty_id:
            if c['id'] == 1:  # skip site-level course
                continue
            print(f"    #{c['id']}  shortname={c.get('shortname',''):<40s}  "
                  f"fullname={c.get('fullname','')}")

    # ── 9. Moodle Users ─────────────────────────────────────────────────
    section("9. Moodle Users — sample users with idnumber status")

    # Get all users via search (Moodle doesn't have a "get all users" API,
    # but we can search with a wildcard on email)
    users_result = moodle_call(
        "core_user_get_users",
        **{"criteria[0][key]": "email", "criteria[0][value]": "%"},
    )
    users = users_result.get("users", []) if users_result else []

    if users:
        print(f"  Total users found: {len(users)}")
        empty_id = [u for u in users if not u.get("idnumber")]
        has_id = [u for u in users if u.get("idnumber")]
        conflict_id = [u for u in has_id if not u["idnumber"].startswith("001")]  # not SF format
        print(f"  With idnumber set:        {len(has_id)}")
        print(f"  With idnumber empty:      {len(empty_id)}")
        print(f"  With non-SF-format ID:    {len(conflict_id)}  (potential conflicts)")
        print()
        print("  Users WITH idnumber:")
        for u in has_id:
            print(f"    #{u['id']}  email={u.get('email',''):<40s}  "
                  f"idnumber={u.get('idnumber',''):<20s}  "
                  f"name={u.get('firstname','')} {u.get('lastname','')}")
        print()
        print("  Users WITHOUT idnumber (candidates for sync):")
        for u in empty_id:
            if u.get("username") in ("guest", "admin"):
                continue
            print(f"    #{u['id']}  email={u.get('email',''):<40s}  "
                  f"name={u.get('firstname','')} {u.get('lastname','')}")

    # ── 10. Cross-reference: SF Accounts vs Moodle Users ─────────────
    section("10. Cross-reference: SF emails in Moodle?")

    # Re-read SF accounts to cross-check
    sf_df = read_sf(
        "Account",
        query=(
            "SELECT Id, Name, PersonEmail FROM Account "
            "WHERE IsPersonAccount = true"
        ),
    )
    if sf_df is not None and users:
        sf_accounts = [row.asDict() for row in sf_df.collect()]
        moodle_emails = {u.get("email", "").strip().lower() for u in users}

        matched = []
        unmatched = []
        for sf in sf_accounts:
            email = (sf.get("PersonEmail") or "").strip().lower()
            if not email:
                unmatched.append({**sf, "_reason": "no_email"})
            elif email in moodle_emails:
                matched.append(sf)
            else:
                unmatched.append({**sf, "_reason": "not_in_moodle"})

        print(f"  SF Person Accounts:       {len(sf_accounts)}")
        print(f"  Matched in Moodle:        {len(matched)}")
        print(f"  NOT in Moodle:            {len(unmatched)}")
        print()
        print("  MATCHED (will sync):")
        for sf in matched:
            email = (sf.get("PersonEmail") or "").strip().lower()
            # find corresponding moodle user
            mu = next((u for u in users if u.get("email", "").strip().lower() == email), {})
            print(f"    SF {sf['Id']}  {sf.get('Name',''):<30s}  "
                  f"email={email:<35s}  "
                  f"moodle_idnumber={mu.get('idnumber', '(empty)')}")
        print()
        print("  UNMATCHED:")
        for sf in unmatched[:20]:  # limit output
            print(f"    SF {sf.get('Id','')}  {sf.get('Name',''):<30s}  "
                  f"email={sf.get('PersonEmail',''):<35s}  "
                  f"reason={sf.get('_reason','')}")
    else:
        print("  Could not cross-reference (missing SF or Moodle data)")

    # ── 11. Summary: Test readiness ─────────────────────────────────────
    section("11. TEST READINESS SUMMARY")
    print()
    if courses:
        course_empty = len([c for c in courses if not c.get("idnumber") and c["id"] != 1])
        course_set = len([c for c in courses if c.get("idnumber")])
        print(f"  COURSES:  {course_empty} ready to sync, {course_set} already have IDs")
    if users:
        user_empty = len([u for u in users
                          if not u.get("idnumber")
                          and u.get("username") not in ("guest", "admin")])
        user_set = len([u for u in users if u.get("idnumber")])
        print(f"  USERS:    {user_empty} ready to sync, {user_set} already have IDs")
    if sf_df is not None and users:
        print(f"  SF→MOODLE MATCH: {len(matched)} accounts match by email")
    print()
    print("  Faculty test data needed:")
    print("    - CourseOffering with faculty reference (see sections 2-4 above)")
    print("    - Faculty Account with PersonEmail matching a Moodle user")
    print("    - Moodle user for faculty with empty idnumber")
    print("    - Moodle user for faculty with conflicting idnumber (edge case)")

else:
    section("MOODLE AUDIT SKIPPED")
    print("  Set --moodle_url and --moodle_token to enable Moodle audit")
    print("  Example: --moodle_url https://ies-sbox.unhosting.site "
          "--moodle_token 41c6b3ad321ff6a69f04814d66362a3b")

job.commit()
print("\nDone.")
