"""Local report: Moodle-perspective matching — Gold vs SF UAT.

Compares gold_course_offering matches to SF UAT AtlasId matches,
all from the Moodle side: for every Moodle course, what matched?
"""

import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

OUTPUT_DIR = Path("output")


def extract_atlas_id(shortname):
    """Extract trailing 5+ digit Atlas ID from Moodle shortname."""
    m = re.search(r"-(\d{5,})$", shortname or "")
    return m.group(1) if m else None


def load_json(path):
    return json.loads(Path(path).read_text())


def load_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ── Load data ────────────────────────────────────────────────────────────

print("=" * 80)
print("  MOODLE PERSPECTIVE REPORT: Gold vs SF UAT")
print("=" * 80)

# 1. Moodle courses
moodle = load_json(OUTPUT_DIR / "moodle_all_courses_cache.json")
print("\n  Moodle courses loaded: %d" % len(moodle))

# 2. SF UAT offerings (has AtlasId from HCG_External_ID__c)
sf_uat = load_json(OUTPUT_DIR / "sf_course_offerings_uat.json")
print("  SF UAT offerings loaded: %d" % len(sf_uat))

# 3. Gold course offering matches
gold_csv = load_csv(OUTPUT_DIR / "gold_moodle_match" / "gold_course_offering_moodle_match.csv")
print("  Gold course offering records loaded: %d" % len(gold_csv))

# ── Build indexes ────────────────────────────────────────────────────────

# Moodle atlas_id index
moodle_by_atlas = {}
for c in moodle:
    atlas = extract_atlas_id(c.get("shortname", ""))
    if atlas:
        c["_atlas_id"] = atlas
        moodle_by_atlas.setdefault(atlas, []).append(c)
    else:
        c["_atlas_id"] = None

# SF UAT: AtlasId → offering
sf_uat_by_atlas = {}
for sf in sf_uat:
    aid = str(sf.get("AtlasId", "")).strip()
    if aid:
        sf_uat_by_atlas.setdefault(aid, []).append(sf)

# Gold: Course_Id → record (only matched ones)
gold_by_atlas = {}
for g in gold_csv:
    cid = str(g.get("Course_Id", "")).strip()
    if cid:
        gold_by_atlas.setdefault(cid, []).append(g)

# Also build gold matched moodle IDs
gold_matched_atlas = set()
for g in gold_csv:
    if g.get("atlas_id"):
        gold_matched_atlas.add(g["atlas_id"])


# ── Moodle perspective ───────────────────────────────────────────────────

print("\n" + "=" * 80)
print("  BUILDING MOODLE PERSPECTIVE")
print("=" * 80)

report_rows = []
stats = defaultdict(int)

for c in moodle:
    atlas = c["_atlas_id"]
    row = {
        "moodle_id": c["id"],
        "shortname": c.get("shortname", ""),
        "fullname": c.get("fullname", ""),
        "idnumber": c.get("idnumber", ""),
        "atlas_id": atlas or "",
    }

    if not atlas:
        row["gold_match"] = ""
        row["gold_name"] = ""
        row["uat_match"] = ""
        row["uat_name"] = ""
        row["uat_atlas_id"] = ""
        row["status"] = "NO_ATLAS_ID"
        stats["no_atlas_id"] += 1
        report_rows.append(row)
        continue

    # Check gold match (exact on atlas_id)
    gold_hit = gold_by_atlas.get(atlas, [])
    if gold_hit:
        g = gold_hit[0]
        row["gold_match"] = "YES"
        row["gold_name"] = g.get("Name", "")
    else:
        row["gold_match"] = "NO"
        row["gold_name"] = ""

    # Check SF UAT match (exact on AtlasId = HCG_External_ID__c)
    uat_hit = sf_uat_by_atlas.get(atlas, [])
    if uat_hit:
        sf = uat_hit[0]
        row["uat_match"] = "YES"
        row["uat_name"] = sf.get("Name", "")
        row["uat_atlas_id"] = str(sf.get("AtlasId", ""))
    else:
        row["uat_match"] = "NO"
        row["uat_name"] = ""
        row["uat_atlas_id"] = ""

    # Determine status
    if row["gold_match"] == "YES" and row["uat_match"] == "YES":
        row["status"] = "BOTH_MATCH"
        stats["both_match"] += 1
    elif row["gold_match"] == "YES":
        row["status"] = "GOLD_ONLY"
        stats["gold_only"] += 1
    elif row["uat_match"] == "YES":
        row["status"] = "UAT_ONLY"
        stats["uat_only"] += 1
    else:
        row["status"] = "NEITHER"
        stats["neither"] += 1

    report_rows.append(row)


# ── Summary ──────────────────────────────────────────────────────────────

total = len(report_rows)
has_atlas = sum(1 for r in report_rows if r["atlas_id"])

print("\n  MOODLE COURSE SUMMARY")
print("  %-45s %d" % ("Total Moodle courses", total))
print("  %-45s %d" % ("  With Atlas ID (from shortname)", has_atlas))
print("  %-45s %d" % ("  Without Atlas ID", stats["no_atlas_id"]))
print()
print("  MATCH COMPARISON (Moodle atlas_id = SF/Gold key):")
print("  %-45s %d (%.1f%%)" % (
    "Matched in BOTH Gold + SF UAT",
    stats["both_match"],
    stats["both_match"] / has_atlas * 100 if has_atlas else 0,
))
print("  %-45s %d" % ("Matched in Gold ONLY (not in SF UAT)", stats["gold_only"]))
print("  %-45s %d" % ("Matched in SF UAT ONLY (not in Gold)", stats["uat_only"]))
print("  %-45s %d" % ("Has Atlas ID but NO match in either", stats["neither"]))

# Gold total matched
gold_matched = stats["both_match"] + stats["gold_only"]
uat_matched = stats["both_match"] + stats["uat_only"]
print()
print("  %-45s %d (%.1f%% of Moodle w/ atlas)" % (
    "Total Gold matches", gold_matched, gold_matched / has_atlas * 100 if has_atlas else 0,
))
print("  %-45s %d (%.1f%% of Moodle w/ atlas)" % (
    "Total SF UAT matches", uat_matched, uat_matched / has_atlas * 100 if has_atlas else 0,
))

# ── Details ──────────────────────────────────────────────────────────────

# Gold only (in gold but not UAT)
gold_only = [r for r in report_rows if r["status"] == "GOLD_ONLY"]
if gold_only:
    print("\n" + "-" * 80)
    print("  GOLD ONLY — matched in gold_course_offering but NOT in SF UAT (%d):" % len(gold_only))
    print("  %-10s %-50s %s" % ("Atlas ID", "Moodle Shortname", "Gold Name"))
    for r in gold_only[:30]:
        print("  %-10s %-50s %s" % (r["atlas_id"], r["shortname"][:50], r["gold_name"][:40]))
    if len(gold_only) > 30:
        print("  ... and %d more" % (len(gold_only) - 30))

# UAT only (in UAT but not gold)
uat_only = [r for r in report_rows if r["status"] == "UAT_ONLY"]
if uat_only:
    print("\n" + "-" * 80)
    print("  SF UAT ONLY — matched in SF UAT but NOT in gold (%d):" % len(uat_only))
    print("  %-10s %-50s %s" % ("Atlas ID", "Moodle Shortname", "UAT Name"))
    for r in uat_only[:30]:
        print("  %-10s %-50s %s" % (r["atlas_id"], r["shortname"][:50], r["uat_name"][:40]))
    if len(uat_only) > 30:
        print("  ... and %d more" % (len(uat_only) - 30))

# Neither (has atlas ID but no match)
neither = [r for r in report_rows if r["status"] == "NEITHER"]
if neither:
    print("\n" + "-" * 80)
    print("  NEITHER — Atlas ID but no match in Gold or SF UAT (%d):" % len(neither))
    print("  %-10s %-60s" % ("Atlas ID", "Moodle Shortname"))
    for r in neither[:20]:
        print("  %-10s %-60s" % (r["atlas_id"], r["shortname"][:60]))
    if len(neither) > 20:
        print("  ... and %d more" % (len(neither) - 20))

# SF UAT coverage: how many SF UAT offerings have a Moodle match?
sf_uat_matched = set()
sf_uat_unmatched = []
for sf in sf_uat:
    aid = str(sf.get("AtlasId", "")).strip()
    if aid and aid in moodle_by_atlas:
        sf_uat_matched.add(aid)
    elif aid:
        sf_uat_unmatched.append(sf)

print("\n" + "=" * 80)
print("  SF UAT PERSPECTIVE")
print("  %-45s %d" % ("Total SF UAT offerings", len(sf_uat)))
print("  %-45s %d" % ("  With AtlasId", sum(1 for s in sf_uat if s.get("AtlasId"))))
print("  %-45s %d (%.1f%%)" % (
    "  Matched to Moodle (exact atlas_id)",
    len(sf_uat_matched),
    len(sf_uat_matched) / len(sf_uat) * 100 if sf_uat else 0,
))
print("  %-45s %d" % ("  No Moodle match", len(sf_uat_unmatched)))

if sf_uat_unmatched:
    print("\n  SF UAT NOT IN MOODLE:")
    print("  %-20s %-10s %s" % ("SF Id", "AtlasId", "Name"))
    for sf in sf_uat_unmatched[:20]:
        print("  %-20s %-10s %s" % (sf["Id"], sf.get("AtlasId", ""), sf["Name"][:50]))

# Note about HCG_Format__c
print("\n" + "=" * 80)
print("  NOTE: SF UAT currently uses AtlasId (HCG_External_ID__c) for matching.")
print("  HCG_Format__c has NOT been extracted yet — need to run updated Glue job")
print("  to pull HCG_Format__c and compare if it gives different/better matches.")
print("=" * 80)

# ── Write CSV ────────────────────────────────────────────────────────────

out_path = OUTPUT_DIR / "moodle_perspective_gold_vs_uat.csv"
fieldnames = [
    "moodle_id", "shortname", "fullname", "idnumber", "atlas_id",
    "gold_match", "gold_name", "uat_match", "uat_name", "uat_atlas_id", "status",
]
with open(out_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(report_rows)

print("\n  Report saved: %s (%d rows)" % (out_path, len(report_rows)))
