"""Sync Salesforce Course Offering IDs to Moodle Course ID Numbers.

Tiered match logic:
    1. EXACT     — normalized shortname/fullname exact match → auto-sync
    2. FUZZY ≥90% — difflib similarity ≥ threshold → auto-match
    3. AI REVIEW — 50-89% similarity → Bedrock Claude evaluates → match or flag
    4. MANUAL    — below 50% or AI says no → manual review report

Can run as:
    1. A standalone script (python3 -m pipelines.sf_moodle_sync.course_sync)
    2. An AWS Glue job (reads SF via Glue connector)
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
from dataclasses import dataclass, field
from difflib import SequenceMatcher

from .moodle_client import MoodleClient

logger = logging.getLogger(__name__)

FUZZY_AUTO_THRESHOLD = 0.90
FUZZY_CANDIDATE_THRESHOLD = 0.50


@dataclass
class CourseMatch:
    sf_offering_id: str
    sf_course_name: str
    moodle_course_id: int
    moodle_shortname: str
    moodle_fullname: str
    matched_on: str  # "exact_shortname" | "exact_fullname" | "fuzzy" | "ai"
    confidence: float = 1.0  # 0.0–1.0


@dataclass
class ReviewCandidate:
    """A potential match that needs human review."""

    sf_offering_id: str
    sf_course_name: str
    moodle_course_id: int
    moodle_shortname: str
    moodle_fullname: str
    similarity: float
    ai_confidence: float | None = None  # None = not evaluated by AI
    ai_reasoning: str = ""


@dataclass
class CourseSyncResult:
    matched: list[CourseMatch] = field(default_factory=list)
    unmatched_sf: list[dict] = field(default_factory=list)
    already_set: list[CourseMatch] = field(default_factory=list)
    updated: list[CourseMatch] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)
    needs_review: list[ReviewCandidate] = field(default_factory=list)


def _normalize(name: str) -> str:
    """Lowercase, collapse whitespace."""
    return " ".join(name.lower().split())


def _similarity(a: str, b: str) -> float:
    """Compute similarity ratio between two strings (0.0–1.0)."""
    return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


def _best_moodle_match(
    sf_name: str, moodle_courses: list[dict]
) -> tuple[dict | None, float, str]:
    """Find the best fuzzy match for an SF name among Moodle courses.

    Returns (best_course, similarity, matched_field).
    """
    best: dict | None = None
    best_score = 0.0
    best_field = ""
    norm_sf = _normalize(sf_name)

    for mc in moodle_courses:
        for fname in ("shortname", "fullname"):
            moodle_name = mc.get(fname, "")
            if not moodle_name:
                continue
            score = SequenceMatcher(None, norm_sf, _normalize(moodle_name)).ratio()
            if score > best_score:
                best_score = score
                best = mc
                best_field = fname

    return best, best_score, best_field


def _ai_evaluate_matches(
    candidates: list[dict],
    bedrock_region: str = "us-east-1",
) -> list[dict]:
    """Use Bedrock Claude to evaluate ambiguous course matches.

    Parameters
    ----------
    candidates : list[dict]
        Each has ``sf_name``, ``moodle_shortname``, ``moodle_fullname``, ``similarity``.

    Returns
    -------
    list[dict]
        Each has ``index``, ``is_match`` (bool), ``confidence`` (0.0–1.0), ``reasoning``.
    """
    from agents.bedrock_agent import BedrockAgent

    if not candidates:
        return []

    agent = BedrockAgent(
        model_id=os.environ.get(
            "BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0"
        ),
        region=bedrock_region,
    )

    pairs_text = "\n".join(
        f'{i}. SF: "{c["sf_name"]}"  ↔  Moodle shortname: "{c["moodle_shortname"]}", '
        f'fullname: "{c["moodle_fullname"]}"  (string similarity: {c["similarity"]:.0%})'
        for i, c in enumerate(candidates)
    )

    prompt = f"""You are matching course names between two systems (Salesforce and Moodle).

For each pair below, decide if they refer to the SAME course despite name differences.
Consider: abbreviations, semester/year suffixes, section numbers, minor typos, word reordering.

Return ONLY valid JSON — an array of objects, one per pair:
[
  {{"index": 0, "is_match": true/false, "confidence": 0.0-1.0, "reasoning": "brief explanation"}}
]

Pairs to evaluate:
{pairs_text}"""

    try:
        response_text = agent.invoke(
            prompt,
            system_prompt=(
                "You are a course-matching assistant. "
                "Return only valid JSON, no markdown fences."
            ),
            max_tokens=2048,
        )
        # Strip any markdown fencing the model might add
        text = response_text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        results = json.loads(text.strip())
        if isinstance(results, list):
            return results
        logger.warning("AI returned non-list: %s", type(results))
        return []
    except Exception as e:
        logger.error("Bedrock AI evaluation failed: %s", e)
        return []


def match_courses(
    sf_offerings: list[dict],
    moodle_courses: list[dict],
    use_ai: bool = True,
    bedrock_region: str = "us-east-1",
) -> CourseSyncResult:
    """Match SF course offerings to Moodle courses with tiered matching.

    Tier 1: Exact match on normalized shortname/fullname.
    Tier 2: Fuzzy match ≥ 90% → auto-match.
    Tier 3: Fuzzy 50–89% → AI evaluation via Bedrock.
    Tier 4: Below 50% or AI says no → needs_review / unmatched.

    Parameters
    ----------
    sf_offerings : list[dict]
        Each dict must have ``Id`` and ``Name``.
    moodle_courses : list[dict]
        Moodle courses from ``core_course_get_courses``.
    use_ai : bool
        Whether to use Bedrock AI for ambiguous matches.
    bedrock_region : str
        AWS region for Bedrock.
    """
    # Build exact-match lookups
    by_short: dict[str, dict] = {}
    by_full: dict[str, dict] = {}
    for mc in moodle_courses:
        short = _normalize(mc.get("shortname", ""))
        full = _normalize(mc.get("fullname", ""))
        if short:
            by_short[short] = mc
        if full:
            by_full[full] = mc

    result = CourseSyncResult()
    # Track which Moodle courses are already matched to avoid duplicates
    matched_moodle_ids: set[int] = set()
    fuzzy_candidates: list[tuple[dict, dict, float, str]] = []  # (sf, mc, score, field)

    for sf in sf_offerings:
        sf_id = sf.get("Id", "")
        sf_name = sf.get("Name", "")
        if not sf_name:
            result.unmatched_sf.append(sf)
            continue

        norm = _normalize(sf_name)

        # ── Tier 1: Exact match ──
        mc = by_short.get(norm) or by_full.get(norm)
        if mc and mc["id"] not in matched_moodle_ids:
            matched_on = (
                "exact_shortname"
                if norm == _normalize(mc.get("shortname", ""))
                else "exact_fullname"
            )
            match = CourseMatch(
                sf_offering_id=sf_id,
                sf_course_name=sf_name,
                moodle_course_id=mc["id"],
                moodle_shortname=mc.get("shortname", ""),
                moodle_fullname=mc.get("fullname", ""),
                matched_on=matched_on,
                confidence=1.0,
            )
            result.matched.append(match)
            matched_moodle_ids.add(mc["id"])
            if mc.get("idnumber", "") == sf_id:
                result.already_set.append(match)
            continue

        # ── Tier 2/3: Fuzzy match ──
        available = [c for c in moodle_courses if c["id"] not in matched_moodle_ids]
        best_mc, score, best_field = _best_moodle_match(sf_name, available)

        if best_mc and score >= FUZZY_AUTO_THRESHOLD:
            # Tier 2: High-confidence fuzzy → auto-match
            match = CourseMatch(
                sf_offering_id=sf_id,
                sf_course_name=sf_name,
                moodle_course_id=best_mc["id"],
                moodle_shortname=best_mc.get("shortname", ""),
                moodle_fullname=best_mc.get("fullname", ""),
                matched_on="fuzzy",
                confidence=score,
            )
            result.matched.append(match)
            matched_moodle_ids.add(best_mc["id"])
            if best_mc.get("idnumber", "") == sf_id:
                result.already_set.append(match)
        elif best_mc and score >= FUZZY_CANDIDATE_THRESHOLD:
            # Tier 3: Ambiguous — queue for AI evaluation
            fuzzy_candidates.append((sf, best_mc, score, best_field))
        else:
            # No reasonable match found
            result.unmatched_sf.append(sf)

    # ── Tier 3: AI evaluation of ambiguous matches ──
    if fuzzy_candidates and use_ai:
        logger.info("Sending %d ambiguous matches to Bedrock AI...", len(fuzzy_candidates))
        ai_input = [
            {
                "sf_name": sf.get("Name", ""),
                "moodle_shortname": mc.get("shortname", ""),
                "moodle_fullname": mc.get("fullname", ""),
                "similarity": score,
            }
            for sf, mc, score, _ in fuzzy_candidates
        ]

        ai_results = _ai_evaluate_matches(ai_input, bedrock_region=bedrock_region)

        # Index AI results by their index field
        ai_by_idx: dict[int, dict] = {r["index"]: r for r in ai_results if "index" in r}

        for i, (sf, mc, score, best_field) in enumerate(fuzzy_candidates):
            sf_id = sf.get("Id", "")
            ai = ai_by_idx.get(i, {})
            ai_is_match = ai.get("is_match", False)
            ai_conf = ai.get("confidence", 0.0)
            ai_reason = ai.get("reasoning", "")

            if ai_is_match and ai_conf >= 0.8 and mc["id"] not in matched_moodle_ids:
                # AI says match with high confidence → auto-match
                match = CourseMatch(
                    sf_offering_id=sf_id,
                    sf_course_name=sf.get("Name", ""),
                    moodle_course_id=mc["id"],
                    moodle_shortname=mc.get("shortname", ""),
                    moodle_fullname=mc.get("fullname", ""),
                    matched_on="ai",
                    confidence=ai_conf,
                )
                result.matched.append(match)
                matched_moodle_ids.add(mc["id"])
                if mc.get("idnumber", "") == sf_id:
                    result.already_set.append(match)
            else:
                # AI unsure or says no → needs human review
                result.needs_review.append(
                    ReviewCandidate(
                        sf_offering_id=sf_id,
                        sf_course_name=sf.get("Name", ""),
                        moodle_course_id=mc["id"],
                        moodle_shortname=mc.get("shortname", ""),
                        moodle_fullname=mc.get("fullname", ""),
                        similarity=score,
                        ai_confidence=ai_conf if ai else None,
                        ai_reasoning=ai_reason,
                    )
                )
    elif fuzzy_candidates:
        # AI disabled — all ambiguous go to review
        for sf, mc, score, _ in fuzzy_candidates:
            result.needs_review.append(
                ReviewCandidate(
                    sf_offering_id=sf.get("Id", ""),
                    sf_course_name=sf.get("Name", ""),
                    moodle_course_id=mc["id"],
                    moodle_shortname=mc.get("shortname", ""),
                    moodle_fullname=mc.get("fullname", ""),
                    similarity=score,
                )
            )

    logger.info(
        "Course matching: %d exact+fuzzy matched, %d AI matched, "
        "%d needs review, %d unmatched",
        sum(1 for m in result.matched if m.matched_on != "ai"),
        sum(1 for m in result.matched if m.matched_on == "ai"),
        len(result.needs_review),
        len(result.unmatched_sf),
    )

    return result


def sync_courses(
    sf_offerings: list[dict],
    moodle: MoodleClient,
    dry_run: bool = False,
    moodle_courses: list[dict] | None = None,
    use_ai: bool = True,
    bedrock_region: str = "us-east-1",
) -> CourseSyncResult:
    """Full sync: match SF offerings to Moodle courses and update idnumber.

    Parameters
    ----------
    sf_offerings : list[dict]
        SF Course Offering records with ``Id`` and ``Name``.
    moodle : MoodleClient
        Authenticated Moodle client.
    dry_run : bool
        If True, match only — do not update Moodle.
    moodle_courses : list[dict] | None
        Pre-fetched Moodle courses. If None, fetches all from Moodle.
    use_ai : bool
        Whether to use Bedrock AI for ambiguous matches.
    bedrock_region : str
        AWS region for Bedrock.
    """
    if moodle_courses is None:
        logger.info("Fetching all Moodle courses...")
        moodle_courses = moodle.get_courses()
        logger.info("Fetched %d Moodle courses", len(moodle_courses))

    result = match_courses(
        sf_offerings, moodle_courses, use_ai=use_ai, bedrock_region=bedrock_region
    )

    if dry_run:
        logger.info("Dry run — skipping updates")
        return result

    # Update Moodle courses that need new idnumber
    to_update = [m for m in result.matched if m not in result.already_set]
    if not to_update:
        logger.info("No courses need updating")
        return result

    batch_size = 50
    for i in range(0, len(to_update), batch_size):
        batch = to_update[i : i + batch_size]
        updates = [
            {"id": m.moodle_course_id, "idnumber": m.sf_offering_id} for m in batch
        ]
        try:
            moodle.update_courses(updates)
            result.updated.extend(batch)
            logger.info("Updated batch %d–%d", i + 1, i + len(batch))
        except Exception as e:
            logger.error("Failed to update batch %d–%d: %s", i + 1, i + len(batch), e)
            for m in batch:
                result.errors.append({"match": m.__dict__, "error": str(e)})

    return result


def print_review_report(result: CourseSyncResult, output_path: str | None = None) -> None:
    """Print and optionally save a manual review report for ambiguous matches."""
    lines: list[str] = []
    lines.append("")
    lines.append("=" * 80)
    lines.append("  COURSE MATCHING REPORT")
    lines.append("=" * 80)
    lines.append(f"  Exact matches:        {sum(1 for m in result.matched if 'exact' in m.matched_on)}")
    lines.append(f"  Fuzzy auto-matches:   {sum(1 for m in result.matched if m.matched_on == 'fuzzy')}")
    lines.append(f"  AI auto-matches:      {sum(1 for m in result.matched if m.matched_on == 'ai')}")
    lines.append(f"  Needs manual review:  {len(result.needs_review)}")
    lines.append(f"  Unmatched:            {len(result.unmatched_sf)}")
    lines.append(f"  Already set:          {len(result.already_set)}")
    lines.append("=" * 80)

    if result.needs_review:
        lines.append("")
        lines.append("── NEEDS MANUAL REVIEW ──")
        lines.append(
            f"  {'#':<4s}  {'SF Name':<40s}  {'Moodle Short':<30s}  "
            f"{'Sim':>5s}  {'AI':>5s}  AI Reasoning"
        )
        lines.append("  " + "-" * 120)
        for i, r in enumerate(result.needs_review, 1):
            ai_str = f"{r.ai_confidence:.0%}" if r.ai_confidence is not None else "N/A"
            lines.append(
                f"  {i:<4d}  {r.sf_course_name:<40s}  {r.moodle_shortname:<30s}  "
                f"{r.similarity:>5.0%}  {ai_str:>5s}  {r.ai_reasoning}"
            )

    if result.unmatched_sf:
        lines.append("")
        lines.append("── UNMATCHED SF OFFERINGS (no Moodle candidate found) ──")
        for sf in result.unmatched_sf:
            lines.append(f"  SF {sf.get('Id', '?')}  {sf.get('Name', '(no name)')}")

    if result.matched:
        lines.append("")
        lines.append("── AUTO-MATCHED ──")
        for m in result.matched:
            conf_str = f"{m.confidence:.0%}" if m.confidence < 1.0 else "100%"
            lines.append(
                f"  [{m.matched_on:<15s} {conf_str:>4s}]  "
                f"SF \"{m.sf_course_name}\" → Moodle \"{m.moodle_shortname}\""
            )

    report_text = "\n".join(lines)
    print(report_text)

    if output_path:
        out = pathlib.Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report_text)
        logger.info("Review report saved to %s", output_path)

    # Also save JSON for programmatic use
    if output_path:
        json_path = pathlib.Path(output_path).with_suffix(".json")
        review_data = {
            "summary": {
                "exact_matches": sum(
                    1 for m in result.matched if "exact" in m.matched_on
                ),
                "fuzzy_auto_matches": sum(
                    1 for m in result.matched if m.matched_on == "fuzzy"
                ),
                "ai_auto_matches": sum(
                    1 for m in result.matched if m.matched_on == "ai"
                ),
                "needs_review": len(result.needs_review),
                "unmatched": len(result.unmatched_sf),
            },
            "needs_review": [
                {
                    "sf_offering_id": r.sf_offering_id,
                    "sf_course_name": r.sf_course_name,
                    "moodle_course_id": r.moodle_course_id,
                    "moodle_shortname": r.moodle_shortname,
                    "moodle_fullname": r.moodle_fullname,
                    "similarity": round(r.similarity, 3),
                    "ai_confidence": round(r.ai_confidence, 3)
                    if r.ai_confidence is not None
                    else None,
                    "ai_reasoning": r.ai_reasoning,
                }
                for r in result.needs_review
            ],
            "unmatched_sf": result.unmatched_sf,
            "auto_matched": [
                {
                    "sf_offering_id": m.sf_offering_id,
                    "sf_course_name": m.sf_course_name,
                    "moodle_course_id": m.moodle_course_id,
                    "moodle_shortname": m.moodle_shortname,
                    "matched_on": m.matched_on,
                    "confidence": round(m.confidence, 3),
                }
                for m in result.matched
            ],
        }
        json_path.write_text(json.dumps(review_data, indent=2))
        logger.info("Review JSON saved to %s", json_path)


def load_sf_offerings_from_file(path: str) -> list[dict]:
    """Load SF offerings from a JSON file (for testing or one-time runs)."""
    data = json.loads(pathlib.Path(path).read_text())
    if isinstance(data, list):
        return data
    return data.get("records", data.get("offerings", []))


# ---------------------------------------------------------------------------
# Glue job entry point
# ---------------------------------------------------------------------------

def run_glue_course_sync():
    """Entry point when running as an AWS Glue job."""
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "sf_connection_name",
            "moodle_url",
            "moodle_secret_name",
            "dry_run",
        ],
    )

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Read SF Course Offerings via Glue connector
    sf_dyf = glue_ctx.create_dynamic_frame.from_options(
        connection_type="salesforce",
        connection_options={
            "connectionName": args["sf_connection_name"],
            "ENTITY_NAME": "CourseOffering",
            "API_VERSION": "v60.0",
            "QUERY": "SELECT Id, Name FROM CourseOffering",
        },
    )
    sf_df = sf_dyf.toDF()
    sf_offerings = [row.asDict() for row in sf_df.collect()]
    logger.info("Read %d SF Course Offerings", len(sf_offerings))

    moodle = MoodleClient(
        base_url=args["moodle_url"],
        secret_name=args["moodle_secret_name"],
    )

    result = sync_courses(
        sf_offerings,
        moodle,
        dry_run=args.get("dry_run", "false").lower() == "true",
        use_ai=True,
    )

    print_review_report(result)

    print(
        f"Course sync complete: {len(result.updated)} updated, "
        f"{len(result.unmatched_sf)} unmatched, "
        f"{len(result.needs_review)} needs review, "
        f"{len(result.errors)} errors"
    )

    job.commit()
