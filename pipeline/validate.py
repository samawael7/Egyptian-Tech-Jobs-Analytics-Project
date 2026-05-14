import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION RULES
# ══════════════════════════════════════════════════════════════════════════════

def run_validation(
    parquet_path: Path = Path("data/processed/jobs_cleaned.parquet"),
) -> bool:
    """
    Runs all data quality checks on the cleaned Parquet file.

    WHAT IT CHECKS:
        1. File exists and is readable
        2. Row count is within expected range
        3. Required columns all exist
        4. Critical columns have no nulls
        5. Categorical columns only contain valid values
        6. job_url values are unique
        7. URLs look like real Wuzzuf URLs
        8. Date range is reasonable

    RETURNS:
        True if all checks pass.

    RAISES:
        ValueError with details if any check fails.
        This causes Airflow to mark the task as FAILED.
    """
    logger.info("=" * 55)
    logger.info("VALIDATION STARTED")
    logger.info("=" * 55)

    # Track all failures — collect all issues before raising
    # WHY COLLECT ALL: if we raise on first failure, you fix it,
    # run again, find the next failure. Better to report everything at once.
    failures = []
    passed   = 0

    # ── Load file ─────────────────────────────────────────────────────────────
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} rows from {parquet_path}")

    # ── Check 1: Row count ────────────────────────────────────────────────────
    # WHY: If scraper returns 0 jobs, pipeline should stop immediately.
    # Upper bound catches runaway scraping or data explosions.
    min_rows, max_rows = 10, 10000
    if not (min_rows <= len(df) <= max_rows):
        failures.append(
            f"Row count {len(df)} outside expected range [{min_rows}, {max_rows}]"
        )
    else:
        passed += 1
        logger.info(f"✅ Row count: {len(df)} (expected {min_rows}–{max_rows})")

    # ── Check 2: Required columns exist ───────────────────────────────────────
    required_columns = [
        "job_title", "company_name", "job_type", "skills_list",
        "job_category", "posted_date", "job_url", "city", "work_type",
        "min_experience", "max_experience", "experience_level",
        "scrape_date", "company_type",
    ]
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        failures.append(f"Missing columns: {missing_cols}")
    else:
        passed += 1
        logger.info(f"✅ All {len(required_columns)} required columns present")

    # ── Check 3: No nulls in critical columns ─────────────────────────────────
    # WHY THESE COLUMNS: These are the ones your star schema and
    # dbt surrogate keys depend on. Nulls here break everything downstream.
    critical_columns = [
        "job_title", "job_url", "experience_level",
        "posted_date", "scrape_date", "company_type",
    ]
    for col in critical_columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                failures.append(f"Column '{col}' has {null_count} null values")
            else:
                passed += 1
                logger.info(f"✅ No nulls in '{col}'")

    # ── Check 4: job_url uniqueness ───────────────────────────────────────────
    # WHY: Duplicate URLs = duplicate jobs in Snowflake even with MERGE.
    # The Snowflake MERGE uses job_url as the match key — duplicates here
    # cause the second occurrence to overwrite the first silently.
    if "job_url" in df.columns:
        dupe_count = df["job_url"].duplicated().sum()
        if dupe_count > 0:
            failures.append(f"job_url has {dupe_count} duplicate values")
        else:
            passed += 1
            logger.info(f"✅ job_url is unique across all {len(df)} rows")

    # ── Check 5: experience_level valid values ─────────────────────────────────
    valid_exp_levels = {"junior", "mid", "senior", "executive"}
    if "experience_level" in df.columns:
        invalid = df[~df["experience_level"].isin(valid_exp_levels)]["experience_level"].unique()
        if len(invalid) > 0:
            failures.append(f"experience_level has invalid values: {invalid.tolist()}")
        else:
            passed += 1
            logger.info(f"✅ experience_level values all valid: {df['experience_level'].value_counts().to_dict()}")

    # ── Check 6: work_type valid values ───────────────────────────────────────
    valid_work_types = {"On-site", "Hybrid", "Remote"}
    if "work_type" in df.columns:
        invalid = df[~df["work_type"].isin(valid_work_types)]["work_type"].unique()
        if len(invalid) > 0:
            failures.append(f"work_type has invalid values: {invalid.tolist()}")
        else:
            passed += 1
            logger.info(f"✅ work_type values all valid: {df['work_type'].value_counts().to_dict()}")

    # ── Check 7: company_type valid values ────────────────────────────────────
    valid_company_types = {"MNC", "Corporate", "Startup", "Government", "Unknown"}
    if "company_type" in df.columns:
        invalid = df[~df["company_type"].isin(valid_company_types)]["company_type"].unique()
        if len(invalid) > 0:
            failures.append(f"company_type has invalid values: {invalid.tolist()}")
        else:
            passed += 1
            logger.info(f"✅ company_type values all valid: {df['company_type'].value_counts().to_dict()}")

    # ── Check 8: job_url format ────────────────────────────────────────────────
    # WHY: Ensures scraper returned real Wuzzuf URLs, not garbage.
    if "job_url" in df.columns:
        invalid_urls = df[~df["job_url"].str.startswith("https://wuzzuf.net")]
        if len(invalid_urls) > 0:
            failures.append(
                f"{len(invalid_urls)} job_urls don't start with https://wuzzuf.net"
            )
        else:
            passed += 1
            logger.info(f"✅ All job_urls are valid Wuzzuf URLs")

    # ── Check 9: Date range sanity ────────────────────────────────────────────
    # WHY: Catches date parsing bugs — if posted_date is all NULL or
    # far in the future, something went wrong in clean.py
    if "posted_date" in df.columns:
        try:
            dates      = pd.to_datetime(df["posted_date"])
            min_date   = dates.min()
            max_date   = dates.max()
            today      = pd.Timestamp.today()
            days_range = (today - min_date).days

            if days_range > 365:
                failures.append(
                    f"posted_date range too wide: {min_date} to {max_date} ({days_range} days)"
                )
            elif max_date > today:
                failures.append(f"posted_date has future dates: max={max_date}")
            else:
                passed += 1
                logger.info(f"✅ Date range valid: {min_date.date()} → {max_date.date()}")
        except Exception as e:
            failures.append(f"posted_date parsing failed: {e}")

    # ── Check 10: Skills list not all empty ───────────────────────────────────
    # WHY: If skills are all empty, the bridge_job_skills table will be empty
    # which breaks your skills analysis entirely
    if "skills_list" in df.columns:
        empty_skills = df["skills_list"].apply(
            lambda x: str(x).strip() in ["[]", "None", "nan", ""]
        ).sum()
        empty_pct = empty_skills / len(df) * 100

        if empty_pct > 80:
            failures.append(
                f"{empty_pct:.1f}% of jobs have empty skills — scraper may be broken"
            )
        else:
            passed += 1
            logger.info(f"✅ Skills present: {len(df) - empty_skills}/{len(df)} jobs have skills")

    # ── Save validation report ────────────────────────────────────────────────
    report_dir = Path("data/validation_reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"validation_{timestamp}.json"

    import json
    report = {
        "timestamp"      : timestamp,
        "parquet_path"   : str(parquet_path),
        "total_rows"     : len(df),
        "checks_passed"  : passed,
        "checks_failed"  : len(failures),
        "failures"       : failures,
        "status"         : "PASSED" if not failures else "FAILED",
    }
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    # ── Final result ──────────────────────────────────────────────────────────
    logger.info("-" * 55)
    logger.info(f"Checks passed : {passed}")
    logger.info(f"Checks failed : {len(failures)}")
    logger.info(f"Report saved  → {report_path}")
    logger.info("-" * 55)

    if failures:
        logger.error("VALIDATION FAILED ❌")
        for f in failures:
            logger.error(f"  ✗ {f}")
        raise ValueError(
            f"Validation failed with {len(failures)} issue(s):\n" +
            "\n".join(f"  - {f}" for f in failures)
        )

    logger.info("VALIDATION PASSED ✅")
    logger.info("=" * 55)
    return True


# ── Local Testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_validation(
        parquet_path=Path("data/processed/jobs_cleaned.parquet")
    )