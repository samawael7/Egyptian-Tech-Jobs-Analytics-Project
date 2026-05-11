"""
pipeline/validate.py
====================
Data quality validation using Great Expectations v1.x.

Validates the cleaned Parquet file before it touches Snowflake.
If any expectation fails → raises DataValidationError → Airflow stops pipeline.

AIRFLOW USAGE:
    from pipeline.validate import run_validation
    run_validation(parquet_path=Path("data/processed/jobs_cleaned.parquet"))
"""

import logging
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import great_expectations as gx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom Exception
# ---------------------------------------------------------------------------
class DataValidationError(Exception):
    """Raised when Great Expectations validation fails."""
    pass


# ---------------------------------------------------------------------------
# Report Helper
# ---------------------------------------------------------------------------
def _save_validation_report(results: dict, report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"validation_{timestamp}.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"  Validation report saved → {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# Core Validation Logic
# ---------------------------------------------------------------------------
def run_validation(
    parquet_path: Path = Path("data/processed/jobs_cleaned.parquet"),
    report_dir: Path = Path("data/validation_reports"),
) -> bool:
    """
    Run all Great Expectations v1.x checks on the cleaned Parquet file.

    GE v1.x uses the "fluent" API:
        context → add_pandas_datasource → add_dataframe_asset
        → add_batch_definition → get_batch → add_expectations → validate

    Returns True on success, raises DataValidationError on failure.
    """
    logger.info("=" * 55)
    logger.info("VALIDATION STARTED")
    logger.info("=" * 55)

    # ------------------------------------------------------------------
    # 1. Load data
    # ------------------------------------------------------------------
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}\n"
            "Run pipeline/clean.py first."
        )

    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} rows from {parquet_path}")

    # ------------------------------------------------------------------
    # 2. GE v1.x context + datasource (ephemeral = no files written)
    # ------------------------------------------------------------------
    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_pandas("pipeline_datasource")
    data_asset = datasource.add_dataframe_asset("cleaned_jobs")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # ------------------------------------------------------------------
    # 3. Build Expectation Suite
    # ------------------------------------------------------------------
    suite = context.suites.add(gx.ExpectationSuite(name="egypt_jobs_suite"))

    # Row count
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=10, max_value=5000)
    )

    # Required columns exist
    required_columns = [
        "job_title", "company_name", "job_type", "skills_list",
        "job_category", "posted_date", "job_url", "city", "work_type",
        "min_experience", "max_experience", "experience_level",
        "scrape_date", "company_type",
    ]
    for col in required_columns:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

    # Critical fields: never null
    for col in ["job_url", "job_title", "company_name", "job_category", "scrape_date"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )

    # job_url: unique
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="job_url")
    )

    # job_url: correct format
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="job_url",
            regex=r"^https://wuzzuf\.net/jobs/",
            mostly=0.95,
        )
    )

    # experience_level: valid categories
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="experience_level",
            value_set=["junior", "mid", "senior", "executive"],
        )
    )

    # work_type: valid categories
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="work_type",
            value_set=["On-site", "Remote", "Hybrid", "Unknown"],
        )
    )

    # company_type: valid categories
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="company_type",
            value_set=["Corporate", "Startup", "MNC", "Government", "Unknown"],
        )
    )

    # job_type: valid categories
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="job_type",
            value_set=["Full Time", "Part Time", "Freelance / Project", "Internship"],
            mostly=0.90,
        )
    )

    # min_experience: sane range when present
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="min_experience",
            min_value=0,
            max_value=30,
            mostly=0.95,
        )
    )

    # ------------------------------------------------------------------
    # 4. Validate
    # ------------------------------------------------------------------
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="egypt_jobs_validation",
            data=batch_definition,
            suite=suite,
        )
    )

    results = validation_definition.run(batch_parameters={"dataframe": df})

    # ------------------------------------------------------------------
    # 5. Parse results
    # ------------------------------------------------------------------
    total      = results.statistics["evaluated_expectations"]
    successful = results.statistics["successful_expectations"]
    failed     = results.statistics["unsuccessful_expectations"]
    success    = results.success

    logger.info("-" * 55)
    logger.info(f"Expectations evaluated : {total}")
    logger.info(f"Passed                 : {successful}")
    logger.info(f"Failed                 : {failed}")
    logger.info("-" * 55)

    if not success:
        logger.error("FAILED EXPECTATIONS:")
        for result in results.results:
            if not result.success:
                col = getattr(result.expectation_config, "column", "table-level")
                logger.error(f"  ✗ {result.expectation_config.type} | column: {col}")

    # Save report
    report_summary = {
        "timestamp": datetime.now().isoformat(),
        "parquet_path": str(parquet_path),
        "row_count": len(df),
        "total_expectations": total,
        "passed": successful,
        "failed": failed,
        "success": success,
    }
    _save_validation_report(report_summary, report_dir)

    # ------------------------------------------------------------------
    # 6. Raise on failure
    # ------------------------------------------------------------------
    if not success:
        logger.error("=" * 55)
        logger.error("VALIDATION FAILED — pipeline stopped")
        logger.error("=" * 55)
        raise DataValidationError(
            f"Validation failed: {failed}/{total} expectations did not pass. "
            f"Check report in {report_dir}"
        )

    logger.info("=" * 55)
    logger.info("VALIDATION PASSED ✅")
    logger.info("=" * 55)
    return True


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_validation(
        parquet_path=Path("data/processed/jobs_cleaned.parquet"),
        report_dir=Path("data/validation_reports"),
    )