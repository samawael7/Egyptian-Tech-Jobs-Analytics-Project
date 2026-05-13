"""
pipeline/load.py
─────────────────
WHAT:
    Loads cleaned Parquet data into Snowflake raw_jobs table.
    Uses MERGE (upsert) — only inserts new jobs, updates existing ones.

WHY MERGE NOT INSERT:
    After daily runs you'll have thousands of jobs.
    INSERT would create duplicates — same job added every day it's still posted.
    MERGE checks job_url first:
        - If URL already exists → UPDATE (refresh scraped_at, skills, etc.)
        - If URL is new → INSERT
    Result: each job exists exactly once, always with latest data.

WHY NOT OVERWRITE THE WHOLE TABLE:
    After 6 months you'll have 5000+ jobs in Snowflake.
    Overwriting daily means deleting 5000 rows to re-add them.
    MERGE only touches the ~30-50 new/changed rows per day.
    Much faster, much cheaper (less Snowflake compute credits).

HOW AIRFLOW USES THIS:
    from pipeline.load import load_to_snowflake
    load_to_snowflake(
        parquet_path=Path("/opt/airflow/data/processed/jobs_cleaned.parquet")
    )
"""
from dotenv import load_dotenv
load_dotenv()

import logging
import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Snowflake Connection Config ───────────────────────────────────────────────
# WHY ENVIRONMENT VARIABLES NOT HARDCODED:
#   Hardcoding credentials = security risk if you push to GitHub.
#   Environment variables are set in Airflow's .env file (gitignored).
#   Locally you can set them in your terminal or a .env file.
SNOWFLAKE_CONFIG = {
    "account"  : os.getenv("SNOWFLAKE_ACCOUNT",   "jsgtohn-dcb76328"),
    "user"     : os.getenv("SNOWFLAKE_USER",       "samawael"),
    "password" : os.getenv("SNOWFLAKE_PASSWORD",   ""),   
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE",  "dev_wh"),
    "database" : os.getenv("SNOWFLAKE_DATABASE",   "egypt_jobs_db"),
    "schema"   : os.getenv("SNOWFLAKE_SCHEMA",     "raw"),
    "role"     : os.getenv("SNOWFLAKE_ROLE",       "ACCOUNTADMIN"),
}

# Table names
RAW_TABLE   = "egypt_jobs_db.raw.raw_jobs"
STAGE_TABLE = "egypt_jobs_db.raw.raw_jobs_stage"


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

def get_connection():
    """
    WHAT: Creates and returns a Snowflake connection.

    WHY A FUNCTION NOT A GLOBAL:
        Global connections stay open forever and can time out.
        A function creates a fresh connection each time — safer and cleaner.
        Airflow tasks are short-lived so connection overhead is negligible.
    """
    password = SNOWFLAKE_CONFIG["password"]
    if not password:
        raise ValueError(
            "SNOWFLAKE_PASSWORD environment variable not set. "
            "Set it with: set SNOWFLAKE_PASSWORD=yourpassword (Windows) "
            "or export SNOWFLAKE_PASSWORD=yourpassword (Mac/Linux)"
        )

    logger.info(f"Connecting to Snowflake: {SNOWFLAKE_CONFIG['account']}")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    logger.info("Connected ✅")
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# LOAD PARQUET
# ══════════════════════════════════════════════════════════════════════════════

def load_parquet(parquet_path: Path) -> pd.DataFrame:
    """
    WHAT: Reads the cleaned Parquet file into a DataFrame.
    WHY: Parquet preserves types — dates stay dates, nulls stay nulls.
    """
    logger.info(f"Reading Parquet from {parquet_path}")
    df = pd.read_parquet(parquet_path, engine="pyarrow")

    # Convert dates to strings for Snowflake compatibility
    # WHY STRING: Snowflake connector handles string→DATE casting better
    # than passing Python datetime objects directly
    for col in ["posted_date", "scrape_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Convert skills_list to string if it's not already
    if "skills_list" in df.columns:
        df["skills_list"] = df["skills_list"].astype(str)

    # Uppercase column names
    # WHY: Snowflake stores column names in uppercase by default.
    # Matching case prevents "column not found" errors.
    df.columns = [c.upper() for c in df.columns]

    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# MERGE (UPSERT)
# ══════════════════════════════════════════════════════════════════════════════

def merge_into_snowflake(df: pd.DataFrame, conn) -> dict:
    """
    Upserts DataFrame into Snowflake raw_jobs using MERGE.
    """
    cursor = conn.cursor()

    try:
        # ── Set context explicitly ─────────────────────────────────────────
        # WHY: write_pandas uses the connection's current database/schema.
        # Setting context ensures staging table lands in the right place.
        cursor.execute("USE WAREHOUSE dev_wh")
        cursor.execute("USE DATABASE egypt_jobs_db")
        cursor.execute("USE SCHEMA raw")

        # ── Step 1: Drop old staging table ────────────────────────────────
        logger.info("Dropping old staging table if exists...")
        cursor.execute("DROP TABLE IF EXISTS raw_jobs_stage")

        # ── Step 2: Write to staging table ────────────────────────────────
        logger.info(f"Writing {len(df)} rows to staging table...")

        success, num_chunks, num_rows, output = write_pandas(
            conn           = conn,
            df             = df,
            table_name     = "RAW_JOBS_STAGE",   # simple name — schema set above
            auto_create_table = True,
            overwrite      = True,
        )

        if not success:
            raise RuntimeError(f"Failed to write staging table: {output}")

        logger.info(f"Staging table loaded: {num_rows} rows")

        # ── Step 3: Verify staging table exists ───────────────────────────
        cursor.execute("SELECT COUNT(*) FROM raw_jobs_stage")
        staging_count = cursor.fetchone()[0]
        logger.info(f"Staging table row count: {staging_count}")

        # ── Step 4: MERGE staging → raw_jobs ──────────────────────────────
        merge_sql = """
            MERGE INTO raw_jobs AS target
            USING raw_jobs_stage AS source
            ON target.JOB_URL = source.JOB_URL

            WHEN MATCHED THEN UPDATE SET
                target.JOB_TITLE        = source.JOB_TITLE,
                target.COMPANY_NAME     = source.COMPANY_NAME,
                target.JOB_TYPE         = source.JOB_TYPE,
                target.SKILLS_LIST      = source.SKILLS_LIST,
                target.JOB_CATEGORY     = source.JOB_CATEGORY,
                target.POSTED_DATE      = source.POSTED_DATE,
                target.CITY             = source.CITY,
                target.WORK_TYPE        = source.WORK_TYPE,
                target.MIN_EXPERIENCE   = source.MIN_EXPERIENCE,
                target.MAX_EXPERIENCE   = source.MAX_EXPERIENCE,
                target.EXPERIENCE_LEVEL = source.EXPERIENCE_LEVEL,
                target.SCRAPE_DATE      = source.SCRAPE_DATE,
                target.COMPANY_TYPE     = source.COMPANY_TYPE

            WHEN NOT MATCHED THEN INSERT (
                JOB_TITLE, COMPANY_NAME, JOB_TYPE, SKILLS_LIST,
                JOB_CATEGORY, POSTED_DATE, JOB_URL, CITY, WORK_TYPE,
                MIN_EXPERIENCE, MAX_EXPERIENCE, EXPERIENCE_LEVEL,
                SCRAPE_DATE, COMPANY_TYPE
            ) VALUES (
                source.JOB_TITLE, source.COMPANY_NAME, source.JOB_TYPE,
                source.SKILLS_LIST, source.JOB_CATEGORY, source.POSTED_DATE,
                source.JOB_URL, source.CITY, source.WORK_TYPE,
                source.MIN_EXPERIENCE, source.MAX_EXPERIENCE,
                source.EXPERIENCE_LEVEL, source.SCRAPE_DATE, source.COMPANY_TYPE
            )
        """

        logger.info("Running MERGE...")
        cursor.execute(merge_sql)
        rows_affected = cursor.rowcount
        logger.info(f"MERGE complete — {rows_affected} rows affected")

        # ── Step 5: Drop staging table ─────────────────────────────────────
        cursor.execute("DROP TABLE IF EXISTS raw_jobs_stage")
        logger.info("Staging table dropped")

        # ── Step 6: Verify final count ─────────────────────────────────────
        cursor.execute("SELECT COUNT(*) FROM raw_jobs")
        total_rows = cursor.fetchone()[0]
        logger.info(f"Total rows in raw_jobs: {total_rows}")

        return {
            "rows_affected" : rows_affected,
            "total_in_table": total_rows,
        }

    except Exception as e:
        try:
            cursor.execute("DROP TABLE IF EXISTS raw_jobs_stage")
        except Exception:
            pass
        raise e

    finally:
        cursor.close()
# ══════════════════════════════════════════════════════════════════════════════
# MASTER FUNCTION — Airflow Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def load_to_snowflake(
    parquet_path: Path = Path("data/processed/jobs_cleaned.parquet"),
) -> dict:
    """
    WHAT: Full load pipeline — reads Parquet, connects to Snowflake, MERGEs data.
          This is the single function Airflow calls.

    RETURNS:
        Dict with rows_affected and total_in_table for logging.

    AIRFLOW USAGE:
        from pipeline.load import load_to_snowflake
        result = load_to_snowflake(
            parquet_path=Path("/opt/airflow/data/processed/jobs_cleaned.parquet")
        )
    """
    logger.info("=" * 55)
    logger.info("SNOWFLAKE LOAD STARTED")
    logger.info("=" * 55)

    df   = load_parquet(parquet_path)
    conn = get_connection()

    try:
        result = merge_into_snowflake(df, conn)
    finally:
        # WHY FINALLY: connection MUST close even if merge fails
        conn.close()
        logger.info("Snowflake connection closed")

    logger.info("=" * 55)
    logger.info(f"SNOWFLAKE LOAD COMPLETE")
    logger.info(f"  Rows affected : {result['rows_affected']}")
    logger.info(f"  Total in table: {result['total_in_table']}")
    logger.info("=" * 55)

    return result


# ── Local Testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # Set your password as environment variable before running:
    # Windows: set SNOWFLAKE_PASSWORD=yourpassword
    # Then: python pipeline/load.py

    result = load_to_snowflake(
        parquet_path=Path("data/processed/jobs_cleaned.parquet")
    )
    print(f"\n✔ Done — {result['rows_affected']} rows affected")
    print(f"  Total in Snowflake: {result['total_in_table']}")