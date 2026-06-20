import json
import logging
import re
import ast
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


TECH_SKILLS_VOCAB = {
    # Languages
    "python", "sql", "r ", " r,", "java", "scala", "go ", "golang", "rust",
    "c++", "c#", ".net", "php", "ruby", "bash", "shell", "powershell",
    "typescript", "javascript", "kotlin", "swift", "matlab", "julia",

    # Data & Analytics
    "pandas", "numpy", "spark", "hadoop", "kafka", "airflow", "dbt",
    "snowflake", "redshift", "bigquery", "databricks", "hive", "presto",
    "flink", "nifi", "luigi", "prefect", "dagster",

    # Databases
    "postgresql", "postgres", "mysql", "oracle", "sql server", "mongodb",
    "redis", "cassandra", "elasticsearch", "neo4j", "dynamodb", "sqlite",
    "mssql", "mariadb", "cockroachdb",

    # BI & Visualization
    "power bi", "tableau", "looker", "qlik", "metabase", "superset",
    "grafana", "kibana", "plotly", "matplotlib", "seaborn", "d3",

    # Cloud & DevOps
    "aws", "azure", "gcp", "google cloud", "docker", "kubernetes", "k8s",
    "terraform", "ansible", "jenkins", "gitlab", "github actions", "ci/cd",
    "linux", "ubuntu", "helm", "prometheus", "datadog", "cloudwatch",

    # ML & AI
    "machine learning", "deep learning", "nlp", "computer vision",
    "tensorflow", "pytorch", "keras", "scikit", "sklearn", "xgboost",
    "lightgbm", "hugging face", "transformers", "llm", "openai", "langchain",
    "mlflow", "kubeflow", "sagemaker", "vertex ai", "feature store",
    "reinforcement learning", "neural network", "bert", "gpt",

    # ETL & Engineering
    "etl", "elt", "data pipeline", "data warehouse", "data lake",
    "data lakehouse", "data modeling", "data mesh", "data governance",
    "data quality", "great expectations", "dbt", "fivetran", "stitch",
    "airbyte", "talend", "informatica", "ssis", "pentaho",

    # Software Engineering
    "rest api", "graphql", "microservices", "api", "git", "agile", "scrum",
    "django", "flask", "fastapi", "spring", "laravel", "node", "react",
    "angular", "vue", "flutter", "android", "ios", "docker", "oauth",

    # ERP & Enterprise (keep only tech-adjacent ones)
    "sap", "odoo", "dynamics", "erp", "crm", "salesforce", "servicenow",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)



def load_raw_json(input_path: Path) -> pd.DataFrame:
    logger.info(f"Loading raw JSON from {input_path}")

    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    jobs = data.get("jobs", [])
    if not jobs:
        raise ValueError(f"No jobs found in {input_path}")
    normalized = []
    for job in jobs:
        normalized.append({
            "job_title"     : job.get("title", ""),
            "company_name"  : job.get("company", "Confidential"),
            "job_type"      : job.get("job_type", "Full Time"),
            "experience_raw": job.get("experience", "Not Specified"),
            "skills_list"   : job.get("skills", []),
            "job_category"  : job.get("keyword", ""),   # search keyword as initial category
            "posted_date"   : job.get("posted_date", ""),
            "job_url"       : job.get("url", ""),
            "city"          : _extract_city(job.get("location", "Egypt")),
            "work_type"     : _extract_work_type(job.get("location", ""), job.get("job_type", "")),
            "scraped_at"    : job.get("scraped_at", ""),
        })

    df = pd.DataFrame(normalized)
    logger.info(f"Loaded {len(df)} jobs from JSON")
    return df


def _extract_city(location: str) -> str:
    """
    Extract city from Wuzzuf location string.
    Wuzzuf returns locations like: 'Cairo, Egypt, On-site' or 'Giza, Egypt'
    We want just the city: 'Cairo' or 'Giza'
    """
    if not location or location == "Egypt":
        return "Cairo"  # default

    parts = [p.strip() for p in location.split(",")]
    # Remove known non-city parts
    non_city = {"egypt", "on-site", "hybrid", "remote", ""}
    city_parts = [p for p in parts if p.lower() not in non_city]

    return city_parts[0] if city_parts else "Cairo"


def _extract_work_type(location: str, job_type: str) -> str:
    """
    Extract work type (On-site/Hybrid/Remote) from location or job_type strings.
    Wuzzuf sometimes puts work mode in the location span.
    """
    combined = f"{location} {job_type}".lower()
    if "remote" in combined:
        return "Remote"
    if "hybrid" in combined:
        return "Hybrid"
    return "On-site"  # default


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — CLEAN JOB TITLES
# ══════════════════════════════════════════════════════════════════════════════

def clean_job_titles(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Strips whitespace, normalizes spacing in job titles.

    WHY:
        Scraped titles sometimes have extra spaces, newlines, or
        trailing punctuation. Clean titles = better matching downstream.
    """
    logger.info("Cleaning job titles...")
    before_nulls = df["job_title"].isna().sum()

    df["job_title"] = (
        df["job_title"]
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)   # collapse multiple spaces
    )

    # Drop rows with no meaningful title
    df = df[df["job_title"].str.len() > 2].copy()

    logger.info(f"  Dropped {before_nulls} null titles. Remaining: {len(df)}")
    return df



def filter_egypt_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Removes jobs that are not based in Egypt.

    WHY:
        Wuzzuf lists international jobs — same role posted across
        multiple UK/UAE/Saudi cities inflates counts and corrupts analysis.
        This project specifically analyzes Egypt's job market.

    HOW:
        Check job_url — Egyptian jobs always contain Egyptian cities.
        Check city — known non-Egyptian cities get dropped.
        Check company_name pattern for repeated international postings.
    """
    logger.info("Filtering Egypt-only jobs...")
    before = len(df)

    # Known non-Egyptian city indicators in URLs
    non_egypt_url_patterns = [
        "london", "manchester", "liverpool", "dundee", "chesterfield",
        "united-kingdom", "riyadh", "dubai", "abu-dhabi", "doha",
        "kuwait", "jeddah", "khobar", "saudi", "qatar", "uae"
    ]

    def is_egypt_job(row):
        url  = str(row["job_url"]).lower()
        city = str(row["city"]).lower()

        # Check URL for non-Egyptian indicators
        if any(pattern in url for pattern in non_egypt_url_patterns):
            return False

        # Check city
        non_egypt_cities = {
            "london", "manchester", "liverpool", "dundee", "chesterfield",
            "city of london", "riyadh", "dubai", "abu dhabi", "doha",
            "kuwait city", "jeddah", "khobar",
        }
        if city.lower() in non_egypt_cities:
            return False

        return True

    df = df[df.apply(is_egypt_job, axis=1)].copy()
    df = df.reset_index(drop=True)

    logger.info(f"  Removed {before - len(df)} non-Egypt jobs. Remaining: {len(df)}")
    return df
# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — PARSE EXPERIENCE
# ══════════════════════════════════════════════════════════════════════════════

def parse_experience_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Extracts min/max years from experience_raw text.

    WHY:
        experience_raw contains strings like:
        - "2 - 4 Yrs of Exp"  → min=2, max=4
        - "2+ Yrs of Exp"     → min=2, max=None
        - "0 To 1 Year"       → min=0, max=1
        - "Not Specified"     → min=None, max=None

        We need numeric columns for analysis and star schema.
    """
    logger.info("Parsing experience years...")

    def extract_min_max(raw: str):
        raw = str(raw).lower().strip()

        # Pattern: "2 - 4 yrs" or "2 to 4 yrs"
        range_match = re.search(r"(\d+)\s*[-to]+\s*(\d+)", raw)
        if range_match:
            return float(range_match.group(1)), float(range_match.group(2))

        # Pattern: "2+ yrs" or "more than 2"
        plus_match = re.search(r"(\d+)\+|more than (\d+)", raw)
        if plus_match:
            val = plus_match.group(1) or plus_match.group(2)
            return float(val), None

        # Pattern: "0 to 1 year"
        zero_match = re.search(r"0\s*to\s*(\d+)", raw)
        if zero_match:
            return 0.0, float(zero_match.group(1))

        return None, None

    min_exp, max_exp = zip(*df["experience_raw"].map(extract_min_max))
    df["min_experience"] = list(min_exp)
    df["max_experience"] = list(max_exp)

    logger.info(
        f"  min_experience nulls: {df['min_experience'].isna().sum()} / {len(df)}"
    )
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — CLASSIFY EXPERIENCE LEVEL
# ══════════════════════════════════════════════════════════════════════════════

def classify_experience_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Assigns junior/mid/senior/executive to each job.

    WHY TITLE-FIRST LOGIC:
        Title keywords are the most reliable signal.
        "Middle Data Engineer" should be mid — not senior.
        "Middle" is an Egyptian market term for mid-level.
        Year-based fallback handles jobs with no level keyword in title.
    """
    logger.info("Classifying experience levels...")

    def classify(row):
        title   = str(row["job_title"]).lower()
        min_exp = row["min_experience"]
        raw     = str(row["experience_raw"]).lower()

        executive_kw = ["director", "vp ", "vice president", "c-level", "cto", "cdo", "head of", "manager"]
        senior_kw    = ["senior", "sr.", "sr ", "lead", "principal", "staff", "expert", "section head"]
        mid_kw       = ["middle", "mid ", "mid-", "associate"]
        junior_kw    = ["junior", "jr.", "jr ", "entry", "fresh", "graduate", "intern", "trainee", "assistant"]

        if any(k in title for k in executive_kw): return "executive"
        if any(k in title for k in senior_kw):    return "senior"
        if any(k in title for k in mid_kw):       return "mid"
        if any(k in title for k in junior_kw):    return "junior"

        # Fallback: years of experience
        if pd.notna(min_exp):
            if min_exp <= 2:  return "junior"
            elif min_exp <= 4: return "mid"
            elif min_exp <= 7: return "senior"
            else:              return "executive"

        # Raw text hints
        if any(k in raw for k in ["intern", "fresh", "0 to 1"]):
            return "junior"

        return "mid"  # safe default

    df["experience_level"] = df.apply(classify, axis=1)

    logger.info(f"  Distribution:\n{df['experience_level'].value_counts().to_string()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — CONVERT POSTED DATE
# ══════════════════════════════════════════════════════════════════════════════

def convert_posted_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Converts relative dates → absolute dates.
          Adds scrape_date column.

    WHY:
        Wuzzuf shows "posted 2 days ago" not "posted 2026-04-24".
        For time-series analysis (hiring velocity) we need real dates.
        We derive the absolute date using scrape_date - offset.

    WHY SCRAPE_DATE FROM scraped_at:
        scraped_at is an ISO timestamp from the scraper.
        We parse it to get the scrape date as the reference point.
    """
    logger.info("Converting posted dates...")

    # Get scrape date from scraped_at column
    # Use today as fallback if scraped_at is missing
    try:
        sample_ts  = df["scraped_at"].dropna().iloc[0]
        scrape_date = datetime.fromisoformat(sample_ts).date()
    except (IndexError, ValueError):
        scrape_date = datetime.today().date()

    logger.info(f"  Scrape date: {scrape_date}")

    def parse_date(raw: str) -> str:
        raw = str(raw).lower().strip()

        match_hours  = re.search(r"(\d+)\s+hour",  raw)
        match_days   = re.search(r"(\d+)\s+day",   raw)
        match_weeks  = re.search(r"(\d+)\s+week",  raw)
        match_months = re.search(r"(\d+)\s+month", raw)

        if match_hours:
            return str(scrape_date)
        if match_days:
            return str(scrape_date - timedelta(days=int(match_days.group(1))))
        if match_weeks:
            return str(scrape_date - timedelta(weeks=int(match_weeks.group(1))))
        if match_months:
            return str(scrape_date - timedelta(days=int(match_months.group(1)) * 30))

        # Try parsing as actual date string
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
            try:
                return str(datetime.strptime(raw, fmt).date())
            except ValueError:
                continue

        return str(scrape_date)  # fallback to scrape date

    df["posted_date"] = df["posted_date"].apply(parse_date)
    df["posted_date"] = pd.to_datetime(df["posted_date"])
    df["scrape_date"] = pd.to_datetime(str(scrape_date))

    logger.info(f"  Date range: {df['posted_date'].min()} → {df['posted_date'].max()}")
    logger.info(f"  Null dates: {df['posted_date'].isna().sum()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — CLEAN SKILLS LIST
# ══════════════════════════════════════════════════════════════════════════════

def clean_skills_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Cleans the skills list — removes noise AND filters to tech domain only.

    WHY TWO-STAGE FILTERING:
        Stage 1 (existing): Remove sentences, soft skills, job titles.
        Stage 2 (new): Keep only skills that match TECH_SKILLS_VOCAB.
        This eliminates "Stakeout", "QuickBooks", "Calendar Management"
        while keeping "Python", "Power BI", "Apache Spark".

    WHY CONTAINS NOT EXACT MATCH:
        "Apache Spark" should match "spark".
        "scikit-learn" should match "scikit".
        Exact match would miss these compound skill names.
    """
    logger.info("Cleaning skills lists...")

    noise_skills = {
        "english", "arabic", "communication", "communication skills",
        "icdl", "microsoft office", "problem solving", "teamwork",
        "time management", "leadership", "negotiation", "research",
        "team collaboration", "self-starter", "learning", "ms office",
    }

    title_keywords = ["junior", "senior", "middle", "engineer", "developer", "analyst", "manager"]

    def is_tech_skill(skill: str) -> bool:
        """Returns True if skill matches any entry in TECH_SKILLS_VOCAB."""
        skill_lower = skill.lower()
        return any(vocab_term in skill_lower for vocab_term in TECH_SKILLS_VOCAB)

    def clean_skills(raw):
        if isinstance(raw, list):
            skills = raw
        else:
            try:
                skills = ast.literal_eval(str(raw))
            except Exception:
                return []

        cleaned = []
        for skill in skills:
            skill = str(skill).strip()

            # Stage 1 — structural filters (same as before)
            if len(skill) > 50:
                continue
            if any(m in skill for m in ["✔", "•", "·", "–", "->", " and ", " who "]):
                continue
            if skill.lower() in noise_skills:
                continue
            if sum(1 for k in title_keywords if k in skill.lower()) >= 2:
                continue

            # Stage 2 — tech domain filter (new)
            if not is_tech_skill(skill):
                logger.debug(f"  Dropped non-tech skill: {skill!r}")
                continue

            cleaned.append(skill)

        return cleaned

    df["skills_list"] = df["skills_list"].apply(clean_skills)

    empty = df["skills_list"].apply(lambda x: len(x) == 0).sum()
    logger.info(f"  Empty skills lists after tech filter: {empty} / {len(df)}")
    return df
# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — CLASSIFY JOB CATEGORY
# ══════════════════════════════════════════════════════════════════════════════

def classify_job_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Assigns a clean job category based on job title keywords.

    WHY TITLE-BASED NOT KEYWORD-BASED:
        The scraper's keyword field reflects which search query found the job —
        not what the job actually is. A Laravel developer scraped under
        "analytics engineer" would get wrong category from keyword alone.
        Title-based classification is more accurate.
    """
    logger.info("Classifying job categories...")

    def classify(title: str) -> str:
        t = str(title).lower()

        if any(k in t for k in ["data engineer", "etl", "data pipeline", "data warehouse", "data platform", "data governance", "master data"]):
            return "data engineer"
        if any(k in t for k in ["data analyst", "data analysis", "reporting analyst", "mis analyst", "research analyst", "process analyst"]):
            return "data analyst"
        if any(k in t for k in ["data scientist", "data science"]):
            return "data scientist"
        if any(k in t for k in ["machine learning", "ml engineer"]):
            return "machine learning engineer"
        if any(k in t for k in ["ai engineer", "ai developer", "artificial intelligence", "aiops", "prompt engineer", "ai automation"]):
            return "ai engineer"
        if any(k in t for k in ["bi engineer", "bi developer", "power bi", "tableau", "looker", "obiee", "analytics engineer"]):
            return "bi developer"
        if any(k in t for k in ["business analyst", "business analysis", "business intelligence", "product owner"]):
            return "business analyst"
        if any(k in t for k in ["database", "dba", "oracle", "erp", "odoo", "sap", "dynamics 365", "netsuite", "sql developer"]):
            return "database & erp"
        if any(k in t for k in ["backend", "back-end", "laravel", "django", ".net", "php developer", "java developer", "golang", "python developer"]):
            return "backend developer"
        if any(k in t for k in ["frontend", "front-end", "react", "angular", "vue", "javascript developer"]):
            return "frontend developer"
        if any(k in t for k in ["android", "ios developer", "flutter", "mobile developer"]):
            return "mobile developer"
        if any(k in t for k in ["software engineer", "software developer", "full stack", "fullstack", "qa engineer", "quality assurance engineer", "automation qa", "tester"]):
            return "software engineer"
        if any(k in t for k in ["devops", "cloud engineer", "network engineer", "infrastructure", "splunk", "embedded", "it engineer", "it specialist", "helpdesk", "technical support"]):
            return "devops & infrastructure"
        if any(k in t for k in ["security engineer", "cyber security", "cybersecurity", "soc ", "noc "]):
            return "cybersecurity"
        if any(k in t for k in ["product manager", "product designer", "ux designer", "scrum master"]):
            return "product & design"
        if any(k in t for k in ["accountant", "financial", "fp&a", "audit", "budget", "credit"]):
            return "finance & accounting"
        if any(k in t for k in ["hr ", "human resource", "talent acquisition", "recruitment", "payroll"]):
            return "human resources"
        if any(k in t for k in ["sales", "marketing", "media buyer", "copywriter", "social media", "b2b sales"]):
            return "sales & marketing"
        if any(k in t for k in ["supply chain", "logistics", "procurement", "demand planner", "purchasing"]):
            return "supply chain & logistics"
        if any(k in t for k in ["civil", "mechanical", "electrical engineer", "hvac", "structural", "production engineer", "site engineer", "planning engineer", "tender", "construction"]):
            return "engineering"

        return "other"

    df["job_category"] = df["job_title"].apply(classify)

    logger.info(f"  Distribution:\n{df['job_category'].value_counts().head(10).to_string()}")
    logger.info(f"  'other' count: {(df['job_category'] == 'other').sum()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — CLASSIFY COMPANY TYPE
# ══════════════════════════════════════════════════════════════════════════════

def classify_company_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Assigns MNC/Corporate/Startup/Government/Unknown to each company.

    WHY:
        One of your key analytical questions is "do startups or MNCs hire more?"
        This column enables that analysis in Power BI.

    HOW:
        1. Confidential → Unknown (company chose to hide name)
        2. Known MNC names → MNC
        3. Government keywords → Government
        4. Startup signals (tech words, short name) → Startup
        5. Everything else → Corporate
    """
    logger.info("Classifying company types...")

    known_mncs = [
        "hyundai", "samsung", "huawei", "oppo", "realme", "oracle", "sap",
        "microsoft", "google", "amazon", "meta", "ibm", "cisco", "dell",
        "siemens", "abb ", "schneider", "honeywell", "saint gobain",
        "electrolux", "arcelormittal", "sumitomo", "docusign", "pwc",
        "deloitte", "kpmg", "ernst", "mckinsey", "atkins", "dabur",
        "unilever", "nestle", "pepsi", "coca cola", "henkel", "bayer",
        "almarai", "beyti", "spinneys", "circle k", "miniso", "evyap",
        "groupe atlantic", "aerzen", "prometeon", "intouch", "ttec",
        "sana commerce", "idealratings", "sinoma", "gulsan", "egabi",
        "gb corp", "raya",
    ]

    government_keywords = [
        "itida", "egec", "neric", "e finance", "erada", "itac", "egic", "isfp", "icpm",
    ]

    startup_keywords = [
        "ai", "tech", "digital", "soft", "solutions", "startup", "platform",
        "app", "cloud", "data", "labs", "studio", "ventures", "innovation",
        "dev", "bot", "smart", "fintech", "edtech", "healthtech", "saas",
    ]

    # Manual overrides from original cleaning
    overrides = {
        "Egyptian Company for Cosmetics"        : "Corporate",
        "Dutch Egyptian Capri"                  : "Corporate",
        "Sites International"                   : "Corporate",
        "National Technology Group"             : "Corporate",
        "National technology group"             : "Corporate",
        "B Tech"                                : "Corporate",
        "Pachin For Paints"                     : "Corporate",
        "Regal Home Appliances"                 : "Corporate",
    }

    def classify(name: str) -> str:
        if not name or str(name).strip().lower() in ["confidential", "unknown", ""]:
            return "Unknown"

        # Manual overrides first
        if name in overrides:
            return overrides[name]

        name_lower = str(name).lower()

        if any(mnc in name_lower for mnc in known_mncs):
            return "MNC"
        if any(gov in name_lower for gov in government_keywords):
            return "Government"

        startup_score = sum(1 for k in startup_keywords if k in name_lower)
        if startup_score >= 2:
            return "Startup"
        if startup_score == 1 and len(name.split()) <= 3:
            return "Startup"

        return "Corporate"

    df["company_type"] = df["company_name"].apply(classify)

    logger.info(f"  Distribution:\n{df['company_type'].value_counts().to_string()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — DROP REDUNDANT COLUMNS & DEDUPLICATE
# ══════════════════════════════════════════════════════════════════════════════

def finalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHAT: Final cleanup before saving.
        - Drop columns not needed downstream
        - Deduplicate by job_url
        - Reset index

    WHY DEDUPLICATE HERE:
        Even after scraper deduplication, the same job can appear if:
        - It was in a previous scrape run (loaded from old JSON)
        - Two keywords found it and upsert didn't catch it
        This is a safety net.
    """
    logger.info("Finalizing dataframe...")

    before = len(df)

    # Drop redundant columns
    # WHY DROP experience_raw:
    #   We extracted min/max from it — the raw string adds no value to analysis
    # WHY DROP scraped_at:
    #   scrape_date (date only) is what we need — full timestamp is redundant
    cols_to_drop = ["experience_raw", "scraped_at"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # Deduplicate by job_url — keep last (most recently scraped version)
    df = df.drop_duplicates(subset=["job_url"], keep="last")

    # Reset index
    df = df.reset_index(drop=True)

    # Convert skills_list to string for Parquet/Snowflake compatibility
    # WHY STRING: Snowflake VARCHAR can store list as string, FLATTEN handles it later
    df["skills_list"] = df["skills_list"].apply(
        lambda x: str(x) if isinstance(x, list) else x
    )

    logger.info(f"  Removed {before - len(df)} duplicates. Final rows: {len(df)}")
    logger.info(f"  Final columns: {df.columns.tolist()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 10 — SAVE AS PARQUET
# ══════════════════════════════════════════════════════════════════════════════

def save_as_parquet(df: pd.DataFrame, output_path: Path) -> Path:
    """
    WHAT: Saves cleaned DataFrame as Parquet file.

    WHY PARQUET:
        - Columnar format — Snowflake reads only columns it needs
        - Compressed automatically — 5-10x smaller than CSV
        - Preserves dtypes — dates stay dates, not strings
        - Industry standard for data pipelines

    WHY NOT CSV:
        CSV loses type information — dates become strings, nulls become "None".
        Every load then needs type casting which adds complexity and risk.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False, engine="pyarrow")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"Saved Parquet → {output_path} ({size_mb:.2f} MB, {len(df)} rows)")
    return output_path


# ══════════════════════════════════════════════════════════════════════════════
# MASTER FUNCTION — Airflow Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def run_cleaning_pipeline(
    input_path : Path = Path("data/raw/wuzzuf_combined.json"),
    output_path: Path = Path("data/processed/jobs_cleaned.parquet"),
) -> pd.DataFrame:
    """
    WHAT: Runs the full cleaning pipeline end to end.
          This is the single function Airflow calls.

    WHY ONE MASTER FUNCTION:
        Airflow tasks should be simple — one function call per task.
        This function orchestrates all cleaning steps in the right order
        and handles logging so Airflow's UI shows progress clearly.

    RETURNS:
        Cleaned DataFrame (also saved as Parquet at output_path)

    AIRFLOW USAGE:
        from pipeline.clean import run_cleaning_pipeline
        df = run_cleaning_pipeline(
            input_path=Path("/opt/airflow/data/raw/wuzzuf_combined.json"),
            output_path=Path("/opt/airflow/data/processed/jobs_cleaned.parquet")
        )
    """
    logger.info("=" * 55)
    logger.info("CLEANING PIPELINE STARTED")
    logger.info("=" * 55)

    # Run all steps in order
    df = load_raw_json(input_path)
    df = clean_job_titles(df)
    df = filter_egypt_only(df) 
    df = parse_experience_years(df)
    df = classify_experience_level(df)
    df = convert_posted_date(df)
    df = clean_skills_list(df)
    df = classify_job_category(df)
    df = classify_company_type(df)
    df = finalize_dataframe(df)
    save_as_parquet(df, output_path)

    logger.info("=" * 55)
    logger.info(f"CLEANING PIPELINE COMPLETE — {len(df)} clean jobs")
    logger.info("=" * 55)

    return df


# ── Local Testing ─────────────────────────────────────────────────────────────
# WHY THIS BLOCK:
#   Lets you test the pipeline locally without Airflow.
#   python pipeline/clean.py
if __name__ == "__main__":
    df = run_cleaning_pipeline(
        input_path =Path("data/raw/wuzzuf_combined.json"),
        output_path=Path("data/processed/jobs_cleaned.parquet"),
    )
    print(f"\n✔ Done — {len(df)} clean jobs")
    print(f"\nSample:\n{df.head(3).to_string()}")
    print(f"\nColumns: {df.columns.tolist()}")
    print(f"\nNull counts:\n{df.isnull().sum().to_string()}")