"""
scraper/wuzzuf_scraper.py
─────────────────────────
Scrapes job listings from Wuzzuf.net across multiple keywords.
Phase 1 of the Egyptian Job Market Analytics Pipeline.

WHAT THIS FILE DOES:
    1. Opens a headless browser using Playwright
    2. Searches Wuzzuf for each keyword in SEARCH_KEYWORDS
    3. Extracts job data from each result card
    4. Filters irrelevant jobs using TECH_TITLE_KEYWORDS
    5. Deduplicates across keywords (same job can appear in multiple searches)
    6. Saves results as JSON (one file per keyword + one combined file)
    7. Returns all jobs as a list for the pipeline to use

WHY PLAYWRIGHT NOT SELENIUM:
    Wuzzuf uses CSS-in-JS (Emotion) which generates hashed class names
    (e.g. css-1gatmva) that change on every frontend deployment.
    Playwright uses structural/semantic selectors that survive these changes.
    Selenium used hardcoded class names that broke constantly.

HOW AIRFLOW USES THIS:
    from scraper.wuzzuf_scraper import scrape_all_keywords
    jobs = scrape_all_keywords(output_dir=Path("data/raw"), max_pages=5)
"""

import json
import logging
import time
import re
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL      = "https://wuzzuf.net/search/jobs/"
DELAY_SECONDS = 2          # polite delay between pages to avoid rate limiting
REQUEST_TIMEOUT = 15       # seconds before giving up on a page load

# ── Search Keywords ───────────────────────────────────────────────────────────
# WHY THIS LIST:
#   These are the exact roles we want to track in Egypt's tech job market.
#   Each keyword becomes a separate Wuzzuf search → results are combined later.
#   We kept only genuinely tech/data roles — removed broad terms like
#   "financial analyst" and "operations analyst" that pulled irrelevant jobs.
SEARCH_KEYWORDS = [
    # Core data roles
    "data engineer",
    "data analyst",
    "data scientist",
    "analytics engineer",

    # BI & Visualization
    "business intelligence",
    "power bi developer",
    "tableau developer",
    "bi developer",

    # SQL & Python
    "sql developer",
    "python developer",

    # ETL & Pipelines
    "etl developer",
    "data pipeline",

    # Big Data & Cloud
    "big data engineer",
    "data warehouse engineer",
    "cloud data engineer",
    "snowflake developer",

    # AI & ML
    "machine learning engineer",
    "ai engineer",
    "nlp engineer",

    # Business & Product
    "business analyst",
    "product analyst",
]

# ── Relevance Filter ──────────────────────────────────────────────────────────
# WHY THIS EXISTS:
#   Wuzzuf's search is not strict — searching "data analyst" returns HVAC
#   engineers, accountants, and production managers because Wuzzuf does
#   partial keyword matching across the full job description.
#   We check the job TITLE (not description) against this list.
#   If the title contains NONE of these keywords → job is irrelevant → drop it.
#
# HOW IT WORKS:
#   is_relevant_job("Senior Data Engineer") → True  (contains "data")
#   is_relevant_job("HVAC Design Engineer") → False (no tech keywords)
#   is_relevant_job("Laravel Developer")    → True  (contains "developer")
TECH_TITLE_KEYWORDS = [
    # Data
    "data", "analytics", "analytical", "analyst",
    # Engineering
    "engineer", "engineering",
    # Development
    "developer", "development",
    # AI/ML
    "machine learning", "artificial intelligence", "ai ", " ai",
    "nlp", "llm", "deep learning", "computer vision",
    # Specific tools
    "sql", "python", "power bi", "tableau", "snowflake",
    "spark", "kafka", "airflow", "dbt", "hadoop",
    # Roles
    "scientist", "architect", "devops", "cloud",
    "business intelligence", "bi ", " bi",
    "etl", "elt", "pipeline",
    # Software
    "software", "backend", "frontend", "fullstack", "full stack",
    "mobile", "android", "ios",
    # IT
    "it ", " it", "network", "security", "cybersecurity",
    "database", "dba", "erp", "oracle", "sap",
    # Product
    "product", "scrum", "agile",
    # QA
    "quality assurance", "qa ", " qa", "automation",
]

# ── Selector Constants ────────────────────────────────────────────────────────
# WHY THESE SELECTORS:
#   Wuzzuf's hashed CSS classes change on every deployment.
#   We use structural selectors (href patterns, element relationships)
#   that describe WHAT an element does, not WHAT class it has.
#   This makes the scraper survive Wuzzuf frontend updates.

KNOWN_WORK_MODES = {"on-site", "hybrid", "remote"}
KNOWN_JOB_TYPES = {
    "full time", "part time", "internship",
    "freelance / project", "freelance", "contract",
    "temporary", "volunteer",
}

CARD_SELECTORS = [
    "article[data-jobid]",
    "div[data-jobid]",
    "div[class*='JobCard']",
    "div[class*='job-card']",
    "div:has(> div > h2 a[href*='/jobs/p/'])",
]

FIELD_SELECTORS = {
    "title_link" : "h2 a[href*='/jobs/p/']",
    "company"    : ["a[href*='/jobs/c/']", "a[href*='/company/']"],
    "location"   : ["span[class*='location']", "span[class*='Location']"],
    "job_type"   : [
        "a[href*='filters%5Btype%5D']", "a[href*='filters[type]']",
        "a[href*='type%5D']", "span[class*='type']", "a[class*='type']",
    ],
    "experience" : [
        "span[class*='xp']", "span[class*='exp']", "span[class*='Exp']",
        "span[class*='experience']", "i.fas.fa-briefcase + span",
    ],
    "skills"     : [
        "a[href*='filters%5Bskill']", "a[href*='filters[skill]']",
        "a[href*='skills']", "a[class*='tag']", "a[class*='Tag']",
        "span[class*='tag'] a", "span[class*='Tag'] a",
    ],
    "posted_date": [
        "time", "span[class*='date']", "div[class*='date']",
        "span[class*='Date']", "span[class*='ago']",
        "div[class*='ago']", "span[class*='post']",
    ],
}


# ── Relevance Check ───────────────────────────────────────────────────────────
def is_relevant_job(title: str) -> bool:
    """
    Returns True if the job title contains at least one tech keyword.

    WHY WE CHECK TITLE NOT DESCRIPTION:
        Job descriptions are long and contain many unrelated words.
        "Data Analyst" in a job description doesn't mean the role IS data.
        The title is the most reliable signal of what the job actually is.

    EXAMPLES:
        is_relevant_job("Senior Data Engineer")     → True
        is_relevant_job("HVAC Design Engineer")     → False
        is_relevant_job("Traffic Engineer")         → False
        is_relevant_job("Machine Learning Intern")  → True
    """
    title_lower = title.lower()
    return any(kw in title_lower for kw in TECH_TITLE_KEYWORDS)


# ── Helper Functions ──────────────────────────────────────────────────────────
def _first(card, selectors):
    """Try selectors in order, return the first matching element."""
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        found = card.query_selector(sel)
        if found:
            return found
    return None


def _all(card, selectors):
    """Try selectors in order, return all matches for the first that hits."""
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        found = card.query_selector_all(sel)
        if found:
            return found
    return []


# ── Scraper Class ─────────────────────────────────────────────────────────────
class WuzzufScraper:
    """
    Scrapes job listings from Wuzzuf.net for a single search keyword.

    WHY A CLASS:
        Each keyword needs its own browser session, job list, and state.
        A class keeps all of this organized and makes cleanup reliable.

    HOW AIRFLOW USES IT:
        The class is called by scrape_all_keywords() below.
        Airflow never calls this class directly.
    """

    def __init__(self, keyword: str, max_pages: int, output_dir: Path):
        self.keyword    = keyword
        self.max_pages  = max_pages
        self.output_dir = output_dir
        self.jobs: list[dict] = []

        # Start Playwright browser
        # headless=True means no visible browser window — runs in background
        # This is essential for Airflow which runs in Docker with no display
        self.playwright = sync_playwright().start()
        self.browser    = self.playwright.chromium.launch(headless=True)
        self.context    = self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        self.page = self.context.new_page()

    def _get_page(self, page_num: int) -> bool:
        """Navigate to a search results page. Returns False if it fails."""
        url = f"{BASE_URL}?q={self.keyword}&a%5Bpage%5D={page_num}"
        try:
            self.page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
            return True
        except Exception as e:
            logger.error(f"Failed to load page {page_num}: {e}")
            return False

    def _find_cards(self) -> list:
        """
        Find all job cards on the current page.

        WHY MULTIPLE SELECTORS:
            Wuzzuf's HTML structure changes. We try the most specific
            selector first, fall back to less specific ones.
            The heuristic fallback (walking up from title links) ensures
            we always find something even when all selectors fail.
        """
        for sel in CARD_SELECTORS:
            cards = self.page.query_selector_all(sel)
            if cards:
                return cards

        # Heuristic fallback — walk up from title links
        title_links = self.page.query_selector_all("h2 a[href*='/jobs/p/']")
        if title_links:
            logger.warning("Using grandparent heuristic — update CARD_SELECTORS")
            seen, cards = set(), []
            for link in title_links:
                container = link
                for _ in range(3):
                    if container:
                        container = container.query_selector("xpath=..")
                if container and id(container) not in seen:
                    seen.add(id(container))
                    cards.append(container)
            return cards
        return []

    def _parse_job_card(self, card, scraped_at: str) -> dict | None:
        """
        Extract all fields from a single job card element.

        WHY TRY/EXCEPT AROUND EVERYTHING:
            If one card fails to parse, we don't want to lose all other cards.
            We log the warning and continue to the next card.
        """
        try:
            # Title + URL — if we can't get these, the card is useless
            title_tag = _first(card, FIELD_SELECTORS["title_link"])
            if not title_tag:
                return None

            title = title_tag.inner_text().strip()
            url   = title_tag.get_attribute("href") or ""
            if not url.startswith("http"):
                url = "https://wuzzuf.net" + url

            # ── RELEVANCE FILTER ─────────────────────────────────────────────
            # WHY HERE (not after scraping):
            #   Filtering at parse time means we never even store irrelevant
            #   jobs in memory. Faster and cleaner than filtering afterward.
            if not is_relevant_job(title):
                logger.debug(f"  Skipped irrelevant job: {title!r}")
                return None

            # Job ID — from URL slug, used for deduplication
            job_id = card.get_attribute("data-jobid") or ""
            if not job_id:
                m      = re.search(r"/jobs/p/([^/?#]+)", url)
                job_id = m.group(1) if m else url.split("/")[-1]

            # Company name
            company_tag = _first(card, FIELD_SELECTORS["company"])
            if company_tag:
                company = company_tag.inner_text().strip()
            else:
                company = "Unknown"
                for a in card.query_selector_all("a[href]"):
                    href = a.get_attribute("href") or ""
                    text = a.inner_text().strip()
                    if (
                        "/jobs/p/" not in href and text
                        and text.lower() not in KNOWN_JOB_TYPES
                        and text.lower() not in KNOWN_WORK_MODES
                    ):
                        company = text
                        break
            company = re.sub(r"\s*-\s*$", "", company).strip()

            # Location + work mode (Wuzzuf puts both in one span)
            location_tag = _first(card, FIELD_SELECTORS["location"])
            if location_tag:
                raw_loc = location_tag.inner_text().strip()
            else:
                spans = []
                for s in card.query_selector_all("span"):
                    text = s.inner_text().strip()
                    if text and len(text) < 60 and not any(
                        kw in text.lower() for kw in {"yrs", "year"}
                    ):
                        inside_h2 = s.evaluate("el => el.closest('h2') !== null")
                        if not inside_h2:
                            spans.append(text)
                raw_loc = ", ".join(spans[:2]) if spans else "Egypt"

            loc_parts      = [p.strip() for p in raw_loc.split(",")]
            extracted_type = "Unknown"
            while loc_parts and loc_parts[-1].lower() in KNOWN_WORK_MODES | KNOWN_JOB_TYPES:
                last = loc_parts.pop()
                if extracted_type == "Unknown":
                    extracted_type = last
            location = ", ".join(loc_parts) if loc_parts else raw_loc

            # Job type
            job_type_tag = _first(card, FIELD_SELECTORS["job_type"])
            job_type     = job_type_tag.inner_text().strip() if job_type_tag else extracted_type

            # Experience
            exp_tag    = _first(card, FIELD_SELECTORS["experience"])
            if exp_tag:
                experience = exp_tag.inner_text().strip()
            else:
                experience = "Not specified"
                for el in card.query_selector_all("span, p, div"):
                    text = el.inner_text().strip()
                    if re.search(r"\d+.*yr", text, re.IGNORECASE) and len(text) < 50:
                        experience = text
                        break

            # Skills
            skill_tags = _all(card, FIELD_SELECTORS["skills"])
            if skill_tags:
                skills = [s.inner_text().strip() for s in skill_tags if s.inner_text().strip()]
            else:
                skills = []
                for a in card.query_selector_all("a[href]"):
                    href = a.get_attribute("href") or ""
                    text = a.inner_text().strip()
                    if (
                        "skill" in href.lower() or "tag" in href.lower()
                        or (
                            "/jobs/p/" not in href and "/jobs/c/" not in href
                            and "/company/" not in href and text
                            and len(text) <= 25
                            and text.lower() not in KNOWN_JOB_TYPES
                            and text.lower() not in KNOWN_WORK_MODES
                            and not re.search(r"\d+\s*(yr|yrs|year)", text, re.I)
                        )
                    ):
                        skills.append(text)

            # Posted date
            date_tag    = _first(card, FIELD_SELECTORS["posted_date"])
            if date_tag:
                posted_date = date_tag.get_attribute("datetime") or date_tag.inner_text().strip()
            else:
                posted_date = "Unknown"
                for el in card.query_selector_all("span, div, time, p"):
                    text = el.inner_text().strip()
                    if re.search(r"\d+\s*(hour|day|week|month)s?\s*ago", text, re.I):
                        posted_date = text
                        break
                    if re.search(
                        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b",
                        text, re.I
                    ) and len(text) < 30:
                        posted_date = text
                        break

            return {
                "job_id"     : job_id,
                "title"      : title,
                "company"    : company,
                "location"   : location,
                "job_type"   : job_type,
                "experience" : experience,
                "skills"     : skills,
                "posted_date": posted_date,
                "url"        : url,
                "scraped_at" : scraped_at,
                "keyword"    : self.keyword,
            }

        except Exception as e:
            logger.warning(f"Failed to parse card: {e}")
            return None

    def _parse_page(self, scraped_at: str) -> list[dict]:
        """Parse all job cards on the current page."""
        cards = self._find_cards()
        logger.info(f"  Found {len(cards)} cards on page")
        return [j for card in cards if (j := self._parse_job_card(card, scraped_at))]

    def _is_empty_results_page(self) -> bool:
        """Detect if Wuzzuf is showing a 'no results' page."""
        has_jobs = bool(self.page.query_selector_all("h2 a[href*='/jobs/p/']"))
        if has_jobs:
            return False

        empty_selectors = [
            "div[class*='EmptyState']", "div[class*='empty-state']",
            "div[class*='no-results']", "div[class*='NoResults']",
        ]
        for sel in empty_selectors:
            if self.page.query_selector(sel):
                return True

        for heading in self.page.query_selector_all("h1, h2, h3"):
            text = heading.inner_text().strip().lower()
            if re.search(r"no (results|jobs|vacancies) found|0 jobs", text):
                return True

        logger.warning("No cards found but no empty-state detected — selectors may need updating")
        return False

    def run(self) -> list[dict]:
        """
        Main scraping loop — iterates through pages for this keyword.
        Returns list of relevant, deduplicated job dicts.
        """
        logger.info(f"Scraping: '{self.keyword}' (up to {self.max_pages} pages)")

        for page_num in range(1, self.max_pages + 1):
            logger.info(f"  Page {page_num}/{self.max_pages}")
            scraped_at = datetime.now(timezone.utc).isoformat()

            if not self._get_page(page_num):
                logger.warning(f"  Skipping page {page_num} — failed to load")
                continue

            if self._is_empty_results_page():
                logger.info("  No more results — stopping early")
                break

            page_jobs = self._parse_page(scraped_at)

            # Deduplicate within this keyword's results
            seen_ids = {j["job_id"] for j in self.jobs}
            new_jobs = [j for j in page_jobs if j["job_id"] not in seen_ids]
            dupes    = len(page_jobs) - len(new_jobs)

            if dupes:
                logger.warning(f"  Skipped {dupes} duplicate(s)")

            # If entire page was duplicates → Wuzzuf pagination exhausted
            if dupes == len(page_jobs) and page_jobs:
                logger.info("  Entire page was duplicates — pagination exhausted")
                break

            self.jobs.extend(new_jobs)
            logger.info(f"  Total so far: {len(self.jobs)}")

            if page_num < self.max_pages:
                time.sleep(DELAY_SECONDS)

        logger.info(f"Done: '{self.keyword}' → {len(self.jobs)} relevant jobs")
        return self.jobs
    

    def save(self) -> Path:
        """
        Save scraped jobs to a JSON file per keyword.
        Uses upsert logic — existing job_ids get updated, new ones get appended.
        WHY PER-KEYWORD FILES:
            Audit trail — if combined file corrupts you can rebuild from these.
            Also useful for debugging which keyword found which jobs.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        keyword_slug = re.sub(r"[^a-z0-9]+", "_", self.keyword.lower()).strip("_")
        filename     = f"wuzzuf_{keyword_slug}.json"
        output_path  = self.output_dir / filename

        # Load existing file if it exists (for upsert)
        existing_jobs: dict[str, dict] = {}
        if output_path.exists():
            try:
                with open(output_path, encoding="utf-8") as f:
                    old_data = json.load(f)
                for job in old_data.get("jobs", []):
                    existing_jobs[job["job_id"]] = job
            except (json.JSONDecodeError, KeyError):
                logger.warning("Existing file %s unreadable — starting fresh.", filename)

        # Upsert — update existing, append new
        updated = appended = 0
        for job in self.jobs:
            if job["job_id"] in existing_jobs:
                existing_jobs[job["job_id"]].update(job)
                updated += 1
            else:
                existing_jobs[job["job_id"]] = job
                appended += 1

        merged_jobs = list(existing_jobs.values())

        payload = {
            "metadata": {
                "keyword"       : self.keyword,
                "total_jobs"    : len(merged_jobs),
                "pages_scraped" : self.max_pages,
                "last_updated"  : datetime.now(timezone.utc).isoformat(),
                "updated_count" : updated,
                "appended_count": appended,
                "source"        : "wuzzuf.net",
            },
            "jobs": merged_jobs,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(
            "Saved %s → updated=%d appended=%d total=%d",
            filename, updated, appended, len(merged_jobs),
        )
        return output_path

    def close(self):
        """Always call this after scraping to release browser resources."""
        self.context.close()
        self.browser.close()
        self.playwright.stop()


# ── Multi-Keyword Orchestration ───────────────────────────────────────────────
def scrape_all_keywords(
    output_dir: Path = Path("data/raw"),
    max_pages: int   = 5,
    keywords: list   = None,
) -> list[dict]:
    """
    Scrape Wuzzuf for ALL keywords in SEARCH_KEYWORDS.
    This is the main function called by Airflow.

    WHY ONE FUNCTION FOR ALL KEYWORDS:
        Each keyword is a separate Wuzzuf search. The same job can appear
        in multiple searches (a "Data Engineer" job appears in both
        "data engineer" and "python developer" searches).
        This function deduplicates across ALL keywords by job_id.

    WHY WE OPEN/CLOSE BROWSER PER KEYWORD:
        Keeping one browser open for 20+ keywords × 5 pages = 100 page loads
        risks memory leaks and detection. Fresh browser per keyword is safer.

    RETURNS:
        List of all unique relevant jobs across all keywords.
        Also saves one JSON file per keyword + one combined file.

    AIRFLOW USAGE:
        from scraper.wuzzuf_scraper import scrape_all_keywords
        all_jobs = scrape_all_keywords(output_dir=Path("/opt/airflow/data/raw"))
    """
    keywords    = keywords or SEARCH_KEYWORDS
    output_dir  = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_jobs: dict[str, dict] = {}  # job_id → job dict (deduplicates across keywords)
    scrape_date = datetime.now(timezone.utc).isoformat()

    for i, keyword in enumerate(keywords, 1):
        logger.info(f"\n{'='*55}")
        logger.info(f"Keyword {i}/{len(keywords)}: '{keyword}'")
        logger.info(f"{'='*55}")

        scraper = WuzzufScraper(
            keyword    = keyword,
            max_pages  = max_pages,
            output_dir = output_dir,
        )

        try:
            jobs = scraper.run()

            # Add to global deduplicated dict FIRST
            # WHY FIRST: if save() fails, jobs are already in memory
            new_count = 0
            for job in jobs:
                if job["job_id"] not in all_jobs:
                    new_count += 1
                all_jobs[job["job_id"]] = job

            logger.info(f"'{keyword}': {len(jobs)} jobs ({new_count} new unique)")

            # Save per-keyword JSON file AFTER (audit trail)
            scraper.save()


        except Exception as e:
            logger.error(f"Keyword '{keyword}' failed: {e}")
            # WHY CONTINUE NOT RAISE:
            #   One failed keyword shouldn't kill the entire pipeline.
            #   Log the error and move to the next keyword.
            continue

        finally:
            # WHY FINALLY:
            #   close() MUST run even if scraping fails.
            #   Otherwise browser processes pile up and eat memory.
            scraper.close()

        # Polite delay between keywords
        if i < len(keywords):
            time.sleep(3)

    # Save combined file
    # WHY A COMBINED FILE:
    #   The pipeline (clean.py, validate.py) reads ONE file, not 20.
    #   This combined file is the single source of truth for downstream steps.
    all_jobs_list = list(all_jobs.values())
    combined_path = output_dir / "wuzzuf_combined.json"

    combined_payload = {
        "metadata": {
            "total_jobs"    : len(all_jobs_list),
            "keywords_count": len(keywords),
            "scrape_date"   : scrape_date,
            "last_updated"  : datetime.now(timezone.utc).isoformat(),
            "source"        : "wuzzuf.net",
        },
        "jobs": all_jobs_list,
    }

    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(combined_payload, f, ensure_ascii=False, indent=2)

    logger.info(f"\n{'='*55}")
    logger.info(f"SCRAPING COMPLETE")
    logger.info(f"Total unique relevant jobs: {len(all_jobs_list)}")
    logger.info(f"Combined file: {combined_path}")
    logger.info(f"{'='*55}")

    return all_jobs_list


# ── CLI Entry Point ───────────────────────────────────────────────────────────
# WHY KEEP CLI:
#   Lets you run the scraper manually for testing without Airflow.
#   python scraper/wuzzuf_scraper.py --max-pages 3
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape jobs from Wuzzuf.net")
    parser.add_argument("--max-pages", "-p", type=int, default=5,
                        help="Max pages per keyword (default: 5)")
    parser.add_argument("--output-dir", "-o", type=Path, default=Path("data/raw"),
                        help="Output directory (default: data/raw)")
    parser.add_argument("--keyword", "-k", type=str, default=None,
                        help="Single keyword to test (default: all keywords)")
    args = parser.parse_args()

    keywords = [args.keyword] if args.keyword else None

    jobs = scrape_all_keywords(
        output_dir = args.output_dir,
        max_pages  = args.max_pages,
        keywords   = keywords,
    )

    print(f"\n✔ Done — {len(jobs)} unique relevant jobs scraped")