# 🇪🇬 Egypt Tech Job Market Intelligence Platform

An end-to-end data engineering project that scrapes, processes, and visualizes Egypt's tech job market using a modern data stack.

---

## 📌 Project Overview

This platform answers key questions about Egypt's data job market:
- Which technical skills dominate Egypt's tech job market?
- Do startups or MNCs hire more data professionals?
- Is remote work growing or shrinking in Egyptian tech roles?
- What skills separate a junior from a senior data engineer in Egypt?

---

## 🏗️ Architecture

Wuzzuf.net → Playwright Scraper (JSON, 66 jobs) ─┐
                                                   ├→ Combined Raw Data → Python/Pandas Cleaning → Snowflake (Raw) → dbt (Star Schema) → Power BI
Wuzzuf.net → Selenium Scraper (CSV, 524 jobs)  ──┘
---

## 🛠️ Tech Stack

| Layer | Tools |
|---|---|
| Scraping | Python, Selenium (CSV — 524 jobs), Playwright (JSON — 66 jobs) |
| Cleaning & Enrichment | Python, Pandas, Regex |
| Cloud Warehouse | Snowflake |
| Transformation | dbt |
| Visualization | Power BI, DAX |
| Version Control | Git, GitHub |

---

## 📁 Project Structure
Egyptian-Tech-Jobs-Analytics-Project/
│
├── data/
│   └── cleaned/
│       └── jobs_combined.csv           # Final cleaned dataset
│
├── notebooks/
│   └── 01_cleaning.ipynb               # Full cleaning pipeline documented
│
├── egypt_tech_jobs/                    # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_jobs.sql
│   │   └── marts/
│   │       ├── dim_company.sql
│   │       ├── dim_date.sql
│   │       ├── dim_job.sql
│   │       ├── dim_location.sql
│   │       ├── dim_skills.sql
│   │       ├── bridge_job_skills.sql
│   │       └── fact_jobs.sql
│   ├── dbt_project.yml
│   └── packages.yml
│
└── README.md
---

## ⭐ Star Schema
               ┌─────────────────┐
               │   fact_jobs     │
               │─────────────────│
         ┌────►│ job_id (FK)     │◄────┐
         │     │ company_id (FK) │     │
         │     │ location_id (FK)│     │
         │     │ date_id (FK)    │     │
         │     │ min_experience  │     │
         │     │ max_experience  │     │
         │     │ job_type        │     │
         │     │ work_type       │     │
         │     │ job_url         │     │
         └─────┴─────────────────┘─────┘
         │                             │
┌──────────┴──────┐           ┌──────────┴──────┐
│  dim_company    │           │  dim_location   │
│─────────────────│           │─────────────────│
│ company_id (PK) │           │ location_id (PK)│
│ company_name    │           │ city            │
│ company_type    │           │ country         │
└─────────────────┘           └─────────────────┘
┌─────────────────┐           ┌─────────────────┐
│   dim_job       │           │   dim_date      │
│─────────────────│           │─────────────────│
│ job_id (PK)     │           │ date_id (PK)    │
│ job_title       │           │ full_date       │
│ job_category    │           │ day/month/year  │
│ experience_level│           │ quarter         │
│ scrape_date     │           │ week_of_year    │
└─────────────────┘           └─────────────────┘
┌─────────────────┐   ┌─────────────────────┐
│   dim_skills    │   │  bridge_job_skills  │
│─────────────────│   │─────────────────────│
│ skill_id (PK)   │◄──│ job_id (FK)         │
│ skill_name      │   │ skill_id (FK)        │
└─────────────────┘   └─────────────────────┘

---

## 📊 Dataset Stats

| Metric | Value |
|---|---|
| Total Job Postings | 590 |
| Unique Companies | 360 |
| Unique Skills | 1,510 |
| Job-Skill Combinations | 4,141 |
| Unique Cities | 11 |
| Date Range | Feb 2026 – Apr 2026 |

---

## 🧹 Cleaning & Enrichment

Performed in `notebooks/01_cleaning.ipynb`:

- ✅ Fixed `experience_level` misclassification (Egyptian "Middle" titles)
- ✅ Converted relative `posted_date` → absolute dates
- ✅ Extracted `min_experience` and `max_experience` from raw text
- ✅ Cleaned `skills_list` — removed sentences, noise, empty lists
- ✅ Normalized `job_category` from 180+ values → 20 clean categories
- ✅ Added `company_type` (MNC / Corporate / Startup / Government / Unknown)
- ✅ Added `scrape_date` column
- ✅ Dropped redundant columns (`experience_raw`, `data_source`)

---

## 🚀 How to Run (dbt)

### Prerequisites
- Snowflake account
- Python 3.8+
- dbt-snowflake installed

### Setup

```bash
# 1. Install dbt
pip install dbt-snowflake

# 2. Clone the repo
git clone https://github.com/samawael7/Egyptian-Tech-Jobs-Analytics-Project.git
cd Egyptian-Tech-Jobs-Analytics-Project/egypt_tech_jobs

# 3. Create your profiles.yml at C:\Users\<username>\.dbt\profiles.yml
egypt_tech_jobs:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: jsgtohn-dcb76328
      user: <your_snowflake_username>
      password: <your_snowflake_password>
      role: ACCOUNTADMIN
      warehouse: dev_wh
      database: egypt_jobs_db
      schema: staging
      threads: 4

# 4. Install packages
dbt deps

# 5. Test connection
dbt debug

# 6. Run all models
dbt run
```

---

## 📈 Power BI Dashboard — 5 Pages

| Page | Focus |
|---|---|
| Market Overview | KPIs, job distribution, company types, cities |
| Skills Intelligence | Top skills, skills by category, skills by seniority |
| Hiring Patterns | Company type analysis, top hiring companies |
| Remote Work Trends | Work type over time, remote by role and company |
| Career Progression | Seniority funnel, skill roadmap by level |

---

## 👩‍💻 Author

**Sama Wael Abdou**
[![LinkedIn](https://img.shields.io/badge/LinkedIn-sama--waell-blue)](https://linkedin.com/in/sama-waell)
[![GitHub](https://img.shields.io/badge/GitHub-samawael7-black)](https://github.com/samawael7)
