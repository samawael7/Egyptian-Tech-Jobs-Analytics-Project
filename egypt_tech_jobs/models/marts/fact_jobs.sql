-- fact_jobs — one row per job posting
-- This is the center of your star schema
-- It contains foreign keys to all dimensions + measurable facts
-- Power BI joins everything through this table
-- Facts (measurable numbers): min_experience, max_experience
-- Keys (links to dimensions): company_id, location_id, date_id, job_id

WITH stg AS (
    SELECT * FROM {{ ref('stg_jobs') }}
)

SELECT
    -- Primary key for this fact row
    {{ dbt_utils.generate_surrogate_key(['stg.job_url']) }}     AS fact_id,

    -- Foreign keys — linking to each dimension table
    -- LEFT JOIN means: even if no match found, keep the job row
    dj.job_id,          -- links to dim_job
    co.company_id,      -- links to dim_company
    lo.location_id,     -- links to dim_location
    da.date_id,         -- links to dim_date

    -- Measurable facts — numerical values you can aggregate
    stg.min_experience,
    stg.max_experience,

    -- Degenerate dimensions — descriptive attributes with no dimension table
    -- Not worth making a full dimension for these
    stg.job_type,       -- Full Time / Part Time / Freelance
    stg.work_type,      -- On-site / Hybrid / Remote
    stg.job_url         -- kept for traceability back to source

FROM stg

-- Join each dimension to get its surrogate key
LEFT JOIN {{ ref('dim_job') }}      dj ON {{ dbt_utils.generate_surrogate_key(['stg.job_url']) }} = dj.job_id
LEFT JOIN {{ ref('dim_company') }}  co ON stg.company_name  = co.company_name
LEFT JOIN {{ ref('dim_location') }} lo ON stg.city          = lo.city
LEFT JOIN {{ ref('dim_date') }}     da ON stg.posted_date   = da.full_date