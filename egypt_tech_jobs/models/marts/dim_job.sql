-- dim_job — one row per job posting
-- Describes the "what" of each job — title, category, seniority level
-- This dimension is what powers your seniority-to-skill progression analysis
-- and skill distribution by job category in Power BI

SELECT
    -- Same surrogate key logic as staging — generated from job_url
    -- This is the key that links dim_job to fact_jobs
    {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,

    job_title,
    job_category,
    experience_level,
    scrape_date

FROM {{ ref('stg_jobs') }}