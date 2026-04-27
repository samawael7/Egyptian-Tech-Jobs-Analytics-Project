-- This is your staging model — the first transformation layer
-- It sits directly on top of raw_jobs and does light cleaning only
-- Think of it as "making the data usable" before building dimensions and facts

-- {{ source('raw', 'raw_jobs') }} references the table we registered in sources.yml
-- This is how dbt knows the lineage: stg_jobs depends on raw_jobs

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_jobs') }}
),

cleaned AS (
    SELECT
        -- Generate a unique ID for each job using the URL
        -- dbt_utils.generate_surrogate_key hashes the value into a unique string
        {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,

        -- Job info — TRIM removes accidental spaces
        TRIM(job_title)                                         AS job_title,
        TRIM(LOWER(job_category))                               AS job_category,
        TRIM(LOWER(experience_level))                           AS experience_level,
        TRIM(job_type)                                          AS job_type,
        TRIM(work_type)                                         AS work_type,
        job_url,
        skills_list,

        -- Experience — kept as-is, nulls are valid
        min_experience,
        max_experience,

        -- Company info
        TRIM(company_name)                                      AS company_name,
        TRIM(company_type)                                      AS company_type,

        -- Location — we add country manually since all jobs are from Egypt
        TRIM(city)                                              AS city,
        'Egypt'                                                 AS country,

        -- Dates
        posted_date,
        scrape_date

    FROM source
    WHERE job_url IS NOT NULL  -- safety filter, removes any row with no URL
)

SELECT * FROM cleaned