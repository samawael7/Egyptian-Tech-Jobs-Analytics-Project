WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_jobs') }}
),

cleaned AS (
    SELECT

        {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,

        TRIM(job_title)                                         AS job_title,
        TRIM(LOWER(job_category))                               AS job_category,
        TRIM(LOWER(experience_level))                           AS experience_level,
        TRIM(job_type)                                          AS job_type,
        TRIM(work_type)                                         AS work_type,
        job_url,
        skills_list,

        min_experience,
        max_experience,

        TRIM(company_name)                                      AS company_name,
        TRIM(company_type)                                      AS company_type,

        TRIM(city)                                              AS city,
        'Egypt'                                                 AS country,

        -- Dates
        posted_date,
        scrape_date

    FROM source
    WHERE job_url IS NOT NULL  
)

SELECT * FROM cleaned