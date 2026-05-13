
{{
    config(
        materialized = 'incremental',
        unique_key   = 'job_id'
    )
}}

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_jobs') }}
    {% if is_incremental() %}
        WHERE scrape_date > (
            SELECT MAX(scrape_date)
            FROM {{ this }}

        )
    {% endif %}

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

        CASE
            WHEN min_experience IS NOT NULL
             AND max_experience IS NOT NULL
            THEN max_experience - min_experience
            ELSE NULL
        END                                                     AS experience_gap,

        TRIM(company_name)                                      AS company_name,
        TRIM(company_type)                                      AS company_type,

        TRIM(city)                                              AS city,
        'Egypt'                                                 AS country,

        posted_date,
        scrape_date,

        DATEDIFF('day', posted_date, scrape_date)               AS days_on_market

    FROM source
    WHERE job_url IS NOT NULL   

)

SELECT * FROM cleaned