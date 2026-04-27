SELECT

    {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,

    job_title,
    job_category,
    experience_level,
    scrape_date

FROM {{ ref('stg_jobs') }}