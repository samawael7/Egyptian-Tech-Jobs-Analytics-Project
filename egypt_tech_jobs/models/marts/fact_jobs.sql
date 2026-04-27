WITH stg AS (
    SELECT * FROM {{ ref('stg_jobs') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg.job_url']) }}     AS fact_id,

    dj.job_id,          
    co.company_id,      
    lo.location_id,     
    da.date_id,         

    stg.min_experience,
    stg.max_experience,

    stg.job_type,       
    stg.work_type,     
    stg.job_url      

FROM stg

LEFT JOIN {{ ref('dim_job') }}      dj ON {{ dbt_utils.generate_surrogate_key(['stg.job_url']) }} = dj.job_id
LEFT JOIN {{ ref('dim_company') }}  co ON stg.company_name  = co.company_name
LEFT JOIN {{ ref('dim_location') }} lo ON stg.city          = lo.city
LEFT JOIN {{ ref('dim_date') }}     da ON stg.posted_date   = da.full_date