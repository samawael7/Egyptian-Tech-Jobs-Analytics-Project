

WITH companies AS (

    SELECT DISTINCT
        company_name,
        company_type
    FROM {{ ref('stg_jobs') }}
    WHERE company_name IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['company_name']) }}    AS company_id,
    company_name,
    company_type
FROM companies