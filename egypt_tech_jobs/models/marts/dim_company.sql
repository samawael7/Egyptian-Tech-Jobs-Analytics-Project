-- dim_company — one row per unique company
-- This is a dimension table — it describes the "who" of each job posting
-- Power BI will use this to filter/group by company name and type

WITH companies AS (
    -- Pull distinct companies from staging
    -- {{ ref('stg_jobs') }} is how dbt references another model
    -- dbt automatically knows to run stg_jobs first before this model
    SELECT DISTINCT
        company_name,
        company_type
    FROM {{ ref('stg_jobs') }}
    WHERE company_name IS NOT NULL
)

SELECT
    -- Generate a unique ID for each company
    {{ dbt_utils.generate_surrogate_key(['company_name']) }}    AS company_id,
    company_name,
    company_type
FROM companies