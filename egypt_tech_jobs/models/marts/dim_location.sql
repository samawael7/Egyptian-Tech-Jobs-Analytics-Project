-- dim_location — one row per unique city
-- Describes the "where" of each job posting
-- Power BI will use this to build location-based analysis
-- e.g. which cities have the most data jobs in Egypt

WITH locations AS (
    SELECT DISTINCT
        city,
        country
    FROM {{ ref('stg_jobs') }}
    WHERE city IS NOT NULL
)

SELECT
    -- Surrogate key generated from city + country combination
    -- We use both in case we expand to other countries later
    {{ dbt_utils.generate_surrogate_key(['city', 'country']) }}    AS location_id,
    city,
    country
FROM locations