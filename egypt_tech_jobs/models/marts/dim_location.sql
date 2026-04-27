WITH locations AS (
    SELECT DISTINCT
        city,
        country
    FROM {{ ref('stg_jobs') }}
    WHERE city IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['city', 'country']) }}    AS location_id,
    city,
    country
FROM locations