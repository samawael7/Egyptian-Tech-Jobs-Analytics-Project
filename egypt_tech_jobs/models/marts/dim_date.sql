-- dim_date — one row per unique date
-- This is one of the most important dimensions in any data warehouse
-- It breaks dates into parts (day, month, quarter, year) so Power BI
-- can easily build time-series analysis like hiring velocity over time

WITH dates AS (
    SELECT DISTINCT posted_date AS full_date
    FROM {{ ref('stg_jobs') }}
    WHERE posted_date IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['full_date']) }}    AS date_id,
    full_date,

    -- Break the date into parts for easy filtering in Power BI
    DAY(full_date)                                           AS day,
    MONTH(full_date)                                         AS month,
    YEAR(full_date)                                          AS year,
    QUARTER(full_date)                                       AS quarter,
    WEEKOFYEAR(full_date)                                    AS week_of_year,
    DAYOFWEEK(full_date)                                     AS day_of_week,

    -- Human readable names for Power BI labels
    MONTHNAME(full_date)                                     AS month_name,
    DAYNAME(full_date)                                       AS day_name
FROM dates