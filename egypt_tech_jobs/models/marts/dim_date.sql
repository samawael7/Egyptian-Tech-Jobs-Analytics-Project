

WITH dates AS (
    SELECT DISTINCT posted_date AS full_date
    FROM {{ ref('stg_jobs') }}
    WHERE posted_date IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['full_date']) }}    AS date_id,
    full_date,

    DAY(full_date)                                           AS day,
    MONTH(full_date)                                         AS month,
    YEAR(full_date)                                          AS year,
    QUARTER(full_date)                                       AS quarter,
    WEEKOFYEAR(full_date)                                    AS week_of_year,
    DAYOFWEEK(full_date)                                     AS day_of_week,

    MONTHNAME(full_date)                                     AS month_name,
    DAYNAME(full_date)                                       AS day_name
FROM dates