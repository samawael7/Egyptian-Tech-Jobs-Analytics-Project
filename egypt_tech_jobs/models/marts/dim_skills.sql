-- dim_skills — one row per unique skill
-- Skills in your CSV are stored as a Python list string: ['Python', 'SQL', 'Spark']
-- We need to explode that into individual rows
-- FLATTEN is Snowflake's function that explodes arrays into rows
-- SPLIT converts the comma-separated string into an array first
-- Then LATERAL FLATTEN iterates over each element

WITH raw_skills AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,
        skills_list
    FROM {{ ref('stg_jobs') }}
    WHERE skills_list IS NOT NULL
        AND skills_list != '[]'
),

-- Explode the list string into individual skill rows
exploded AS (
    SELECT
        job_id,
        TRIM(
            -- Remove the brackets, quotes, and apostrophes from each skill
            REPLACE(
                REPLACE(
                    REPLACE(value::STRING, '[', ''),
                ']', ''),
            '''', '')
        ) AS skill_name
    FROM raw_skills,
    -- LATERAL FLATTEN is the key — it explodes the array into rows
    LATERAL FLATTEN(
        input => SPLIT(
            REGEXP_REPLACE(skills_list, '\\[|\\]', ''), ','
        )
    )
),

-- Get only unique skills across all jobs
unique_skills AS (
    SELECT DISTINCT
        TRIM(LOWER(skill_name))     AS skill_name
    FROM exploded
    WHERE skill_name IS NOT NULL
        AND TRIM(skill_name) != ''
        AND LENGTH(TRIM(skill_name)) > 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['skill_name']) }}  AS skill_id,
    skill_name
FROM unique_skills