-- bridge_job_skills — one row per job-skill combination
-- This is the many-to-many bridge table between dim_job and dim_skills
-- Example: if a job has 5 skills → 5 rows in this table, all with same job_id
-- This is what powers:
--   → Which skills appear most in job postings (COUNT by skill_id)
--   → Which skills co-occur together (self-join on job_id)
--   → Which skills are needed per seniority level (join to dim_job)

WITH raw_skills AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,
        skills_list
    FROM {{ ref('stg_jobs') }}
    WHERE skills_list IS NOT NULL
        AND skills_list != '[]'
),

-- Same exploding logic as dim_skills
exploded AS (
    SELECT
        job_id,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(value::STRING, '[', ''),
                ']', ''),
            '''', '')
        ) AS skill_name
    FROM raw_skills,
    LATERAL FLATTEN(
        input => SPLIT(
            REGEXP_REPLACE(skills_list, '\\[|\\]', ''), ','
        )
    )
),

cleaned AS (
    SELECT DISTINCT
        job_id,
        TRIM(LOWER(skill_name))     AS skill_name
    FROM exploded
    WHERE skill_name IS NOT NULL
        AND TRIM(skill_name) != ''
        AND LENGTH(TRIM(skill_name)) > 1
)

-- Join to dim_skills to get the skill_id
-- This replaces the raw skill name with the proper foreign key
SELECT
    c.job_id,
    s.skill_id
FROM cleaned c
JOIN {{ ref('dim_skills') }} s
    ON c.skill_name = s.skill_name