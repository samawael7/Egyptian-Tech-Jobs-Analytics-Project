WITH raw_skills AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['job_url']) }}     AS job_id,
        skills_list
    FROM {{ ref('stg_jobs') }}
    WHERE skills_list IS NOT NULL
        AND skills_list != '[]'
),

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


SELECT
    c.job_id,
    s.skill_id
FROM cleaned c
JOIN {{ ref('dim_skills') }} s
    ON c.skill_name = s.skill_name