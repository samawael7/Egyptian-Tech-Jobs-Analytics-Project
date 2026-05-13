{{
    config(
        materialized = 'table'
    )
}}

WITH total_jobs AS (

    SELECT COUNT(*) AS total
    FROM {{ ref('fact_jobs') }}


),


skill_metrics AS (

    SELECT
        s.skill_name,
        COUNT(DISTINCT b.job_id)                                AS jobs_requiring_skill,

        ROUND(
            COUNT(DISTINCT b.job_id) * 100.0 / t.total
        , 2)                                                    AS skill_demand_score,

        ROUND(
            100 - (COUNT(DISTINCT b.job_id) * 100.0 / t.total)
        , 2)                                                    AS skill_rarity_score

    FROM {{ ref('bridge_job_skills') }}     b


    JOIN {{ ref('dim_skills') }}            s
        ON b.skill_id = s.skill_id

    CROSS JOIN total_jobs                   t

    GROUP BY s.skill_name, t.total

),


category_metrics AS (

    SELECT
        j.job_category,
        j.experience_level,

        COUNT(f.job_id)                                         AS job_count,


        ROUND(AVG(f.min_experience), 1)                         AS avg_min_experience,


        ROUND(
            AVG(CASE WHEN f.work_type = 'Remote' THEN 1.0 ELSE 0.0 END) * 100
        , 2)                                                    AS remote_ratio,


        ROUND(
            COUNT(f.job_id) * 100.0 / t.total
        , 2)                                                    AS market_demand_index

    FROM {{ ref('fact_jobs') }}             f
    JOIN {{ ref('dim_job') }}               j
        ON f.job_id = j.job_id
    CROSS JOIN total_jobs                   t

    GROUP BY j.job_category, j.experience_level, t.total

)


SELECT
    sm.skill_name,
    sm.jobs_requiring_skill,
    sm.skill_demand_score,
    sm.skill_rarity_score,


    CASE
        WHEN sm.skill_demand_score >= 30 THEN 'High Demand'
        WHEN sm.skill_demand_score >= 10 THEN 'Medium Demand'
        ELSE 'Niche'
    END                                                         AS demand_category

FROM skill_metrics sm
ORDER BY sm.skill_demand_score DESC
