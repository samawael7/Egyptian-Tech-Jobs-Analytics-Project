import os
import pandas as pd
from scipy import stats
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

# Query 1: jobs with skill counts
query1 = """
SELECT 
    j.job_id,
    j.experience_level,
    j.job_category,
    COUNT(DISTINCT b.skill_id) AS skill_count
FROM dim_job j
JOIN bridge_job_skills b ON j.job_id = b.job_id
GROUP BY j.job_id, j.experience_level, j.job_category
"""
df = pd.read_sql(query1, conn)

# Query 2: skill-category pairs
query2 = """
SELECT 
    s.skill_name,
    j.job_category
FROM bridge_job_skills b
JOIN dim_skills s ON b.skill_id = s.skill_id
JOIN dim_job j ON b.job_id = j.job_id
"""
df_skills = pd.read_sql(query2, conn)

conn.close()

print(df.columns.tolist())
print(df_skills.columns.tolist())

print("\n=== ANOVA: skill count by experience level ===")
groups = [group['SKILL_COUNT'].values for name, group in df.groupby('EXPERIENCE_LEVEL')]
f_stat, p_value = stats.f_oneway(*groups)
print(f"F-statistic: {f_stat:.3f}, p-value: {p_value:.4f}")
print(df.groupby('EXPERIENCE_LEVEL')['SKILL_COUNT'].agg(['mean', 'count']))

print("\n=== Correlation: posting count vs avg skill count per category ===")
category_summary = df.groupby('JOB_CATEGORY').agg(
    posting_count=('JOB_ID', 'count'),
    avg_skill_count=('SKILL_COUNT', 'mean')
).reset_index()
correlation = category_summary['posting_count'].corr(category_summary['avg_skill_count'])
print(f"Correlation: {correlation:.3f}")
print(category_summary)

print("\n=== Chi-square: top 20 skills vs category ===")
top_skills = df_skills['SKILL_NAME'].value_counts().head(20).index
df_top = df_skills[df_skills['SKILL_NAME'].isin(top_skills)]
contingency_table = pd.crosstab(df_top['SKILL_NAME'], df_top['JOB_CATEGORY'])
chi2, p_value, dof, expected = stats.chi2_contingency(contingency_table)
print(f"Chi-square: {chi2:.3f}, p-value: {p_value:.4f}, degrees of freedom: {dof}")