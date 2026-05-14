import subprocess
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path("/opt/airflow/project")
sys.path.insert(0, str(PROJECT_ROOT))   

default_args = {
    "owner"           : "sama",
    "depends_on_past" : False,      
    "start_date"      : datetime(2026, 5, 1),
    "email_on_failure": False,      
    "email_on_retry"  : False,
    "retries"         : 2,         
    "retry_delay"     : timedelta(minutes=5),  
}

with DAG(
    dag_id="egypt_jobs_pipeline",
    default_args=default_args,
    description="Daily Egypt tech job market scraping and analytics pipeline",
    schedule_interval="0 7 * * *",

    catchup=False,
    tags=["egypt_jobs", "scraping", "snowflake", "dbt"],
) as dag:

    def scrape_task(**context):
        from scraper.wuzzuf_scraper import scrape_all_keywords

        output_dir = PROJECT_ROOT / "data" / "raw"
        jobs = scrape_all_keywords(
            output_dir = output_dir,
            max_pages  = 5,
        )

        if not jobs:
            raise ValueError("Scraper returned 0 jobs — something is wrong")
        context["ti"].xcom_push(key="jobs_scraped", value=len(jobs))

        print(f"✔ Scraped {len(jobs)} relevant jobs")
        return len(jobs)

    task_scrape = PythonOperator(
        task_id         = "scrape_wuzzuf",
        python_callable = scrape_task,
        provide_context = True,
    )

    def validate_task(**context):
        from pipeline.clean import run_cleaning_pipeline
        from pipeline.validate import run_validation

        input_path  = PROJECT_ROOT / "data" / "raw" / "wuzzuf_combined.json"
        output_path = PROJECT_ROOT / "data" / "processed" / "jobs_cleaned.parquet"

        # Clean first
        df = run_cleaning_pipeline(
            input_path  = input_path,
            output_path = output_path,
        )

        # Then validate
        run_validation(parquet_path=output_path)

        context["ti"].xcom_push(key="jobs_cleaned", value=len(df))
        print(f"✔ Validation passed — {len(df)} clean jobs")
        return len(df)

    task_validate = PythonOperator(
        task_id         = "validate_and_clean",
        python_callable = validate_task,
        provide_context = True,
    )

    #Load to Snowflake
    def load_task(**context):
        """
        MERGEs cleaned Parquet into Snowflake raw_jobs table.
        Only inserts new jobs, updates existing ones.
        """
        from pipeline.load import load_to_snowflake

        parquet_path = PROJECT_ROOT / "data" / "processed" / "jobs_cleaned.parquet"
        result       = load_to_snowflake(parquet_path=parquet_path)

        context["ti"].xcom_push(key="rows_affected",  value=result["rows_affected"])
        context["ti"].xcom_push(key="total_in_table", value=result["total_in_table"])

        print(f"✔ Snowflake load complete")
        print(f"  Rows affected : {result['rows_affected']}")
        print(f"  Total in table: {result['total_in_table']}")
        return result

    task_load = PythonOperator(
        task_id         = "load_to_snowflake",
        python_callable = load_task,
        provide_context = True,
    )

    #Run dbt
    def dbt_task(**context):
        dbt_project_dir = PROJECT_ROOT / "egypt_tech_jobs"

        print("Running dbt models...")
        result = subprocess.run(
            ["dbt", "run", "--project-dir", str(dbt_project_dir)],
            capture_output = True,
            text           = True,
            env            = {
                **os.environ,
                "DBT_PROFILES_DIR": "/opt/airflow/dbt_profiles",
            }
        )

        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt run failed:\n{result.stderr}")

        # Run dbt tests
        print("Running dbt tests...")
        test_result = subprocess.run(
            ["dbt", "test", "--project-dir", str(dbt_project_dir)],
            capture_output = True,
            text           = True,
            env            = {
                **os.environ,
                "DBT_PROFILES_DIR": "/opt/airflow/dbt_profiles",
            }
        )

        print(test_result.stdout)
        if test_result.returncode != 0:
            print(test_result.stderr)
            raise RuntimeError(f"dbt test failed:\n{test_result.stderr}")

        print("✔ dbt run and test completed successfully")

    task_dbt = PythonOperator(
        task_id         = "run_dbt",
        python_callable = dbt_task,
        provide_context = True,
    )


    task_scrape >> task_validate >> task_load >> task_dbt


    