import sys
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

# --------------------
# DEFAULT ARGUMENTS
# --------------------
default_args = {
    "owner": "health_finance_claims",
    "retries": 1,
}

# --------------------
# DAG DEFINITION
# --------------------
with DAG(
    dag_id="health_finance_claims_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 15),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["insurance", "glue", "dbt"],
) as dag:

    # --------------------
    # START
    # --------------------
    start = EmptyOperator(task_id="start")

    # --------------------
    # GENERATE DUMMY DATA
    # --------------------
    def generate_data():
        os.system(
        "python /mnt/c/Users/DEEPTHI/health_finance_claims/Scripts/generate_dummy_data.py"
        )

    generate_dummy_data = PythonOperator(
        task_id="generate_dummy_data",
        python_callable=generate_data,
    )

    # --------------------
    # UPLOAD TO S3
    # --------------------
    def upload_data():
        os.system(
        "python /mnt/c/Users/DEEPTHI/health_finance_claims/Scripts/aws_s3_upload.py"
        )

    aws_s3_upload = PythonOperator(
        task_id="aws_s3_upload",
        python_callable=upload_data,
    )

    # --------------------
    # GLUE JOB: CSV → PARQUET
    # --------------------
    glue_csv_to_parquet = GlueJobOperator(
        task_id="glue_csv_to_parquet",
        job_name="INSURANCE_CLAIMS_CLEAN",
        region_name="ap-southeast-2",
        wait_for_completion=True,
    )

    # --------------------
    # GLUE JOB: PARQUET → SNOWFLAKE
    # --------------------
    glue_parquet_to_snowflake = GlueJobOperator(
        task_id="glue_parquet_to_snowflake",
        job_name="glue_sf_load",
        region_name="ap-southeast-2",
        wait_for_completion=True,
    )

    # --------------------
    # DBT BUILD
    # --------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd ~/health_finance_claims &&
        dbt build
        """,
    )

    # --------------------
    # END
    # --------------------
    end = EmptyOperator(task_id="end")

    # --------------------
    # TASK DEPENDENCIES
    # --------------------
    (
        start
        >> generate_dummy_data
        >> aws_s3_upload
        >> glue_csv_to_parquet
        >> glue_parquet_to_snowflake
        >> dbt_run
        >> end
    )
