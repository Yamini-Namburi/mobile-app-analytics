from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


PROJECT_DIR = "/usr/local/airflow"

BUCKET_NAME = "mobile-app-analytics-dev-yourname"
AWS_CONN_ID = "aws_default"
AWS_REGION = "eu-north-1"

REPORT_MONTH = "202604"

default_args = {
    "owner": "yamini",
}


with DAG(
    dag_id="dag_app_performance_pipeline",
    default_args=default_args,
    description="App performance pipeline: mock API -> raw S3 -> Glue -> silver S3",
    start_date=datetime(2026, 4, 28),
    schedule=None,
    catchup=False,
    tags=["mobile-app-analytics", "app-performance"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    google_installs_ingest = BashOperator(
        task_id="google_installs_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/app_performance/google_installs.py
        """,
    )

    google_crashes_ingest = BashOperator(
        task_id="google_crashes_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/app_performance/google_crashes.py
        """,
    )

    apple_installs_ingest = BashOperator(
        task_id="apple_installs_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/app_performance/apple_installs.py
        """,
    )

    apple_crashes_ingest = BashOperator(
        task_id="apple_crashes_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/app_performance/apple_crashes.py
        """,
    )

    google_installs_glue = GlueJobOperator(
        task_id="google_installs_raw_to_silver",
        job_name="google_installs_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    google_crashes_glue = GlueJobOperator(
        task_id="google_crashes_raw_to_silver",
        job_name="google_crashes_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    apple_installs_glue = GlueJobOperator(
        task_id="apple_installs_raw_to_silver",
        job_name="apple_installs_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    apple_crashes_glue = GlueJobOperator(
        task_id="apple_crashes_raw_to_silver",
        job_name="apple_crashes_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    start >> [
        google_installs_ingest,
        google_crashes_ingest,
        apple_installs_ingest,
        apple_crashes_ingest,
    ]

    google_installs_ingest >> google_installs_glue >> end
    google_crashes_ingest >> google_crashes_glue >> end
    apple_installs_ingest >> apple_installs_glue >> end
    apple_crashes_ingest >> apple_crashes_glue >> end