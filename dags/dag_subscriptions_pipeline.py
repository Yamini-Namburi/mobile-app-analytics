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
    dag_id="dag_subscriptions_pipeline",
    default_args=default_args,
    description="Subscriptions pipeline: mock API -> raw S3 -> Glue -> silver S3",
    start_date=datetime(2026, 4, 28),
    schedule=None,
    catchup=False,
    tags=["mobile-app-analytics", "subscriptions"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    google_subscriptions_ingest = BashOperator(
        task_id="google_subscriptions_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/subscriptions/google_subscriptions.py
        """,
    )

    apple_subscriptions_ingest = BashOperator(
        task_id="apple_subscriptions_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/subscriptions/apple_subscriptions.py
        """,
    )

    google_subscriptions_glue = GlueJobOperator(
        task_id="google_subscriptions_raw_to_silver",
        job_name="google_subscriptions_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    apple_subscriptions_glue = GlueJobOperator(
        task_id="apple_subscriptions_raw_to_silver",
        job_name="apple_subscriptions_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    start >> [
        google_subscriptions_ingest,
        apple_subscriptions_ingest,
    ]

    google_subscriptions_ingest >> google_subscriptions_glue >> end
    apple_subscriptions_ingest >> apple_subscriptions_glue >> end