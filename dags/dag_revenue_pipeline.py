from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


PROJECT_DIR = "/usr/local/airflow"

BUCKET_NAME = "mobile-app-analytics-dev-yourname"
AWS_CONN_ID = "aws_default"
AWS_REGION = "eu-north-1"

GOOGLE_REPORT_MONTH = "202604"
APPLE_SALES_REPORT_MONTH = "202604"
APPLE_FINANCE_REPORT_MONTH = "2026-03"

default_args = {
    "owner": "yamini",
}


with DAG(
    dag_id="dag_revenue_pipeline",
    default_args=default_args,
    description="Revenue pipeline: mock API -> raw S3 -> Glue -> silver S3",
    start_date=datetime(2026, 4, 28),
    schedule=None,
    catchup=False,
    tags=["mobile-app-analytics", "revenue"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    google_estimated_sales_ingest = BashOperator(
        task_id="google_estimated_sales_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/revenue/google_estimated_sales.py
        """,
    )

    google_earnings_ingest = BashOperator(
        task_id="google_earnings_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/revenue/google_earnings.py
        """,
    )

    apple_sales_ingest = BashOperator(
        task_id="apple_sales_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/revenue/apple_sales.py
        """,
    )

    apple_finance_ingest = BashOperator(
        task_id="apple_finance_ingest",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        python include/ingestion/revenue/apple_finance.py
        """,
    )

    google_estimated_sales_glue = GlueJobOperator(
        task_id="google_estimated_sales_raw_to_silver",
        job_name="google_estimated_sales_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": GOOGLE_REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    google_earnings_glue = GlueJobOperator(
        task_id="google_earnings_raw_to_silver",
        job_name="google_earnings_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": GOOGLE_REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    apple_sales_glue = GlueJobOperator(
        task_id="apple_sales_raw_to_silver",
        job_name="apple_sales_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": APPLE_SALES_REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    apple_finance_glue = GlueJobOperator(
        task_id="apple_finance_raw_to_silver",
        job_name="apple_finance_raw_to_silver",
        script_args={
            "--bucket_name": BUCKET_NAME,
            "--report_month": APPLE_FINANCE_REPORT_MONTH,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
    )

    start >> [
        google_estimated_sales_ingest,
        google_earnings_ingest,
        apple_sales_ingest,
        apple_finance_ingest,
    ]

    google_estimated_sales_ingest >> google_estimated_sales_glue >> end
    google_earnings_ingest >> google_earnings_glue >> end
    apple_sales_ingest >> apple_sales_glue >> end
    apple_finance_ingest >> apple_finance_glue >> end