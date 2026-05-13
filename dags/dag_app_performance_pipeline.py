from datetime import datetime
import importlib
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


PROJECT_DIR = "/usr/local/airflow"

BUCKET_NAME = "mobile-app-analytics-dev-yourname"
AWS_CONN_ID = "aws_default"
AWS_REGION = "eu-north-1"

REPORT_MONTH = "202604"

SOURCE_SYSTEMS = ["google", "apple"]
REPORT_TYPES = ["installs", "crashes"]

default_args = {
    "owner": "yamini",
}


def run_ingestion_script(module_path: str):
    """
    Dynamically import an ingestion module and run its main() function.
    Example module_path:
    include.ingestion.app_performance.apple_crashes
    """

    if PROJECT_DIR not in sys.path:
        sys.path.append(PROJECT_DIR)

    module = importlib.import_module(module_path)

    if not hasattr(module, "main"):
        raise AttributeError(f"{module_path} does not have a main() function")

    module.main()


with DAG(
    dag_id="dag_app_performance_pipeline_python_operator",
    default_args=default_args,
    description="App performance pipeline using PythonOperator",
    start_date=datetime(2026, 4, 28),
    schedule=None,
    catchup=False,
    tags=["mobile-app-analytics", "app-performance", "python-operator"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for source_system in SOURCE_SYSTEMS:
        for report_type in REPORT_TYPES:

            ingest_task_id = f"{source_system}_{report_type}_ingest"
            glue_task_id = f"{source_system}_{report_type}_raw_to_silver"

            module_path = (
                f"include.ingestion.app_performance."
                f"{source_system}_{report_type}"
            )

            glue_job_name = f"{source_system}_{report_type}_raw_to_silver"

            ingestion_task = PythonOperator(
                task_id=ingest_task_id,
                python_callable=run_ingestion_script,
                op_kwargs={
                    "module_path": module_path,
                },
            )

            glue_task = GlueJobOperator(
                task_id=glue_task_id,
                job_name=glue_job_name,
                script_args={
                    "--bucket_name": BUCKET_NAME,
                    "--report_month": REPORT_MONTH,
                },
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
                wait_for_completion=True,
            )

            start >> ingestion_task >> glue_task >> end