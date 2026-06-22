"""
ingestion_dag.py
────────────────
Daily DAG: extract from ClinicalTrials.gov API → publish to Kafka → Spark bronze write.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tariqul",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ingestion_dag",
    description="NIH ClinicalTrials.gov daily extraction and Kafka ingestion",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["nih", "ingestion", "kafka"],
) as dag:

    def extract_task(**context) -> str:
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from extract import extract
        run_date = context["ds"]
        out_path = extract(run_date=run_date)
        context["ti"].xcom_push(key="raw_path", value=out_path)
        return out_path

    def publish_to_kafka(**context) -> dict:
        import json
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from kafka_producer import publish_studies

        raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="extract")
        if not raw_path:
            raise ValueError("No raw path from extract task")

        with open(raw_path) as f:
            data = json.load(f)

        result = publish_studies(data.get("studies", []))
        if result["failed"] > 0:
            raise ValueError(f"Kafka publish had {result['failed']} failures")
        return result

    def upload_to_s3(**context) -> None:
        import boto3
        import os
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from config import S3_RAW_BUCKET

        raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="extract")
        if not raw_path:
            return

        run_date = context["ds"]
        year, month, day = run_date.split("-")
        s3_key = f"json/{year}/{month}/{day}/studies.json"

        s3 = boto3.client("s3")
        s3.upload_file(raw_path, S3_RAW_BUCKET, s3_key)

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    t_publish_kafka = PythonOperator(
        task_id="publish_to_kafka",
        python_callable=publish_to_kafka,
    )

    t_upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    t_spark_streaming_check = BashOperator(
        task_id="verify_bronze_write",
        bash_command=(
            "aws s3 ls s3://$S3_BRONZE_BUCKET/parquet/ingest_date={{ ds }}/ "
            "| wc -l | xargs -I{} test {} -gt 0"
        ),
    )

    t_extract >> t_publish_kafka >> t_upload_s3 >> t_spark_streaming_check
