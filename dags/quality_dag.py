"""
quality_dag.py
──────────────
Hourly DAG: run Great Expectations checkpoints on bronze and silver layers.
Fails loudly on quality violations — downstream DAGs have sensor dependencies on this.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tariqul",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="quality_dag",
    description="Hourly Great Expectations data quality checks on bronze and silver",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["nih", "quality", "great-expectations"],
) as dag:

    def run_checkpoint(checkpoint_name: str, **context) -> None:
        import great_expectations as gx

        context_gx = gx.get_context(
            context_root_dir="/opt/airflow/great_expectations"
        )
        result = context_gx.run_checkpoint(checkpoint_name=checkpoint_name)

        if not result["success"]:
            failed = [
                vr["expectation_config"]["expectation_type"]
                for vr in result.list_validation_results()
                if not vr["success"]
            ]
            raise ValueError(
                f"Checkpoint '{checkpoint_name}' failed. "
                f"Failing expectations: {failed}"
            )

    t_bronze_check = PythonOperator(
        task_id="bronze_quality_check",
        python_callable=run_checkpoint,
        op_kwargs={"checkpoint_name": "bronze_checkpoint"},
    )

    t_silver_check = PythonOperator(
        task_id="silver_quality_check",
        python_callable=run_checkpoint,
        op_kwargs={"checkpoint_name": "silver_checkpoint"},
    )

    def notify_quality_pass(**context) -> None:
        run_date = context["ds"]
        print(f"[{run_date}] All quality checks passed — pipeline cleared for downstream.")

    t_notify = PythonOperator(
        task_id="notify_quality_pass",
        python_callable=notify_quality_pass,
    )

    t_bronze_check >> t_silver_check >> t_notify
