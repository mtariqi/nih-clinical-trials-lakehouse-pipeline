"""
ml_retrain_dag.py
─────────────────
Weekly DAG: dbt run → silver/gold → ML model retraining → MLflow registration.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tariqul",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ml_retrain_dag",
    description="Weekly dbt run + ML model retraining with MLflow tracking",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["nih", "ml", "dbt", "mlflow"],
) as dag:

    # Wait for quality checks to pass before running dbt
    t_wait_quality = ExternalTaskSensor(
        task_id="wait_for_quality_checks",
        external_dag_id="quality_dag",
        external_task_id="notify_quality_pass",
        timeout=3600,
        mode="reschedule",
    )

    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    )

    def retrain_model(**context) -> dict:
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from ml_model import train_and_register

        run_date = context["ds"]
        metrics = train_and_register(run_date=run_date)
        context["ti"].xcom_push(key="model_metrics", value=metrics)
        return metrics

    t_retrain = PythonOperator(
        task_id="retrain_ml_model",
        python_callable=retrain_model,
    )

    def log_metrics(**context) -> None:
        metrics = context["ti"].xcom_pull(key="model_metrics", task_ids="retrain_ml_model")
        if metrics:
            print(f"Model retrain complete — MAE: {metrics.get('mae'):.4f}, "
                  f"R2: {metrics.get('r2'):.4f}, "
                  f"AUC: {metrics.get('auc', 'N/A')}")

    t_log = PythonOperator(
        task_id="log_model_metrics",
        python_callable=log_metrics,
    )

    t_wait_quality >> t_dbt_run >> t_dbt_test >> t_retrain >> t_log
