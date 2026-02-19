from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="week2_three_tasks",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    schedule_interval='*/1 * * * *',
    catchup=False,
    tags=["week2"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command='echo "The Current date is:" && date',
    )

    process = BashOperator(
        task_id="process",
        bash_command='echo "Step 2: Doing real work (placeholder)"',
    )

    list_files = BashOperator(
        task_id="list_files",
        bash_command="ls -la /opt/airflow/dags",
    )

    hello >> process >> list_files
