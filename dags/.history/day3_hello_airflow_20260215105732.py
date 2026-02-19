from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="week1_basics",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["week1"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command='echo "Hello from Airflow" && date',
    )

    fail_once = BashOperator(
        task_id="fail_once",
        bash_command='echo "About to fail" && exit 1',
    )

    list_files = BashOperator(
        task_id="list_files",
        bash_command="ls -la /opt/airflow/dags",
    )

    hello >> fail_once >> list_files
