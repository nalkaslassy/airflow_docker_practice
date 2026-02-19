from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="week2_etl_scripts",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["week2"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/scripts/extract.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/scripts/transform.py",
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/scripts/load.py",
    )

    extract >> transform >> load
