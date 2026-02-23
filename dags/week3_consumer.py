from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="week3_consumer",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["week3"],
) as dag:

    # Instead of ExternalTaskSensor, check if the output file exists
    wait_for_etl = BashSensor(
        task_id="wait_for_etl",
        bash_command="ls /opt/airflow/data/clean/*.csv >/dev/null 2>&1",
        poke_interval=10,
        timeout=300,
        mode='reschedule',
    )

    downstream_task = BashOperator(
        task_id="consume_clean_data",
        bash_command='echo "Consuming cleaned data"',
    )

    wait_for_etl >> downstream_task