from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
from airflow.providers.standard.sensors.filesystem import FileSensor

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
        do_xcom_push=True,
    )
wait_for_raw = FileSensor(
    task_id="wait_for_raw",
    filepath="data/raw",  # Relative path from the base path
    fs_conn_id="fs_default",  # Explicitly specify connection
    poke_interval=5,
    timeout=60,
)
transform = BashOperator(
    task_id="transform",
    bash_command="""
    FILE={{ ti.xcom_pull(task_ids='extract') }}
    python /opt/airflow/scripts/transform.py $FILE
    """,
    )   

load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/scripts/load.py",
    )

extract >>  wait_for_raw >> transform >> load
