from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator


DEFAULT_ARGS = {
    "owner": "nadav",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BUCKET = "nadav-lakehouse-dev"

with DAG(
    dag_id="s3_to_glue_to_athena",
    default_args=DEFAULT_ARGS,
    description="Wait for S3 file, run Glue, then run Athena downstream query",
    start_date=datetime(2026, 3, 2),
    schedule="@daily",
    catchup=False,
    tags=["week4", "aws", "orchestration"],
) as dag:

    wait_for_s3 = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=BUCKET,
        bucket_key="raw/transactions/dt={{ ds }}/transactions.csv",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        poke_interval=60,
        timeout=60 * 60,
        mode="poke",
    )

    run_glue = GlueJobOperator(
        task_id="run_glue_job",
        job_name="transactions_raw_to_curated",
        script_args={
            "--run_date": "{{ ds }}",
            "--s3_input_path": f"s3://{BUCKET}/raw/transactions/",
            "--s3_output_path": f"s3://{BUCKET}/curated/transactions/",
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    run_athena = AthenaOperator(
        task_id="run_downstream_athena_query",
        query="MSCK REPAIR TABLE lakehouse_db.transactions;",
        database="lakehouse_db",
        output_location=f"s3://{BUCKET}/athena-results/",
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    wait_for_s3 >> run_glue >> run_athena