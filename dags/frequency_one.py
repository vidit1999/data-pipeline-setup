from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import logging
import os
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 0,
}

FREQUENCY = 1

with DAG(
    dag_id='frequency_one',
    default_args=default_args,
    description='Frequency one DAG',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['example', 'one'],
) as dag:
    for pipeline in requests.get("http://backend:8000/configs/").json():
        if pipeline["frequency"] == FREQUENCY:
            pipeline_topic = pipeline["topic_name"]
            pipeline_id = pipeline["id"]
            pipeline_mode = pipeline["mode"]
            task_id = f"docker_task_append_{pipeline_id}_{pipeline_topic}"

            append_task = DockerOperator(
                task_id=task_id,
                image=os.environ['COMPOSE_PROJECT_NAME'] + "-" + "jupyter-lab",
                command=["spark-submit", "append_pipeline.py", f"{pipeline_id}"],
                network_mode=os.environ['COMPOSE_PROJECT_NAME'] + '_default',
                mem_limit="1g",
                environment={
                    "AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT_URL"],
                    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                },
                mount_tmp_dir=False,
                auto_remove='force',
                tty=True
            )

            if pipeline_mode == "upsert":
                task_id = f"docker_task_upsert_{pipeline_id}_{pipeline_topic}"

                upsert_task = DockerOperator(
                    task_id=task_id,
                    image=os.environ['COMPOSE_PROJECT_NAME'] + "-" + "jupyter-lab",
                    command=["spark-submit", "upsert_pipeline.py", f"{pipeline_id}"],
                    network_mode=os.environ['COMPOSE_PROJECT_NAME'] + '_default',
                    mem_limit="1g",
                    environment={
                        "AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT_URL"],
                        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                    },
                    mount_tmp_dir=False,
                    auto_remove='force',
                    tty=True
                )

                append_task >> upsert_task

