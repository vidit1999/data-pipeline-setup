from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os


def my_continuous_task(**context):
    """
    This function will run every time the DAG is triggered.
    You can put any logic here—DB reads/writes, HTTP calls, etc.
    """
    # Example: log invocation time and run_id
    run_id = context['run_id']
    exec_date = context['execution_date']
    logging.info(f"Running continuous task. run_id={run_id}, execution_date={exec_date}")

    # … your custom logic here …
    return f"Task completed at {datetime.utcnow().isoformat()}"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='continuous_python_dag',
    default_args=default_args,
    description='A continuously scheduled PythonOperator DAG',
    # schedule="* * * * *",
    schedule="@continuous",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['example', 'continuous'],
) as dag:

    continuous_task = PythonOperator(
        task_id='run_my_continuous_task',
        python_callable=my_continuous_task,
    )
