from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import subprocess

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

dag = DAG("elt_pipeline", default_args=default_args, schedule_interval="@daily")


def extract_upload_gcs():
    subprocess.run(["python", "/container/extract_upload_local/scripts/extract_upload_to_gcs.py"], check=True)


def load_to_bq():
    subprocess.run(["python", "/container/load_to_bq/scripts/load_to_bq.py"], check=True)


def load_to_snowflake():
    subprocess.run(["python", "/container/load_to_snowflake/scripts/load_to_snowflake.py"], check=True)


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

task_extract_upload_gcs = PythonOperator(
    task_id="extract_upload_gcs",
    python_callable=extract_upload_gcs,
    dag=dag,
)

task_load_to_bq = PythonOperator(
    task_id="load_to_bq",
    python_callable=load_to_bq,
    dag=dag,
)

task_load_to_snowflake = PythonOperator(
    task_id="load_to_snowflake",
    python_callable=load_to_snowflake,
    dag=dag,
)

start >> task_extract_upload_gcs >> [task_load_to_bq, task_load_to_snowflake] >> end
