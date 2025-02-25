from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from datetime import datetime
from kubernetes.client import models as k8s


namespace = Variable.get("namespace", default_var="default")
env = Variable.get("env", default_var="dev")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

dag = DAG("elt_pipeline", default_args=default_args, schedule_interval="@daily")


extract_env_vars = [
    k8s.V1EnvVar(name="DB_HOST", value=Variable.get("DB_HOST")),
    k8s.V1EnvVar(name="DB_USER", value=Variable.get("DB_USER")),
    k8s.V1EnvVar(name="DB_PASSWORD", value=Variable.get("DB_PASSWORD")),
    k8s.V1EnvVar(name="DB_NAME", value=Variable.get("DB_NAME")),
    k8s.V1EnvVar(name="DB_TABLE", value="source_transactions"),
    k8s.V1EnvVar(name="GCS_BUCKET", value=Variable.get("GCS_BUCKET")),
    k8s.V1EnvVar(name="ENV", value=env)
]


load_env_vars = [
    k8s.V1EnvVar(name="SNOWFLAKE_ACCOUNT", value=Variable.get("SNOWFLAKE_ACCOUNT")),
    k8s.V1EnvVar(name="SNOWFLAKE_USER", value=Variable.get("SNOWFLAKE_USER")),
    k8s.V1EnvVar(name="SNOWFLAKE_PASSWORD", value=Variable.get("SNOWFLAKE_PASSWORD")),
    k8s.V1EnvVar(name="SNOWFLAKE_DATABASE", value=Variable.get("SNOWFLAKE_DATABASE")),
    k8s.V1EnvVar(name="SNOWFLAKE_SCHEMA", value=Variable.get("SNOWFLAKE_SCHEMA")),
    k8s.V1EnvVar(name="SNOWFLAKE_WAREHOUSE", value=Variable.get("SNOWFLAKE_WAREHOUSE")),
    k8s.V1EnvVar(name="TABLE_NAME", value="transactions"),
    k8s.V1EnvVar(name="ENV", value=env)
]


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)


task_extract_upload_gcs = KubernetesPodOperator(
    task_id="task_extract_upload_gcs",
    name="task_extract_upload_gcs",
    namespace=namespace,
    image="gcr.io/my-gcp-project/extract_upload_to_gcs:latest",
    cmds=["python", "/scripts/entrypoint.py", "extract_upload_to_gcs"],
    env_vars=extract_env_vars,
    is_delete_operator_pod=True,
    dag=dag,
)


task_load_to_snowflake = KubernetesPodOperator(
    task_id="load_to_snowflake",
    name="load_to_snowflake",
    namespace=namespace,
    image="gcr.io/my-gcp-project/load_to_snowflake:latest",
    cmds=["python", "/scripts/entrypoint.py", "load_to_snowflake"],
    env_vars=load_env_vars,
    is_delete_operator_pod=True,
    dag=dag,
)


start >> task_extract_upload_gcs >> task_load_to_snowflake >> end
