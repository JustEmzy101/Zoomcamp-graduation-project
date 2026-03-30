import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.empty import EmptyOperator

# git-sync mounts the full repo 
REPO_ROOT = os.path.join(os.path.dirname(__file__), "../..")

with open(os.path.join(REPO_ROOT, "spark_apps/using_spark.yaml")) as f:
    spark_app = yaml.safe_load(f)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="gcs_to_bq",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Schema Evo","spark", "gcs", "bigquery"],
    default_args=default_args,
    
) as dag:

    start = EmptyOperator(task_id="start")

    gcs_to_bq = SparkKubernetesOperator(
        task_id="gcs_to_bq_task",
        namespace="spark",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        application_file=yaml.dump(spark_app),
        delete_on_termination=True,
    )

    end = EmptyOperator(task_id="end")

    start >> gcs_to_bq >> end
