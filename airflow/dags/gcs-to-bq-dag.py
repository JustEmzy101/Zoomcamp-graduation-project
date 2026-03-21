import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Load the SparkApplication spec from external file — clean separation
with open(os.path.join(os.path.dirname(__file__), "../spark_apps/gcs_to_bq.yaml")) as f:
    spark_app = yaml.safe_load(f)

with DAG(
    dag_id="gcs_to_bq",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",       # daily at 3am
    catchup=False,
    tags=["spark", "gcs", "bigquery"],
    default_args={
        "retries": 1,
        "retry_delay": 300,
    }
) as dag:
    gcs_to_bq = SparkKubernetesOperator(
        task_id="gcs_to_bq_task",
        namespace="spark",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        application_file=yaml.dump(spark_app),
    )

