from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime
import requests

def fetch_spark_yaml(**kwargs):
    """Pull the latest SparkApplication YAML from GitHub"""
    url = "https://raw.githubusercontent.com/JustEmzy101/Zoomcamp-graduation-project/main/spark_apps/gcs_to_bq.yaml"
    response = requests.get(url)
    response.raise_for_status()
    
    yaml_content = response.text
    
    # Make the job name unique per DAG run (very important!)
    yaml_content = yaml_content.replace(
        "name: gcs-to-bq",
        f"name: gcs-to-bq-v1"
    )
    
    # Push the full YAML to XCom so the next task can use it
    kwargs['ti'].xcom_push(key='spark_yaml', value=yaml_content)

with DAG(
    dag_id="gcs_to_bq_github_fresh",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "gcs", "bigquery", "github"],
    max_active_runs=1,
) as dag:

    # Task 1: Pull latest YAML from your GitHub repo
    fetch_yaml = PythonOperator(
        task_id="fetch_latest_yaml_from_github",
        python_callable=fetch_spark_yaml,
    )

    # Task 2: Submit to Spark Kubernetes Operator
    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_gcs_to_bq",
        namespace="spark",
        application_file="{{ ti.xcom_pull(task_ids='fetch_latest_yaml_from_github', key='spark_yaml') }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        get_logs=True,
        delete_on_termination=True,
        is_delete_operator_pod=True,
    )

    fetch_yaml >> submit_spark_job
