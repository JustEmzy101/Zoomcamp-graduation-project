import os
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime


REPO_ROOT = os.path.join(os.path.dirname(__file__), "../..")
with open(os.path.join(REPO_ROOT, "spark_apps/using_spark.yaml")) as f:
    spark_app = yaml.safe_load(f)


with DAG(
    dag_id="spark_using_kubeflow_operator",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kubeflow","spark", "gcs", "bigquery", "github"],
    max_active_runs=1,
) as dag:

    
    starting = EmptyOperator(task_id="start")

    # Task 2: Submit to Spark Kubernetes Operator
    submit_spark_job = SparkKubernetesOperator(
        task_id="new_submit_gcs_to_bq",
        namespace="spark",
        application_file=yaml.dump(spark_app),
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=True,

    )

    fetch_yaml >> submit_spark_job
