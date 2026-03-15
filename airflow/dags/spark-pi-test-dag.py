from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="spark_pi",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark"]
) as dag:

    spark_pi = SparkKubernetesOperator(
        task_id="spark_pi_task",
        namespace="spark",
        application_file="/opt/airflow/dags/spark_pi.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False
    )
