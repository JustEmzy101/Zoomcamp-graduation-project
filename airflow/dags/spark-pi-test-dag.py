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
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        application_file={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": "spark-pi",
                "namespace": "spark"
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": "apache/spark-py:v3.4.0",
                "imagePullPolicy": "Always",
                "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
                "sparkVersion": "3.4.0",
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "cores": 1,
                    "memory": "1g",
                    "serviceAccount": "spark"
                },
                "executor": {
                    "cores": 1,
                    "instances": 2,
                    "memory": "2g"
                }
            }
        }
    )
