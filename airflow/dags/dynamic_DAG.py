import os
import yaml
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime,timedelta

NAMESPACE       = "spark"
SERVICE_ACCOUNT = "spark"
IMAGE           = "apache/spark-py:v3.4.0"
SPARK_VERSION   = "3.4.0"
GIT_REPO        = "https://github.com/JustEmzy101/Zoomcamp-graduation-project.git"
GIT_BRANCH      = "main"
# Path inside the repo where main.py lives
GIT_SCRIPT_SRC  = "/tmp/repo/spark-jobs"


GCS_JAR         = "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
BQ_JAR          = "https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar"

# Your project args (mirrors your existing manifest arguments style)
PROJECT_ID      = "turing-chess-484608-k4"
GCS_RAW         = "gs://my-airbyte-raw-landing/raw/public/"
BQ_DATASET      = "efinance"
GCS_TEMP_BUCKET = "my-airbyte-raw-landing/raw"
GCS_QUARANTINE  = "gs://my-airbyte-raw-landing/raw/quarantine"

default_args = {
    "owner": "Marwan-Mohamed",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False, # you can setup the connection for email in Airflow and turn this true
    "email": ["mmzidane101@gmail.com"],
}
def git_sync_init_container(table: str) -> dict:
    """
    Clones repo at runtime and copies the entry point script
    into the shared emptyDir volume. Same pattern as your existing manifest.
    """
    return {
        "name": "git-sync",
        "image": "alpine/git:latest",
        "command": ["sh", "-c"],
        "args": [
            f"git clone --depth 1 --branch {GIT_BRANCH} {GIT_REPO} /tmp/repo "
            f"&& cp {GIT_SCRIPT_SRC}/{table}_main.py /opt/spark/jobs/{table}_main.py "
            f"&& echo 'Script pulled' && ls -la /opt/spark/jobs/"
        ],
        "volumeMounts": [{
            "name": "spark-jobs",
            "mountPath": "/opt/spark/jobs"
        }]
    }
VOLUMES = [
    {
        "name": "gcp-key",
        "secret": {"secretName": "spark-gcp-key"}
    },
    {
        "name": "spark-jobs",
        "emptyDir": {}
    }
]

VOLUME_MOUNTS = [
    {"name": "gcp-key",    "mountPath": "/etc/gcp",         "readOnly": True},
    {"name": "spark-jobs", "mountPath": "/opt/spark/jobs"},
]
EXECUTOR_CONFIG = {
    "users":                 {"instances": 1, "memory": "1g", "memoryOverhead": "512m"},
    "accounts":              {"instances": 1, "memory": "1g", "memoryOverhead": "512m"},
    "transactions":          {"instances": 1, "memory": "2g", "memoryOverhead": "1g"},   # heaviest
    "enriched_transactions": {"instances": 1, "memory": "2g", "memoryOverhead": "1g"},
    "audit":                 {"instances": 1, "memory": "1g", "memoryOverhead": "512m"},
}

def make_spark_app(table: str, date: str) -> dict:
    """
    Builds a SparkApplication CRD dict for a given table.
    Merges your existing manifest structure with per-table dynamic args.
    """
    exc = EXECUTOR_CONFIG[table]

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            # unique name per run to avoid CRD conflicts
            "name":      f"etl-{table.replace('_', '-')}-{date}",
            "namespace": NAMESPACE,
        },
        "spec": {
            "type":          "Python",
            "pythonVersion": "3",
            "mode":          "cluster",
            "image":          IMAGE,
            "imagePullPolicy": "IfNotPresent",

            # Script is pulled by git-sync into the emptyDir volume
            "mainApplicationFile": f"local:///opt/spark/jobs/{table}_main.py",

            # ── Jars (your existing URLs, no change) ──────────────────
            "deps": {
                "jars": [GCS_JAR, BQ_JAR]
            },

            # ── Per-table CLI arguments ────────────────────────────────
            # Positional: table, date, project_id, gcs_raw, bq_dataset,
            #             gcs_temp_bucket, gcs_quarantine
            "arguments": [
                table,
                date,
                PROJECT_ID,
                f"GCS_RAW/{table}",
                BQ_DATASET,
                GCS_TEMP_BUCKET,
                GCS_QUARANTINE,
            ],

            "sparkVersion": SPARK_VERSION,

            # ── Restart policy ─────────────────────────────────────────
            # Using OnFailure (better for production than Never)
            # Set to Never + retries: 0 if you prefer your original behaviour
            "restartPolicy": {
                "type":                   "OnFailure",
                "onFailureRetries":        1,
                "onFailureRetryInterval":  30,
            },

            # ── Volumes (identical to your manifest) ──────────────────
            "volumes": VOLUMES,

            # ── Driver (same as your manifest + git-sync init) ─────────
            "driver": {
                "cores":          1,
                "memory":         "2g",
                "memoryOverhead": "512m",
                "serviceAccount": SERVICE_ACCOUNT,
                "labels":         {"app": f"etl-{table}", "date": date},
                "volumeMounts":   VOLUME_MOUNTS,
                "initContainers": [git_sync_init_container(table)],
            },

            # ── Executor (scaled per table + git-sync init) ────────────
            "executor": {
                "cores":          1,
                "instances":      exc["instances"],
                "memory":         exc["memory"],
                "memoryOverhead": exc["memoryOverhead"],
                "labels":         {"app": f"etl-{table}", "date": date},
                "volumeMounts":   VOLUME_MOUNTS,
                "initContainers": [git_sync_init_container()],
            },

            # ── Spark config ───────────────────────────────────────────
            "sparkConf": {
                "spark.sql.adaptive.enabled":                    "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.hadoop.google.cloud.auth.service.account.enable": "true",
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile":
                    "/etc/gcp/key.json",
                "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
                # helps with BQ connector temp writes
                "spark.hadoop.fs.gs.impl":
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            },
        }
    }




with DAG(
    dag_id="spark-self-submetting-manifest",
    schedule=None,
    start_date=None,
    catchup=False,
    tags=["kubeflow","spark", "gcs", "bigquery", "github"],
    max_active_runs=1,
) as dag:

    date = "{{ ds }}"


    load_users = SparkKubernetesOperator(
        task_id="load_users",
        namespace=NAMESPACE,
        application_file=make_spark_app("users", date),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        poll_interval=10,       # seconds between status polls
    )

load_users
