# dags/airbyte_sync_dag.py
import time
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Internal k3s cluster DNS for Airbyte
AIRBYTE_BASE_URL = "http://airbyte-airbyte-server-svc.airbyte.svc.cluster.local:8001/api/v1"

# Get your connection ID from the Airbyte UI URL:
# /workspaces/<workspace_id>/connections/<CONNECTION_ID>/status
AIRBYTE_CONNECTION_ID = "d1161597-ff7b-4b75-8d76-528eda729122"


def trigger_airbyte_sync(connection_id: str) -> str:
    """Triggers an Airbyte sync and returns the job ID."""
    response = requests.post(
        f"{AIRBYTE_BASE_URL}/connections/sync",
        json={"connectionId": connection_id},
        timeout=30,
    )
    response.raise_for_status()
    job_id = response.json()["job"]["id"]
    print(f"Triggered Airbyte sync. Job ID: {job_id}")
    return str(job_id)


def wait_for_airbyte_sync(connection_id: str, poll_interval: int = 10, timeout: int = 3600):
    """Polls Airbyte until the sync job for a connection completes."""
    # First, get the latest job for this connection
    response = requests.post(
        f"{AIRBYTE_BASE_URL}/jobs/list",
        json={"configId": connection_id, "configTypes": ["sync"], "count": 1},
        timeout=30,
    )
    response.raise_for_status()
    jobs = response.json().get("jobs", [])
    if not jobs:
        raise Exception("No sync job found for connection.")

    job_id = jobs[0]["job"]["id"]
    print(f"Monitoring Airbyte Job ID: {job_id}")

    elapsed = 0
    while elapsed < timeout:
        status_response = requests.post(
            f"{AIRBYTE_BASE_URL}/jobs/get",
            json={"id": job_id},
            timeout=30,
        )
        status_response.raise_for_status()
        job_status = status_response.json()["job"]["status"]
        print(f"Job {job_id} status: {job_status} (elapsed: {elapsed}s)")

        if job_status == "succeeded":
            print(f"Airbyte sync {job_id} completed successfully.")
            return
        elif job_status in ("failed", "cancelled"):
            raise Exception(f"Airbyte sync {job_id} ended with status: {job_status}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Airbyte sync {job_id} timed out after {timeout}s.")


with DAG(
    dag_id="airbyte_oss_sync",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airbyte", "ingestion"],
) as dag:

    trigger_sync = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_airbyte_sync,
        op_kwargs={"connection_id": AIRBYTE_CONNECTION_ID},
    )

    wait_for_sync = PythonOperator(
        task_id="wait_for_airbyte_sync",
        python_callable=wait_for_airbyte_sync,
        op_kwargs={
            "connection_id": AIRBYTE_CONNECTION_ID,
            "poll_interval": 10,
            "timeout": 3600,
        },
    )

    trigger_sync >> wait_for_sync
