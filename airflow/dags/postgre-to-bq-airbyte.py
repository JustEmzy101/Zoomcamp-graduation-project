from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests

AIRBYTE_API_URL = "http://airbyte-airbyte-server-svc.airbyte.svc.cluster.local:8001"
CONNECTION_ID = "57d583a2-1f63-476d-9a08-0decd365f8a4"


def get_airbyte_token() -> str:
    """Fetch a short-lived token using the Client ID/Secret from the Airflow connection."""
    conn = BaseHook.get_connection("airbyte")
    extras = conn.extra_dejson

    response = requests.post(
        f"{AIRBYTE_API_URL}/api/v1/applications/token",
        json={
            "client_id": extras.get("client_id"),
            "client_secret": extras.get("client_secret"),
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


def check_for_schema_drift():
    """
    Calls the Airbyte API to check for schema drift before triggering sync.
    Fails the task if breaking drift is detected, blocking the sync.
    """
    token = get_airbyte_token()

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(
        f"{AIRBYTE_API_URL}/api/v1/connections/{CONNECTION_ID}",
        headers=headers,
    )
    response.raise_for_status()

    data = response.json()
    drift_status = data.get("schemaChange", "no_change")

    if drift_status != "no_change":
        raise Exception(
            f"BREAKING: Schema drift ({drift_status}) detected "
            f"on connection {CONNECTION_ID}. Sync blocked."
        )

    print("No schema drift detected. Proceeding with sync...")


with DAG(
    dag_id="airbyte_k3s_drift_protection",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",          # ← schedule_interval removed in Airflow 3.x
    catchup=False,
    tags=["airbyte", "ingestion"],
) as dag:

    check_drift = PythonOperator(
        task_id="check_schema_drift",
        python_callable=check_for_schema_drift,
    )

    sync_data = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_job",
        airbyte_conn_id="airbyte",   # ← matches connection created in UI
        connection_id=CONNECTION_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,             # polling interval while waiting for sync
    )

    check_drift >> sync_data
