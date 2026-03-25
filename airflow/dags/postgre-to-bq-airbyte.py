from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests

AIRBYTE_API_URL = "http://airbyte-airbyte-server-svc.airbyte.svc.cluster.local:8001/api/v1"
CONNECTION_ID = "57d583a2-1f63-476d-9a08-0decd365f8a4"


def get_airbyte_auth():
    """Get Basic Auth credentials from the Airflow connection."""
    conn = BaseHook.get_connection("airbyte")
    return (conn.login or "airbyte", conn.password or "password")


def check_for_schema_drift():
    auth = get_airbyte_auth()

    response = requests.post(
        f"{AIRBYTE_API_URL}/web_backend/connections/get",
        json={
            "connectionId": CONNECTION_ID,
            "withRefreshedCatalog": True
        },
        auth=auth,   # ← Basic Auth, not Bearer token
    )
    response.raise_for_status()

    drift_status = response.json().get("schemaChange", "no_change")

    if drift_status != "no_change":
        raise Exception(
            f"Schema drift ({drift_status}) detected on "
            f"connection {CONNECTION_ID}. Sync blocked."
        )

    print("No schema drift detected. Proceeding with sync...")


with DAG(
    dag_id="airbyte_k3s_drift_protection",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["airbyte", "ingestion"],
) as dag:

    check_drift = PythonOperator(
        task_id="check_schema_drift",
        python_callable=check_for_schema_drift,
    )

    sync_data = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_job",
        airbyte_conn_id="airbyte",
        connection_id=CONNECTION_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )

    check_drift >> sync_data
