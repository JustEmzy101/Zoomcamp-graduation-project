from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime
import requests

# INTERNAL K8S ADDRESS
# Format: http://<service-name>.<namespace>.svc.cluster.local:<port>/api/v1
AIRBYTE_API_URL = "http://airbyte-airbyte-server-svc.airbyte.svc.cluster.local:8001/api/v1"
CONNECTION_ID = "57d583a2-1f63-476d-9a08-0decd365f8a4"

def check_for_schema_drift():
    """
    Queries the Airbyte API internally to check for drift before starting the sync.
    """
    # Use the web_backend endpoint because it provides the 'schemaChange' status
    url = f"{AIRBYTE_API_URL}/web_backend/connections/get"
    
    payload = {
        "connectionId": CONNECTION_ID,
        "withRefreshedCatalog": True  # This forces Airbyte to check the source schema NOW
    }
    
    response = requests.post(url, json=payload)
    response.raise_for_status()
    
    data = response.json()
    drift_status = data.get("schemaChange", "no_change")

    if drift_status != "no_change":
        # This will fail the Airflow task and prevent the next task from running
        raise Exception(f"🚨 BREAKING: Schema drift ({drift_status}) detected on connection {CONNECTION_ID}.")
    
    print("✅ No schema drift detected. Proceeding...")

with DAG(
    "airbyte_k3s_drift_protection",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    check_drift = PythonOperator(
        task_id="check_schema_drift",
        python_callable=check_for_schema_drift
    )

    sync_data = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_job",
        airbyte_conn_id="airbyte_default", # Ensure this connection uses the internal cluster URL
        connection_id=CONNECTION_ID,
        asynchronous=False
    )

    check_drift >> sync_data
