import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def trigger_airbyte_sync():
    # 1. GET THE TOKEN
    token_url = "http://10.43.8.59:8001"
    payload = {
        "client_id": "04afdb14-1f34-4f9b-af8d-efc0d930f5b8",
        "client_secret": "e466244f389e1d723cd21d4b447ebe8e3e02371459f2c9aaf4c45b17b40b9817"
    }
    
    print(f"Attempting to get token from {token_url}...")
    token_response = requests.post(token_url, json=payload)
    token_response.raise_for_status()
    token = token_response.json().get("access_token")
    print("Token acquired successfully!")

    # 2. TRIGGER THE SYNC (Replace <CONNECTION_ID> with yours from Airbyte UI URL)
    sync_url = "http://airbyte-airbyte-server-svc.airbyte.svc.cluster.local"
    headers = {"Authorization": f"Bearer {token}"}
    sync_payload = {
        "connectionId": "57d583a2-1f63-476d-9a08-0decd365f8a4", 
        "jobType": "sync"
    }
    
    sync_response = requests.post(sync_url, json=sync_payload, headers=headers)
    print(f"Sync Response: {sync_response.text}")
    sync_response.raise_for_status()

with DAG(dag_id='debug_airbyte_connection', start_date=datetime(2024, 1, 1), schedule=None) as dag:
    PythonOperator(
        task_id='test_raw_http_sync',
        python_callable=trigger_airbyte_sync
    )
