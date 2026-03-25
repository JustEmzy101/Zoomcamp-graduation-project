from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='trigger_airbyte_sync',
    default_args={'retries': 1},
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    trigger_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_task',
        airbyte_conn_id='airbyte_default',
        # Replace this with the UUID from your Airbyte Connection URL
        connection_id='57d583a2-1f63-476d-9a08-0decd365f8a4',
        asynchronous=False, # Set to True if you don't want to wait for it to finish
     # 1 hour timeout
        wait_seconds=3      # Polling interval
    )

    trigger_sync
