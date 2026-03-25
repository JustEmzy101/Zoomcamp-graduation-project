from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime

with DAG(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule='@daily',
         start_date=datetime(2024, 1, 1)
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_money_json_example',
        airbyte_conn_id='airbyte_default',
        connection_id='57d583a2-1f63-476d-9a08-0decd365f8a4',
        asynchronous=False,
        wait_seconds=3
    )
