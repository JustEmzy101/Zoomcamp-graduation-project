from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import time
import logging

# ── Config ────────────────────────────────────────────────────────────────────
AIRBYTE_API_URL = "http://airbyte-airbyte-server-svc.airbyte.svc.cluster.local:8001/api/v1"
CONNECTION_ID   = "6495259a-9b4d-4e8f-8a7c-3fc02bc90958"
POLL_INTERVAL   = 1    # seconds between status checks
TIMEOUT         = 3600  # max seconds to wait for sync
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Task 1: Schema Drift Check ────────────────────────────────────────────────
def check_for_schema_drift():
    """
    Calls Airbyte's web_backend to check if the source schema has drifted.
    Blocks the sync if drift is detected
    """
    response = requests.post(
        f"{AIRBYTE_API_URL}/web_backend/connections/get",
        json={
            "connectionId": CONNECTION_ID,
            "withRefreshedCatalog": True
        },
        timeout=60,
    )
    response.raise_for_status()

    drift_status = response.json().get("schemaChange", "no_change")
    logger.info(f"Schema drift status: {drift_status}")
    if drift_status != "no_change":
        logger.warn(f"Schema drift detected ({drift_status}) on {CONNECTION_ID} ")
            
            
    else:
        logger.info("No schema drift detected. Proceeding with sync...")



# ── Task 2: Trigger Sync ──────────────────────────────────────────────────────
def trigger_airbyte_sync(**context):
    """
    Triggers the Airbyte sync job and pushes the job ID to XCom
    so the next task can poll it.
    """
    response = requests.post(
        f"{AIRBYTE_API_URL}/connections/sync",
        json={"connectionId": CONNECTION_ID},
        timeout=30,
    )
    response.raise_for_status()

    job_id = response.json()["job"]["id"]

    logger.info(f"Airbyte sync triggered. Job ID: {job_id}")
    # Push job_id to XCom for the polling task
    context["ti"].xcom_push(key="airbyte_job_id", value=job_id)


# ── Task 3: Poll Until Complete ───────────────────────────────────────────────
def wait_for_airbyte_sync(**context):
    """
    Polls Airbyte job status every POLL_INTERVAL seconds until
    the job succeeds, fails, or times out.
    """
    job_id = context["ti"].xcom_pull(
        task_ids="trigger_airbyte_sync",
        key="airbyte_job_id"
    )

    if not job_id:
        raise ValueError("No job ID found in XCom — trigger task may have failed.")
    logger.info(f"Polling Airbyte Job ID: {job_id}")
    elapsed = 0

    while elapsed < TIMEOUT:
        response = requests.post(
            f"{AIRBYTE_API_URL}/jobs/get",
            json={"id": job_id},
            timeout=30,
        )
        response.raise_for_status()

        job_status = response.json()["job"]["status"]
        print(f"Job {job_id} status: {job_status} (elapsed: {elapsed}s)")

        if job_status == "succeeded":
            logger.info(f"✅ Airbyte sync {job_id} completed successfully.")
            print()
            return

        if job_status in ("failed", "cancelled", "incomplete"):
            raise Exception(
                f"❌ Airbyte sync {job_id} ended with status: {job_status}"
            )

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    raise TimeoutError(
        f"⏱ Airbyte sync {job_id} timed out after {TIMEOUT}s."
    )


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="airbyte_k3s_drift_protection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test","airbyte", "schema"],
) as dag:

    check_drift = PythonOperator(
        task_id="check_schema_drift",
        python_callable=check_for_schema_drift,
    )

    trigger_sync = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_airbyte_sync,
    )

    wait_for_sync = PythonOperator(
        task_id="wait_for_airbyte_sync",
        python_callable=wait_for_airbyte_sync,
    )

    processing_dag = TriggerDagRunOperator(
        task_id='trigger_target',
        trigger_dag_id='spark-self-submetting-manifest',
        logical_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=False
    )

    check_drift >> trigger_sync >> wait_for_sync >> processing_dag
