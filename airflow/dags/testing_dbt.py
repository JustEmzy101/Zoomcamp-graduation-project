from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='dbt_bigquery_daily',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['dbt', 'bigquery'],
) as dag:

    run_dbt = KubernetesPodOperator(
        task_id='run_dbt_models',
        name='dbt-run',
        namespace='default',
        image='docker.io/library/dbt-bigquery:v1',
        
        cmds=["/bin/bash", "-c"],
        arguments=[
            "git clone --branch main https://github.com/JustEmzy101/Zoomcamp-graduation-project.git /app/dbt_project && "
            "cd /app/dbt_project/dbt && "  # Navigate to dbt subdirectory
            "dbt deps && "
            "dbt run --profiles-dir ."
        ],
        
        env_vars={
            'GCP_PROJECT_ID': 'turing-chess-484608-k4',
            'BQ_DATASET': 'turing-chess-484608-k4.Staging',
            'GOOGLE_APPLICATION_CREDENTIALS': '/secrets/dbt-sa-key.json',
        },
        
        volumes=[
            k8s.V1Volume(name='dbt-code', empty_dir=k8s.V1EmptyDirVolumeSource()),
            k8s.V1Volume(
                name='gcp-key',
                secret=k8s.V1SecretVolumeSource(secret_name='dbt-bq-secret')
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name='dbt-code', mount_path='/app/dbt_project'),
            k8s.V1VolumeMount(name='gcp-key', mount_path='/secrets', read_only=True),
        ],
        
        container_resources=k8s.V1ResourceRequirements(
            requests={'memory': '1Gi', 'cpu': '500m'},
            limits={'memory': '2Gi', 'cpu': '1000m'}
        ),
        
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
    )

    run_dbt
