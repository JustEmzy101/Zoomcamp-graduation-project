from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
#from email_utils import send_pretty_email_failure,send_pretty_email_success

def testing_email_notify():
    a = 3 + 4
    return a


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,10,4),
    'email': ['mmzidane101@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=3),
   # 'on_failure_callback': send_pretty_email_failure,
   # 'on_success_callback': send_pretty_email_success
}


with DAG(dag_id='email_notification_etl',
        default_args=default_args,
        schedule= '@daily',
        catchup=False) as dag:

        tsk_email_on_retry_on_fail = PythonOperator(
            task_id= 'testing_email_notify',
            python_callable=testing_email_notify
            )
