from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Connection
from airflow import settings
from airflow.decorators import task
from airflow import DAG
import datetime

from scripts.extract_spotify import upload_to_S3

default_args = {
    'owner': 'jay',
}

with DAG(
    default_args=default_args,
    dag_id='primary_dag',
    start_date=datetime.datetime.now(),
    schedule_interval='@daily',
    catchup=False
) as dag:

# Task 1
    upload_raw = PythonOperator(
        task_id='extract_and_upload_raw',
        python_callable=upload_to_S3,
        op_kwargs={
        'target_name': 'raw/spotify_data_{{ ds_no_dash }}.json'
        }
    )

# Task 2
    # upload_script = PythonOperator(
    #     task_id='script_to_s3',
    #     python_callable=upload_to_S3,
    #     op_kwargs={
    #     'target_name': 'scripts/processing_json.py',
    #     'script_loc': './dags/scripts/processing_json.py'
    #     }
    # )

# Task 3


# Task 4


################ Setting task order ################
upload_raw #>> upload_script
