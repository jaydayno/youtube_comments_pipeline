from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

from scripts.extract_spotify import upload_to_S3
from scripts.add_connection import add_AWS_connection_to_airflow
from scripts.event_for_lambda import invoke_with_operator

default_args = {
    'owner': 'jay',
}

with DAG(
    default_args=default_args,
    dag_id='primary_dagv3',
    start_date=datetime(2023, 1, 5),
    schedule_interval='@daily',
    catchup=False
) as dag:

# # Task 1
    add_aws_connection = PythonOperator(
        task_id='adding_aws_connection',
        python_callable=add_AWS_connection_to_airflow
    )

# # Task 2
    upload_raw = PythonOperator(
        task_id='extract_and_upload_raw',
        python_callable=upload_to_S3,
        op_kwargs={
        'target_name': 'raw/spotify_data_{{ ds_nodash }}.json'
        }
     )

# Task 3
    invoke_lambda_function = PythonOperator(
        task_id="invoke_lambda_function",
        python_callable=invoke_with_operator,
        op_kwargs={
        'target_name': 'raw/spotify_data_{{ ds_nodash }}.json'
        }
    )

# Task 4
    sense_stage_data = S3KeySensor(
        task_id="wait_for_stage_data_from_lambda",
        bucket_name="{{ task_instance.xcom_pull(task_ids='invoke_lambda_function', key='BUCKET_NAME') }}",
        bucket_key='stage/spotify_data_{{ ds_nodash }}.json',
    )

# Task 5


################ Setting task order ################
add_aws_connection >> upload_raw >> invoke_lambda_function >> sense_stage_data


