from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

from scripts.extract_youtube import upload_to_S3, add_channel_sql
from scripts.add_connection import add_AWS_connection_to_airflow, add_config_as_variables_to_airflow, add_rds_postgres_connection
from scripts.event_for_lambda import invoke_with_operator
from scripts.insert_s3_to_rds import insert_postgres

default_args = {
    'owner': 'jay',
}

with DAG(
    default_args=default_args,
    dag_id='primary_dagv3',
    start_date=datetime(2023, 1, 9),
    schedule_interval='@daily',
    catchup=False
) as dag:

# # Task 0
    begin_task = PythonOperator(
         task_id='adding_variables',
         python_callable=add_config_as_variables_to_airflow
     )

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
         'target_name': 'raw/youtube_data_{{ ds_nodash }}.json',
         'channel_link': 'https://www.youtube.com/@UnderTheInfluenceShow',
         'num_of_comments': 10
         }
      )

# # Task 3
    add_sql_script = PythonOperator(
        task_id='create_channel_specific_sql_script',
        python_callable=add_channel_sql,
         op_kwargs={
         'channel_name': "{{ task_instance.xcom_pull(task_ids='extract_and_upload_raw', key='channel_name') }}",
         }
    )

# # Task 4
    invoke_lambda_function = PythonOperator(
        task_id="invoke_lambda_function",
        python_callable=invoke_with_operator,
        op_kwargs={
        'target_name': 'raw/youtube_data_{{ ds_nodash }}.json',
        'stage_name': 'stage/youtube_data_{{ ds_nodash }}.csv'
        }
    )

# # Task 5
    sense_stage_data = S3KeySensor(
        task_id="wait_for_stage_data_from_lambda",
        bucket_name="{{ task_instance.xcom_pull(task_ids='invoke_lambda_function', key='BUCKET_NAME') }}",
        bucket_key="stage/youtube_data_{{ ds_nodash }}.csv"
    )

# # Inserting S3 data into rds instance postgres DB: spotify_song_db
# # Task 6
    connect_to_rds_postgres = PythonOperator(
        task_id="create_new_conn_to_rds_postgres",
        python_callable=add_rds_postgres_connection
   )

# # Task 7
    create_youtube_table = PostgresOperator(
        task_id='create_table_in_rds',
        postgres_conn_id="db-postgres", #"{{ task_instance.xcom_pull(task_ids='create_new_conn_to_rds_postgres', key='db_id') }}",
        sql="sql/youtube_{{ task_instance.xcom_pull(task_ids='extract_and_upload_raw', key='channel_name') }}_create.sql"
    )

# # Task 8
    insert_into_spotify_table = PythonOperator(
        task_id='insert_s3_data_into_rds',
        python_callable=insert_postgres,
        op_kwargs={
            'stage_name': 'stage/youtube_data_{{ ds_nodash }}.csv'
        }
    )

# # DONE

############### Setting task order ################
begin_task >> add_aws_connection >> upload_raw >> add_sql_script
add_sql_script >> invoke_lambda_function >> sense_stage_data
sense_stage_data >> connect_to_rds_postgres >> create_youtube_table >> insert_into_spotify_table

# from airflow.providers.amazon.aws.operators.rds import RdsStopDbOperator
# # # Task 9
#     start_rds = RdsStopDbOperator(
#         db_identifier="{{ var.value.db_id }}",
#         db_type="instance",
#         aws_conn_id="aws_default"
#    )