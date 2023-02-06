from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow.operators.python import PythonOperator
from dotenv import dotenv_values
from datetime import datetime
from pathlib import Path
from airflow import DAG
from io import StringIO

from scripts.add_connection import add_AWS_connection_to_airflow, add_config_as_variables_to_airflow, add_rds_postgres_connection
from scripts.extract_youtube import upload_to_S3, add_channel_sql, alter_channel_sql
from scripts.event_for_lambda import invoke_with_operator
from scripts.insert_s3_to_rds import insert_postgres

# Provide channel_link, the infix for the name of your table in RDS postgres and max amount of comments [max 100]
# (table name will be formatted to be PostgreSQL friendly, see SQL directory)
provide_channel_name = 'https://www.youtube.com/channel/UCJjSDX-jUChzOEyok9XYRJQ'
provide_table_infix = 'Jubilee'
provide_num_of_comments = 100 

default_args = {
    'owner': 'jay',
}

path_lib = Path(__file__).parent.resolve()
combine_input = StringIO(f'channel_link = {provide_channel_name}' + '\n' + 
                            f'table_infix = {provide_table_infix.strip().replace(" ", "").lower()}'+ '\n' + 
                            f'comment_max = {provide_num_of_comments}')
config = dotenv_values(f"{path_lib}/configuration.env", stream=combine_input)

with DAG(
    default_args=default_args,
    dag_id='youtube_comment_etl_dag',
    start_date=datetime.today(),
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
# # ts_nodash example: 20180101T000000
    upload_raw = PythonOperator(
         task_id='extract_and_upload_raw',
         python_callable=upload_to_S3,
         op_kwargs={
         'target_name': 'raw/youtube_data_{{ ts_nodash }}.json',
         'channel_link': config['channel_link'],
         'num_of_comments': int(config['comment_max'])
         }
      )

# # Task 3
    add_sql_script = PythonOperator(
        task_id='create_channel_specific_sql_script',
        python_callable=add_channel_sql,
         op_kwargs={
         'channel_name': config['table_infix'],
         }
    )

# # Task 4
    invoke_lambda_function = PythonOperator(
        task_id="invoke_lambda_function",
        python_callable=invoke_with_operator,
        op_kwargs={
        'target_name': 'raw/youtube_data_{{ ts_nodash }}.json',
        'stage_name': 'stage/youtube_data_{{ ts_nodash }}.csv'
        }
    )

# # Task 5
    sense_stage_data = S3KeySensor(
        task_id="wait_for_stage_data_from_lambda",
        bucket_name="{{ task_instance.xcom_pull(task_ids='invoke_lambda_function', key='BUCKET_NAME') }}",
        bucket_key="stage/youtube_data_{{ ts_nodash }}.csv"
    )

# # Inserting S3 data into rds instance postgres DB: youtube_comment_db
# # Task 6
    connect_to_rds_postgres = PythonOperator(
        task_id="create_new_conn_to_rds_postgres",
        python_callable=add_rds_postgres_connection
   )

# # Task 7
    create_youtube_table = PostgresOperator(
        task_id="create_table_in_rds",
        postgres_conn_id="db-postgres",
        sql="sql/youtube_"+ config['table_infix'] + "_create.sql"
    )

# # Task 8
    insert_into_youtube_table = PythonOperator(
        task_id='insert_s3_data_into_rds',
        python_callable=insert_postgres,
        op_kwargs={
            'stage_name': 'stage/youtube_data_{{ ts_nodash }}.csv',
            'table_name': "{{ task_instance.xcom_pull(task_ids='create_channel_specific_sql_script', key='whole_table_name') }}"  # 'youtube_{table_infix}_data'
        }
    )

# # Task 9
    alter_sql_script = PythonOperator(
        task_id='alter_data_types_sql_script',
        python_callable=alter_channel_sql,
         op_kwargs={
         'channel_name': config['table_infix'],
         }
    )

# # Task 10
    alter_youtube_table = PostgresOperator(
        task_id="alter_table_in_rds",
        postgres_conn_id="db-postgres",
        sql="sql/youtube_"+ config['table_infix'] + "_alter.sql"
    )


# # DONE ETL data pipeline

############### Setting task order ################
begin_task >> add_aws_connection >> upload_raw >> add_sql_script
add_sql_script >> invoke_lambda_function >> sense_stage_data
sense_stage_data >> connect_to_rds_postgres >> create_youtube_table
create_youtube_table >> insert_into_youtube_table >> alter_sql_script >> alter_youtube_table

# from airflow.providers.amazon.aws.operators.rds import RdsStopDbOperator
# # # Task 11
#     stop_rds = RdsStopDbOperator(
#         db_identifier="{{ var.value.db_id }}",
#         db_type="instance",
#         aws_conn_id="aws_default"
#    )
# alter_youtube_table >> stop_rds