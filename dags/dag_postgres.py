from datetime import  datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG

import sys
sys.path.append('/opt/airflow/dags/scripts')
from extract_spotify import extract_spotifyAPI
from upload_to_postgres import postgres_to_s3

default_args = {
    'owner': 'jay',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='postgres_v02',
    start_date=datetime(2022,12,19),
    schedule_interval='@daily'
) as dag:
    create_spotify_table = PostgresOperator(
        task_id='CreateintoDB',
        postgres_conn_id='postrgres_localhost',
        sql=
        """
        CREATE TABLE IF NOT EXISTS spotify_data (
            song_name character varying,
            artist_name character varying,
            played_at timestamp,
            timestamp timestamp
        )
        """
    ),
    insert_spotify_table = PythonOperator(
        task_id="pg_insert_data",
        python_callable=postgres_to_s3,
        op_kwargs={
            "df": extract_spotifyAPI()
            }
    )
    create_spotify_table >> insert_spotify_table