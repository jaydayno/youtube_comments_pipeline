from datetime import  datetime
import logging
from tempfile import TemporaryDirectory
import pandas as pd
import os
import sys
sys.path.append('/opt/airflow/dags/scripts')
from extract_spotify import extract_spotifyAPI


from airflow import DAG
from airflow.decorators import task
from airflow import settings
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'jay',
}

with DAG(
    default_args=default_args,
    dag_id='postgres_v02',
    start_date=datetime(2022,12,22),
    schedule_interval='@daily'
) as dag:

# Task 1
    @task(task_id='add_connection')
    def add_connection():     
        try:           
            conn = Connection(
                    conn_id="postgres_localhost",
                    conn_type="postgres",
                    description=None,
                    login="airflow",
                    password="airflow",
                    host="host.docker.internal",
                    port=5432,
                    schema="postgres"
                ) #create a connection object
            session = settings.Session() # get the session
            session.add(conn)
            session.commit()
            return 
        except:
            session.rollback()
            pass
        finally:
            session.close()
    add_connection_on_airflow = add_connection()


# Task 2
    create_spotify_table = PostgresOperator(
        task_id='create_table_in_db',
        postgres_conn_id='postgres_localhost',
        sql=
        """
        CREATE TABLE IF NOT EXISTS spotify_data (
            song_name character varying,
            artist_name character varying,
            played_at character varying,
            timestamp character varying
        )
        """,
    )


# Task 3
    @task(task_id="pg_insert_data")
    def postgres_to_s3(df: pd.DataFrame):
        with TemporaryDirectory() as tdr:
            pathName = os.path.join(tdr,'song_df.csv')
            df.to_csv(os.path.join(tdr,'song_df.csv'), index=False)
            pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            with open(pathName, 'r') as f:
                cursor.copy_from(f, "spotify_data", sep=',')
                conn.commit()
                logging.info(f"Song csv {pathName} has been pushed to PostgreSQL DB!")
                cursor.close()
                conn.close()
                return
    insert_spotify_table = postgres_to_s3(extract_spotifyAPI())

################ Setting task order ################
    add_connection_on_airflow.set_downstream(create_spotify_table)
    create_spotify_table.set_downstream(insert_spotify_table)