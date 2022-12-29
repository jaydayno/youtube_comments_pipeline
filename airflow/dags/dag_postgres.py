from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.models import Connection
from airflow import settings
from airflow.decorators import task
from airflow import DAG

import datetime
import logging
from tempfile import TemporaryDirectory
import pandas as pd
import os

from scripts.extract_spotify import extract_spotifyAPI

default_args = {
    'owner': 'jay',
}

with DAG(
    default_args=default_args,
    dag_id='postgres_v02',
    start_date=datetime.datetime.now(),
    schedule_interval='@daily',
    catchup=False
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
            )  # create a connection object
            session = settings.Session()  # get the session
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
        sql="""
        DROP TABLE IF EXISTS spotify_data;

        CREATE TABLE IF NOT EXISTS spotify_data (
            song_name character varying,
            artist_name character varying,
            played_at character varying,
            timestamp character varying
        );
        """,
    )


# Task 3
    @task(task_id="pg_insert_data")
    def insert_postgres(df: pd.DataFrame):
        try:
            with TemporaryDirectory() as tdr:
                pathName = os.path.join(tdr, 'song_df.csv')
                df.to_csv(os.path.join(tdr, 'song_df.csv'), index=False, header=False)
                pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
                conn = pg_hook.get_conn()
                cursor = conn.cursor()
                with open(pathName, 'r') as f:
                    cursor.copy_from(f, "spotify_data", sep=',')
                    conn.commit()
                    logging.info(f"Song csv {pathName} has been pushed to PostgreSQL DB with size {f.tell()} B!")
                    return
        except:
            conn.rollback()
            pass
        finally:
            cursor.close()
            conn.close()

    insert_spotify_table = insert_postgres(extract_spotifyAPI())


# Task 4
    postgres_txt_to_s3 = BashOperator(
        task_id='bash_execute_upload_script',
        bash_command="python /opt/airflow/scripts/upload_from_postgres_to_s3.py {{ ds_nodash }}",
    )

################ Setting task order ################
    add_connection_on_airflow.set_downstream(create_spotify_table)
    create_spotify_table.set_downstream(insert_spotify_table)
    insert_spotify_table.set_downstream(postgres_txt_to_s3)