import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pathlib


def postgres_to_s3(df: pd.DataFrame):
    # step 1: df from extract into csv as tempfile
    with NamedTemporaryFile(mode='w', suffix='.csv') as temp:
        temp_path = str(pathlib.Path(__file__).parent.resolve()) + temp.name
        df.to_csv(temp_path)
    # step 2: upload text file into postgres
        pg_hook = PostgresHook(postgres_conn_id="postrgres_localhost")
        pg_hook.copy_expert(
            f"""
            COPY spotify_data FROM {temp_path} WITH (FORMAT csv, HEADER)
            """, filename=temp_path)
        logging.info(f"Song csv {temp_path} has been pushed to PostgreSQL DB!")
        temp.close()