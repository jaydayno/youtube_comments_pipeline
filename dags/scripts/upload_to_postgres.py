# %%
import time
import csv
import logging
from datetime import datetime, timedelta
import tempfile
from tempfile import NamedTemporaryFile, TemporaryDirectory
import pandas as pd
import pathlib
from pathlib import Path
import os
# %%
from airflow.providers.postgres.hooks.postgres import PostgresHook


# %%
def postgres_to_s3(df: pd.DataFrame):
    # step 1: df from extract into csv as tempfile
    # with NamedTemporaryFile(mode='w', suffix='.csv') as temp:
    #     pg_hook = PostgresHook(postgres_conn_id="postrgres_localhost")
    #     conn = pg_hook.get_conn()
    #     cursor = conn.cursor()
    #     csv_writer = csv.writer(temp)
    #     csv_writer.writerows(df.to_csv(index=False))
    #     temp.seek(0)
    #     cursor.execute(f"COPY spotify_data FROM '{temp.name}' DELIMITER ',' CSV HEADER")
    #     logging.info(f"Song csv {temp.name} has been pushed to PostgreSQL DB!")
    #     temp.flush()
    #     temp.close()
    #     cursor.close()
    #     conn.close()
    with TemporaryDirectory() as tdr:
        pathName = os.path.join(tdr,'song_df.csv')
        df.to_csv(os.path.join(tdr,'song_df.csv'), index=False)
        pg_hook = PostgresHook(postgres_conn_id="postrgres_localhost")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        with open(pathName, 'r') as f:
            cursor.copy_from(f, "spotify_data", sep=',')
            conn.commit()
            logging.info(f"Song csv {pathName} has been pushed to PostgreSQL DB!")
            cursor.close()
            conn.close()        
        