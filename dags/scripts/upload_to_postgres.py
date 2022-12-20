# %%
import logging
from tempfile import TemporaryDirectory
import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


# %%
def postgres_to_s3(df: pd.DataFrame):
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
        