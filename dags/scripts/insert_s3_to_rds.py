import logging
from tempfile import TemporaryDirectory
import pandas as pd
import os
import pathlib
import boto3
from dotenv import dotenv_values


script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")


def insert_postgres(stage_name):
    try:
        with TemporaryDirectory() as tdr:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"]
            )
            S3_BUCKET = config["bucket_name"]

            s3_client.get_file()##############
            pathName = os.path.join(tdr, 'song_df.csv')
            df.to_csv(os.path.join(tdr, 'song_df.csv'), index=False, header=False)
            pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            with open(pathName, 'r') as f:
                cursor.copy_from(f, "spotify_data", sep=',')
                conn.commit()
                logging.info(f"Song csv {pathName} has been pushed to PostgreSQL DB with size {f.tell()} B!")
                return True
    except:
        conn.rollback()
        pass
    finally:
        cursor.close()
        conn.close()
        return False