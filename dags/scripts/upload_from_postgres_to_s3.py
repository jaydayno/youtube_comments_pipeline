from airflow.providers.postgres.hooks.postgres import PostgresHook
from tempfile import NamedTemporaryFile
import logging
import sys
import csv
import os

import boto3

# Import for private data
import pathlib
from dotenv import dotenv_values


script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

S3_BUCKET = config["bucket_name"]


def upload_csv_s3(airflow_ds_nodash):
    """
    Upload txt file from postgres DB to the S3 bucket
    """
    try:
        # step 1: create text file from postgres cursor
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM spotify_data")
        # step 2: upload text file into S3 using user-made function 'upload_file'
        with NamedTemporaryFile(mode='w+', suffix=f"{airflow_ds_nodash}") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cursor)
            tempfile_name = os.path.basename(os.path.normpath(f.name)) # file name is the same as "file_key" 
            logging.info(f"Temp file ({f.name}) with date = {airflow_ds_nodash} created and size {f.tell()} B.")
        # step 2: upload text file into S3 using user-made function 'upload_file'
            s3_client = boto3.client(
                's3',
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"]
            )
            s3_client.upload_file(f.name, S3_BUCKET, tempfile_name)
            logging.info(f"Temp file ({tempfile_name}) uploaded to {S3_BUCKET}")
            return True
    except:
        conn.rollback()
        raise TypeError("Rolling Back, Failed to upload.")
    finally:
        cursor.close()
        conn.close()
        f.close()

if __name__ == "__main__":
    upload_csv_s3(sys.argv[1]) # where sys.argv[1] is {{ ds_nodash }}  