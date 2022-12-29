from airflow.providers.postgres.hooks.postgres import PostgresHook
from tempfile import NamedTemporaryFile
import logging
import sys
import csv
import os

import boto3
from botocore.exceptions import NoCredentialsError


# Import for private data
import pathlib
from dotenv import dotenv_values


script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

S3_BUCKET = config["bucket_name"]

def connect_s3():
    """
    Create a boto3 session and connect to the S3 Resource
    Returns:
        connection to the S3 bucket
    """
    try:
        s3_conn = boto3.resource("s3")
        return s3_conn
    except NoCredentialsError as e:
        raise (e)


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
        with NamedTemporaryFile(mode='w', suffix=f"{airflow_ds_nodash}") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cursor)
            f.flush()
            tempfile_path = f.name
            tempfile_name = os.path.basename(os.path.normpath(f.name))
            logging.info(f"Temp file ({tempfile_path}) with date = {airflow_ds_nodash} created and size {f.tell()} B.")
        # step 2: upload text file into S3
            s3_conn = connect_s3()
            s3_conn.meta.client.upload_file(Filename=tempfile_path, Bucket=S3_BUCKET, Key=tempfile_name)
    except:
        conn.rollback()
        pass
    finally:
        cursor.close()
        conn.close()
    


if __name__ == "__main__":
    upload_csv_s3(sys.argv[1]) # where sys.argv[1] is {{ ds_nodash }}  