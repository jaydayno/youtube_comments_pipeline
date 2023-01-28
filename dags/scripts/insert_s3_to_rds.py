from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
import logging
import sys
import pathlib
from dotenv import dotenv_values


script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

BUCKET_NAME = config['bucket_name']
POSTGRES_CONN_ID = config['db_id']

def insert_postgres(stage_name: str, table_name: str) -> bool:
     # uses S3Hook, reads file (i.e. key), data_stage is a string of the contents of the S3 file
     # PostgresHook connects to rds instance (postgres), write data_stage str into file and let cursor copy_from file and commit into 
     s3 = S3Hook(aws_conn_id='aws_default')
     data_stage = s3.read_key(key=stage_name, bucket_name=BUCKET_NAME)
     
     pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
     conn = pg_hook.get_conn()
     cursor = conn.cursor()
     with NamedTemporaryFile(mode='w+') as f:
          f.write(data_stage)
          f.seek(0)
          cursor.copy_from(f, table_name, sep=',')
          conn.commit()
          logging.info(f"Stage data {f.name} has been pushed to PostgreSQL DB in rds with size {f.tell()} B!")
     cursor.close()
     conn.close()
     return True  