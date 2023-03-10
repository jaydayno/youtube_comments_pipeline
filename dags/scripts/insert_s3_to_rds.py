from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
from dotenv import dotenv_values
import logging
import pathlib

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
          stage_data_lines = f.read().splitlines()
          count_before_insert = len(stage_data_lines)
          insert_sql = f'''
                    INSERT INTO {table_name} (id, author_channel_id, author, published_at, updated_at, like_count, display_text, key_phrase, text_polarities, classifications)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                    (author_channel_id, author, published_at, updated_at, like_count, display_text, key_phrase, text_polarities, classifications) 
                    = (EXCLUDED.author_channel_id, EXCLUDED.author, EXCLUDED.published_at, EXCLUDED.updated_at, EXCLUDED.like_count, EXCLUDED.display_text, EXCLUDED.key_phrase, EXCLUDED.text_polarities, EXCLUDED.classifications);
                    '''
          for line in stage_data_lines:
               values = tuple(line.split('|'))
               cursor.execute(insert_sql, values)
          cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
          result = cursor.fetchone()
          count_after_insert = result[0]
          num_of_rows_added = count_after_insert - count_before_insert
          conn.commit()
          logging.info(f"Stage data {f.name} has been pushed to PostgreSQL DB in rds with size {f.tell()} B!")
          logging.info(f"The number of rows added to {table_name} is: {num_of_rows_added}")
     cursor.close()
     conn.close()
     return True  