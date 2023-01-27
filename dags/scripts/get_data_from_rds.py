from airflow.providers.postgres.hooks.postgres import PostgresHook

import pathlib
import logging
import sys
import csv
import os

from dotenv import dotenv_values

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def get_data(table_name: str) -> bool:
    """
    Get data from rds postgres
    """
    try:
        # step 1: create text file from postgres cursor
        hook = PostgresHook(postgres_conn_id=config['db_id'])
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        with open(f'dags/table_data/{table_name}.txt','w+') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cursor)
        return True
    except:
        conn.rollback()
        raise TypeError("Rolling Back, Failed to upload.")
    finally:
        cursor.close()
        conn.close()
