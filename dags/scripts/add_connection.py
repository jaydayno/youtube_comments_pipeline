from airflow.models import Connection
from airflow import settings
import pathlib
from dotenv import dotenv_values
import logging

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def add_AWS_connection_to_airflow():
    try:
        conn = Connection(
            conn_id="aws_default",
            conn_type="Amazon Web Services",
            extra={
                'aws_access_key_id' : config['aws_access_key_id'],
                'aws_secret_access_key' : config['aws_secret_access_key']
            }
        )  # create a connection object
        session = settings.Session()  # get the session
        session.add(conn)
        session.commit()
        logging.info(f'Added connection: {conn.conn_id}')
        return True
    except:
        session.rollback()
        return False
    finally:
        session.close()