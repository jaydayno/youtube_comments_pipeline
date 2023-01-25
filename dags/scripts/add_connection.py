from airflow.models.variable import Variable
from airflow.models import Connection
from airflow import settings
import pathlib
from dotenv import dotenv_values
import logging

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def add_AWS_connection_to_airflow() -> bool:
    """
    Called in DAG.
    Storing AWS credentials as a Connection, which is used for Airflow S3, lambda and rds operators.

    Returns:
        Returns True when store was made for AWS.
        Returns False when the connection was already made.
    """
    try:
        conn = Connection(
            conn_id="aws_default",
            conn_type="aws",
            login=config['aws_access_key_id'],
            password=config['aws_secret_access_key'],
            extra={
                'region_name' : config['aws_region']
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

def add_config_as_variables_to_airflow() -> bool:
    """
    Called in DAG.
    Storing configuration.env as Variable, which is used throughout the DAG.

    Returns:
        Returns True when store was made for Airflow variables.
        Returns False and stops inputting variables when it reach sensitive data.
    """
    for key_i in config:
        if key_i == '[default]':
            return False
        Variable.set(key_i, config[key_i])
    return True

def add_rds_postgres_connection(ti) -> bool:
    """
    Called in DAG.
    Storing rds postgres instance as Connection, which is used for Airflow postgres operators.

    Args:
        ti: TaskInstance type from Airflow, specific variable for templating.

    Returns:
        Returns True when store was made for rds postgres instance.
        Returns False when the connection was already made.
    """
    try:
        ti.xcom_push(key='db_id', value=config['db_id'])
        conn = Connection(
            conn_id=config['db_id'],
            conn_type="postgres",
            description=None,
            login=config['db_username'],
            password=config['db_full_password'],
            host=config['db_host'],    ## ex. "db-postgres.chofihkladab.us-east-1.rds.amazonaws.com"
            port=5432,
            schema=config['db_main_name']                                    
        )  # create a connection object
        session = settings.Session()  # get the session
        session.add(conn)
        session.commit()
        return True
    except:
        session.rollback()
        return False
    finally:
        session.close()