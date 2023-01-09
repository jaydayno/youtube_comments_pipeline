# %%
from airflow.models.variable import Variable
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
            conn_type="aws",
            extra={
                'aws_access_key_id' : config['aws_access_key_id'],
                'aws_secret_access_key' : config['aws_secret_access_key'],
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
# %%
def add_config_as_variables_to_airflow():
    for key_i in config:
        if key_i == '[default]':
            break
        Variable.set(key_i, config[key_i])
    return True

def add_rds_postgres_connection():
        try:
            conn = Connection(
                conn_id=config['db_id'],
                conn_type="postgres",
                description=None,
                login=config['db_username'],
                password=config['db_password'],
                host=config['db_host'],    ## ex. "db-postgres.chofihkladab.us-east-1.rds.amazonaws.com"
                port=5432,
                schema=config['db_main_name']                                    
            )  # create a connection object
            session = settings.Session()  # get the session
            session.add(conn)
            session.commit()
        except:
            session.rollback()
            return False
        finally:
            session.close()
            return True