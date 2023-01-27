from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from datetime import datetime

from get_data_from_rds import get_data

default_args = {
    'owner' : 'JD'
}

with DAG(
    default_args=default_args,
    dag_id='pipeline_for_analysis',
    start_date=datetime.today(),
    catchup=False
) as dag:

# # Task 0: Extracting data from rds i.e. Postgres database (return str)
    begin_task = PythonOperator(
         task_id='adding_variables',
         python_callable=get_data,
         op_kwargs={
            'table_name' : 'youtube_UnderTheInfluenceShow_data'
         }
     )

# # Task 1: Run sentiment analysis and classify each comment (return pd.Dataframe)

# # Task 2: Insert back into rds i.e. Postgres database, via new table  (return bool)

# # Task 3: Help for dashboard

############### Setting task order ################
begin_task