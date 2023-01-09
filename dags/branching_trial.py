from datetime import datetime
from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "Jayden D"
}

@task.branch(task_id="branching")
def do_branching():
    return "branch_a"

with DAG(
    default_args=default_args,
    dag_id='branchdag',
    start_date=datetime(2023, 1, 9),
    schedule_interval='@daily',
    template_searchpath='/usr/local/airflow/queries',
    catchup=False
) as dag:

    start_task = EmptyOperator(task_id="start")

    branching = do_branching()

    branch_a = EmptyOperator(task_id="branch_a")

    follow_branch_a = EmptyOperator(task_id="follow_branch_a")

    branch_false = EmptyOperator(task_id="branch_false")

    join = EmptyOperator(task_id="join")

# Task Ordering

start_task >> branching

branching >> branch_a >> follow_branch_a >> join
branching >> branch_false >> join

    