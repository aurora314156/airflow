import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BaseOperator
from Task.main import Main

default_args = {
    'owner': 'Rick wu',
    'start_date': datetime(2100, 1, 1, 0, 0),
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def task1(**context):
    Main().task1()

with DAG('hdfs', default_args=default_args) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=task1,
    )
    do_nothing = DummyOperator(task_id='no_do_nothing')

    # define workflow
    task1 >> do_nothing
