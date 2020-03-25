import time, requests, json
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

default_args = {
    'owner': 'Rick wu',
    'start_date': datetime(2100, 1, 1, 0, 0),
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('hql1', default_args=default_args) as dag:
    # "SELECT * FROM item_mart LIMIT 10",
    hql_job_1 = HiveOperator(
        hql="select * from default.item_mart limit 10",
        hive_cli_conn_id = "hive_local",
        schema='default',
        hiveconf_jinja_translate=True,
        task_id='hql_job_1',
        dag=dag)
    
    do_nothing = DummyOperator(task_id='no_do_nothing')

    hql_job_1 >> do_nothing
