import time, requests, json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BaseOperator

default_args = {
    'owner': 'Meng Lee',
    'start_date': datetime(2100, 1, 1, 0, 0),
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def process_metadata(mode, **context):
    if mode == 'read':
        print("取得使用者的閱讀紀錄")
    elif mode == 'write':
        print("更新閱讀紀錄")

def check_comic_info(**context):
    all_comic_info = context['task_instance'].xcom_pull(task_ids='get_read_history')
    print("去漫畫網站看有沒有新的章節")

    anything_new = time.time() % 2 > 1
    return anything_new, all_comic_info

def decide_what_to_do(**context):
    anything_new, all_comic_info = context['task_instance'].xcom_pull(task_ids='check_comic_info')

    print("跟紀錄比較，有沒有新連載？")
    if anything_new:
        return 'yes_generate_notification'
    else:
        return 'no_do_nothing'

def generate_message(**context):
    _, all_comic_info = context['task_instance'].xcom_pull(task_ids='check_comic_info')
    print("產生要寄給 Slack 的訊息內容並存成檔案")

def curl_post(message, **context):
    # HTTP POST Request
    s_url = 'https://hooks.slack.com/services/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'
    dict_headers = {'Content-type': 'application/json'}
    dict_payload = {"text": message}
    json_payload = json.dumps(dict_payload)
    requests.post(s_url, data=json_payload, headers=dict_headers)

with DAG('comic_app_v2', default_args=default_args) as dag:

    get_read_history = PythonOperator(
        task_id='get_read_history',
        python_callable=process_metadata,
        op_args=['read']
    )

    check_comic_info = PythonOperator(
        task_id='check_comic_info',
        python_callable=check_comic_info,
        provide_context=True
    )

    decide_what_to_do = BranchPythonOperator(
        task_id='new_comic_available',
        python_callable=decide_what_to_do,
        provide_context=True
    )

    update_read_history = PythonOperator(
        task_id='update_read_history',
        python_callable=process_metadata,
        op_args=['write'],
        provide_context=True
    )

    generate_notification = PythonOperator(
        task_id='yes_generate_notification',
        python_callable=generate_message,
        provide_context=True
    )

    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=curl_post,
        op_kwargs={'message': "Spark is good"},
        provide_context=True,
        dag=dag
    )

    do_nothing = DummyOperator(task_id='no_do_nothing')

    # define workflow
    get_read_history >> check_comic_info >> decide_what_to_do
    decide_what_to_do >> generate_notification
    decide_what_to_do >> do_nothing
    generate_notification >> send_notification >> update_read_history
