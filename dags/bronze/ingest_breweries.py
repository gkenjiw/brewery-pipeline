
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.api import fetch_brewery_data
from common.notifiers import notify_discord_failure, notify_discord_success

with DAG('bronze-ingest-breweries', start_date=datetime(2025,7,1), schedule='@once') as dag:
    fetch_task = PythonOperator(
        task_id='fetch_breweries',
        python_callable=fetch_brewery_data,
        on_failure_callback=notify_discord_failure,
        on_success_callback=notify_discord_success
    )
