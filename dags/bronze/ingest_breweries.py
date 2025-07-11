
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.api import fetch_brewery_data

with DAG('bronze-ingest-breweries', start_date=datetime(2025,7,1), schedule='@once') as dag:
    fetch_task = PythonOperator(
        task_id='fetch_breweries',
        python_callable=fetch_brewery_data
    )
