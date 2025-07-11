
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.transforms import aggregate_gold

with DAG('gold_aggregate_breweries', start_date=datetime(2025,7,1), schedule='@once') as dag:
    aggregate_task = PythonOperator(
        task_id='aggregate_gold',
        python_callable=aggregate_gold
    )
