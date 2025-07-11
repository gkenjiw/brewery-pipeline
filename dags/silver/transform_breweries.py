
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.transforms import transform_to_silver

with DAG('silver_transform_breweries', start_date=datetime(2025,7,1), schedule='@once') as dag:
    transform_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_to_silver
    )
