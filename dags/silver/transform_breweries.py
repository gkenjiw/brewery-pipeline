
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.transforms import transform_to_silver
from common.notifiers import notify_discord_failure, notify_discord_success

with DAG('silver_transform_breweries', start_date=datetime(2025,7,1), schedule='@once') as dag:
    transform_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_to_silver,
        on_failure_callback=notify_discord_failure,
        on_success_callback=notify_discord_success
    )
