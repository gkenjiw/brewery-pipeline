from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.api import fetch_brewery_data
from common.transforms import transform_to_silver, aggregate_gold
from common.notifiers import notify_discord_failure, notify_discord_success

with DAG("brewery_full_pipeline", start_date=datetime(2025,7,1), schedule="@daily") as dag:
    
    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=fetch_brewery_data,
        on_failure_callback=notify_discord_failure,
        on_success_callback=notify_discord_success
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=transform_to_silver,
        on_failure_callback=notify_discord_failure,
        on_success_callback=notify_discord_success
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=aggregate_gold,
        on_failure_callback=notify_discord_failure,
        on_success_callback=notify_discord_success
    )

    bronze_task >> silver_task >> gold_task
